defmodule Sequin.Runtime.MessageHandler do
  @moduledoc false

  use Sequin.ProcessMetrics.Decorator

  alias Sequin.Constants
  alias Sequin.Consumers
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerEventData
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.ConsumerRecordData
  alias Sequin.Consumers.ConsumerRecordData.Metadata.Sink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Error
  alias Sequin.Error.InvariantError
  alias Sequin.Functions.TestMessages
  alias Sequin.Health
  alias Sequin.Health.Event
  alias Sequin.Replication
  alias Sequin.Replication.PostgresReplicationSlot
  alias Sequin.Replication.WalEvent
  alias Sequin.Replication.WalPipeline
  alias Sequin.Repo
  alias Sequin.Runtime.MessageLedgers
  alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.LogicalMessage
  alias Sequin.Runtime.SlotMessageStore
  alias Sequin.Runtime.SlotProcessor
  alias Sequin.Runtime.SlotProcessor.Message
  alias Sequin.Runtime.TableReaderServer

  require Logger

  @max_payload_sizes_by_replication_slot_id %{
    "42df29fa-d2ba-4ef3-9c36-6525af31e598" => 1024 * 1024
  }

  defmodule Context do
    @moduledoc false
    use TypedStruct

    typedstruct do
      field :consumers, [Sequin.Consumers.consumer()], default: []
      field :wal_pipelines, [WalPipeline.t()], default: []
      field :replication_slot_id, String.t()
      field :postgres_database, PostgresDatabase.t()
      field :table_reader_mod, module(), default: TableReaderServer
      field :partition_count, non_neg_integer()
    end
  end

  @callback handle_messages(Context.t(), [Message.t()]) :: {:ok, non_neg_integer()} | {:error, Sequin.Error.t()}

  def context(%PostgresReplicationSlot{} = pr) do
    pr =
      Repo.preload(
        pr,
        [:wal_pipelines, :not_disabled_sink_consumers, :postgres_database],
        force: true
      )

    %Context{
      consumers: pr.not_disabled_sink_consumers,
      wal_pipelines: pr.wal_pipelines,
      postgres_database: pr.postgres_database,
      replication_slot_id: pr.id,
      partition_count: pr.partition_count
    }
  end

  def before_handle_messages(%Context{} = ctx, messages) do
    # Key ideas:
    # 1. get the list of table oids for which there are active backfills
    # 2. call pks_seen for each table_oid with the associated message pks

    case ctx.table_reader_mod.active_table_oids() do
      [] ->
        :ok

      # Get set of table_oids that have running TRSs for efficient lookup
      backfilling_table_oids ->
        backfilling_table_oids = MapSet.new(backfilling_table_oids)

        # Create filtered map of messages, only including those for tables with running TRSs
        messages_by_table_oid =
          messages
          |> Stream.filter(&MapSet.member?(backfilling_table_oids, &1.table_oid))
          |> Enum.group_by(& &1.table_oid, & &1.ids)
          |> Map.new()

        Enum.each(backfilling_table_oids, fn table_oid ->
          if pks = Map.get(messages_by_table_oid, table_oid) do
            ctx.table_reader_mod.pks_seen(table_oid, pks)
          end
        end)

        :ok
    end
  end

  @spec handle_messages(Context.t(), [Message.t()]) :: {:ok, non_neg_integer()} | {:error, Sequin.Error.t()}
  @decorate track_metrics("handle_messages")
  def handle_messages(%Context{}, []) do
    {:ok, 0}
  end

  @decorate track_metrics("handle_messages")
  def handle_messages(%Context{} = ctx, messages) do
    Logger.debug("[MessageHandler] Handling #{length(messages)} message(s)")

    messages = load_unchanged_toasts(ctx, messages)

    save_test_messages(ctx, messages)

    messages_by_consumer = messages_by_consumer(ctx, messages)

    wal_events = wal_events(ctx, messages)

    matching_pipeline_ids = wal_events |> Enum.map(& &1.wal_pipeline_id) |> Enum.uniq()

    res =
      with {:ok, count} <- call_consumer_message_stores(messages_by_consumer) do
        {:ok, wal_event_count} = insert_wal_events(ctx, wal_events, matching_pipeline_ids)

        {:ok, count + wal_event_count}
      end

    Enum.each(matching_pipeline_ids, fn wal_pipeline_id ->
      :syn.publish(:replication, {:wal_event_inserted, wal_pipeline_id}, :wal_event_inserted)
    end)

    res
  end

  @high_watermark_prefix Constants.backfill_batch_high_watermark()
  def handle_logical_message(ctx, commit_lsn, %LogicalMessage{prefix: @high_watermark_prefix} = msg) do
    content = Jason.decode!(msg.content)
    %{"batch_id" => batch_id, "backfill_id" => backfill_id} = content

    # `replication_slot_id` added later, hence why we access it differently
    # Safe to extract from Jason.decode! in the future
    if Map.get(content, "replication_slot_id") == ctx.replication_slot_id do
      :ok =
        ctx.table_reader_mod.flush_batch(backfill_id, %{
          batch_id: batch_id,
          commit_lsn: commit_lsn
        })
    else
      # If we're sharing the same Postgres database between multiple replication slots,
      # we may receive low watermark messages for other replication slots.
      :ok
    end
  end

  defp violates_payload_size?(replication_slot_id, event_or_record) do
    max_payload_size_bytes = Map.get(@max_payload_sizes_by_replication_slot_id, replication_slot_id)

    if max_payload_size_bytes do
      event_or_record.payload_size_bytes > max_payload_size_bytes
    else
      event_or_record.payload_size_bytes > 5_000_000
    end
  end

  defp consumer_event(consumer, database, message) do
    data = event_data_from_message(message, consumer, database)
    payload_size = :erlang.external_size(data)

    %ConsumerEvent{
      consumer_id: consumer.id,
      commit_lsn: message.commit_lsn,
      commit_idx: message.commit_idx,
      commit_timestamp: message.commit_timestamp,
      record_pks: Enum.map(message.ids, &to_string/1),
      group_id: generate_group_id(consumer, message),
      table_oid: message.table_oid,
      deliver_count: 0,
      data: data,
      replication_message_trace_id: message.trace_id,
      payload_size_bytes: payload_size
    }
  end

  defp consumer_record(consumer, database, message) do
    data = record_data_from_message(message, consumer, database)
    payload_size = :erlang.external_size(data)

    %ConsumerRecord{
      consumer_id: consumer.id,
      commit_lsn: message.commit_lsn,
      commit_idx: message.commit_idx,
      commit_timestamp: message.commit_timestamp,
      deleted: message.action == :delete,
      record_pks: Enum.map(message.ids, &to_string/1),
      group_id: generate_group_id(consumer, message),
      table_oid: message.table_oid,
      deliver_count: 0,
      data: data,
      replication_message_trace_id: message.trace_id,
      payload_size_bytes: payload_size
    }
  end

  defp event_data_from_message(%Message{action: :insert} = message, consumer, database) do
    metadata = metadata(message, consumer, database)
    metadata = Map.update!(metadata, :consumer, &struct(ConsumerEventData.Metadata.Sink, &1))
    metadata = struct(ConsumerEventData.Metadata, metadata)

    %ConsumerEventData{
      record: fields_to_map(message.fields),
      changes: nil,
      action: :insert,
      metadata: metadata
    }
  end

  defp event_data_from_message(%Message{action: :update} = message, consumer, database) do
    new_fields = fields_to_map(message.fields)

    changes =
      if message.old_fields do
        filter_changes(fields_to_map(message.old_fields), new_fields)
      else
        %{}
      end

    metadata = metadata(message, consumer, database)
    metadata = Map.update!(metadata, :consumer, &struct(ConsumerEventData.Metadata.Sink, &1))
    metadata = struct(ConsumerEventData.Metadata, metadata)

    %ConsumerEventData{
      record: new_fields,
      changes: changes,
      action: :update,
      metadata: metadata
    }
  end

  defp event_data_from_message(%Message{action: :delete} = message, consumer, database) do
    metadata = metadata(message, consumer, database)
    metadata = Map.update!(metadata, :consumer, &struct(ConsumerEventData.Metadata.Sink, &1))
    metadata = struct(ConsumerEventData.Metadata, metadata)

    %ConsumerEventData{
      record: fields_to_map(message.old_fields),
      changes: nil,
      action: :delete,
      metadata: metadata
    }
  end

  defp record_data_from_message(%Message{action: action} = message, consumer, database)
       when action in [:insert, :update] do
    metadata = metadata(message, consumer, database)
    metadata = Map.update!(metadata, :consumer, &struct(Sink, &1))
    metadata = struct(ConsumerRecordData.Metadata, metadata)

    %ConsumerRecordData{
      record: fields_to_map(message.fields),
      action: action,
      metadata: metadata
    }
  end

  defp record_data_from_message(%Message{action: :delete} = message, consumer, database) do
    metadata = metadata(message, consumer, database)
    metadata = Map.update!(metadata, :consumer, &struct(Sink, &1))
    metadata = struct(ConsumerRecordData.Metadata, metadata)

    %ConsumerRecordData{
      record: fields_to_map(message.old_fields),
      action: :delete,
      metadata: metadata
    }
  end

  defp metadata(%Message{} = message, consumer, %PostgresDatabase{} = database) do
    metadata = %{
      database_name: database.name,
      table_name: message.table_name,
      table_schema: message.table_schema,
      commit_timestamp: message.commit_timestamp,
      commit_lsn: message.commit_lsn,
      commit_idx: message.commit_idx,
      consumer: %{
        id: consumer.id,
        name: consumer.name,
        annotations: consumer.annotations
      },
      transaction_annotations: nil,
      idempotency_key: Base.encode64("#{message.commit_lsn}:#{message.commit_idx}")
    }

    if consumer.message_kind == :event do
      case decode_transaction_annotations(message.transaction_annotations) do
        {:ok, annotations} ->
          Map.put(metadata, :transaction_annotations, annotations)

        _ ->
          error = Error.invariant(message: "Invalid JSON given to `transaction_annotations.set`")

          Health.put_event(:sink_consumer, consumer.id, %Event{
            slug: :invalid_transaction_annotation_received,
            status: :warning,
            error: error
          })

          metadata
      end
    else
      metadata
    end
  end

  defp fields_to_map(fields) do
    Map.new(fields, fn %{column_name: name, value: value} -> {name, value} end)
  end

  defp filter_changes(old_map, new_map) do
    old_map
    |> Enum.reduce(%{}, fn {k, v}, acc ->
      if Map.get(new_map, k) in [:unchanged_toast, v] do
        acc
      else
        Map.put(acc, k, v)
      end
    end)
    |> Map.new()
  end

  @decorate track_metrics("call_consumer_message_stores")
  defp call_consumer_message_stores(messages_by_consumer) do
    res =
      Enum.reduce_while(messages_by_consumer, :ok, fn {consumer, messages}, :ok ->
        message_ledgers(consumer, messages)

        case put_messages(consumer, messages) do
          :ok -> {:cont, :ok}
          {:error, _} = error -> {:halt, error}
        end
      end)

    with :ok <- res do
      {:ok, Enum.sum_by(messages_by_consumer, fn {_, messages} -> length(messages) end)}
    end
  end

  @max_backoff_ms 100
  @max_attempts 15
  @decorate track_metrics("put_messages")
  defp put_messages(consumer, messages_to_ingest) do
    do_put_messages(consumer, messages_to_ingest)
  end

  defp do_put_messages(consumer, messages_to_ingest, attempt \\ 1) do
    case SlotMessageStore.put_messages(consumer, messages_to_ingest) do
      :ok ->
        Health.put_event(:sink_consumer, consumer.id, %Event{slug: :messages_ingested, status: :success})
        :ok

      {:error, %InvariantError{code: :payload_size_limit_exceeded}} when attempt <= @max_attempts ->
        backoff = Sequin.Time.exponential_backoff(5, attempt, @max_backoff_ms)

        Logger.debug(
          "[MessageHandler] Slot message store for consumer #{consumer.id} is full. " <>
            "Backing off for #{backoff}ms before retry #{attempt + 1}/#{@max_attempts}..."
        )

        Process.sleep(backoff)
        do_put_messages(consumer, messages_to_ingest, attempt + 1)

      {:error, error} ->
        Health.put_event(:sink_consumer, consumer.id, %Event{slug: :messages_ingested, status: :fail, error: error})
        {:error, error}
    end
  end

  @decorate track_metrics("messages_by_consumer")
  defp messages_by_consumer(%Context{} = ctx, messages) do
    # We group_by consumer_id throughput because consumer as a key is slow!
    # So we need to do fast lookups by consumer_id
    consumers_by_id = Map.new(ctx.consumers, fn consumer -> {consumer.id, consumer} end)

    messages
    # First we get a list of consumer_ids that match the SlotProcessor.Message
    |> Stream.map(fn %SlotProcessor.Message{} = message ->
      matching_consumer_ids =
        ctx.consumers
        |> Stream.filter(&Consumers.matches_message?(&1, message))
        # Then we map to a list of consumer_ids
        |> Enum.map(& &1.id)

      {message, matching_consumer_ids}
    end)
    # Next we flat_map it to a list of {matching_consumer_id, consumer_message} tuples,
    # because each consumer_message is unique to a consumer
    |> Stream.flat_map(fn {message, matching_consumer_ids} ->
      Enum.map(matching_consumer_ids, fn consumer_id ->
        consumer = Map.fetch!(consumers_by_id, consumer_id)

        # Just for type clarity
        case consumer_message(consumer, ctx.postgres_database, message) do
          %ConsumerEvent{} = consumer_message -> {consumer_id, consumer_message}
          %ConsumerRecord{} = consumer_message -> {consumer_id, consumer_message}
        end
      end)
    end)
    |> Stream.reject(fn {_, consumer_message} -> violates_payload_size?(ctx.replication_slot_id, consumer_message) end)
    # Finally we return a list of tuples of {consumer, consumer_messages}
    |> Enum.group_by(
      fn {consumer_id, _consumer_message} -> consumer_id end,
      fn {_, consumer_message} -> consumer_message end
    )
    |> Enum.map(fn {consumer_id, messages} ->
      {Map.fetch!(consumers_by_id, consumer_id), messages}
    end)
  end

  @decorate track_metrics("map_to_consumer_message")
  defp consumer_message(%SinkConsumer{} = consumer, %PostgresDatabase{} = database, %SlotProcessor.Message{} = message) do
    case consumer.message_kind do
      :event -> consumer_event(consumer, database, message)
      :record -> consumer_record(consumer, database, message)
    end
  end

  @decorate track_metrics("insert_wal_events")
  defp insert_wal_events(%Context{}, [], _matching_pipeline_ids), do: {:ok, 0}

  @decorate track_metrics("insert_wal_events")
  defp insert_wal_events(%Context{} = ctx, wal_events, matching_pipeline_ids) do
    Repo.transact(fn ->
      {:ok, wal_event_count} = insert_wal_events(wal_events)

      # Update WAL Pipeline Health
      ctx.wal_pipelines
      |> Stream.filter(&(&1.id in matching_pipeline_ids))
      |> Enum.each(fn %WalPipeline{} = pipeline ->
        Health.put_event(pipeline, %Event{slug: :messages_ingested, status: :success})
      end)

      {:ok, wal_event_count}
    end)
  end

  defp insert_wal_events(wal_events) do
    wal_events
    |> Enum.chunk_every(1000)
    |> Enum.reduce({:ok, 0}, fn batch, {:ok, acc} ->
      {:ok, count} = Replication.insert_wal_events(batch)
      {:ok, acc + count}
    end)
  end

  defp wal_event(pipeline, message) do
    wal_event = %WalEvent{
      wal_pipeline_id: pipeline.id,
      commit_lsn: message.commit_lsn,
      commit_idx: message.commit_idx,
      record_pks: Enum.map(message.ids, &to_string/1),
      record: fields_to_map(get_fields(message)),
      changes: get_changes(message),
      action: message.action,
      committed_at: message.commit_timestamp,
      replication_message_trace_id: message.trace_id,
      source_table_oid: message.table_oid,
      source_table_schema: message.table_schema,
      source_table_name: message.table_name,
      transaction_annotations: nil
    }

    case decode_transaction_annotations(message.transaction_annotations) do
      {:ok, annotations} -> Map.put(wal_event, :transaction_annotations, annotations)
      _ -> wal_event
    end
  end

  defp decode_transaction_annotations(nil), do: {:ok, nil}

  defp decode_transaction_annotations(annotations) when is_binary(annotations) do
    case Jason.decode(annotations) do
      {:ok, json} ->
        {:ok, json}

      {:error, error} ->
        Logger.error("Error parsing transaction annotations: #{inspect(error)}", error: error)
        {:error, error}
    end
  end

  defp get_changes(%Message{action: :update} = message) do
    if message.old_fields do
      filter_changes(fields_to_map(message.old_fields), fields_to_map(message.fields))
    else
      %{}
    end
  end

  defp get_changes(_), do: nil

  defp get_fields(%Message{action: :insert} = message), do: message.fields
  defp get_fields(%Message{action: :update} = message), do: message.fields
  defp get_fields(%Message{action: :delete} = message), do: message.old_fields

  defp generate_group_id(consumer, message) do
    default_group_id = Enum.map_join(message.ids, ":", &to_string/1)

    case consumer do
      %SinkConsumer{source_tables: nil} ->
        default_group_id

      %SinkConsumer{source_tables: source_tables} ->
        if source_table = Enum.find(source_tables, &(&1.table_oid == message.table_oid)) do
          source_table.group_column_attnums
          |> Enum.map(fn attnum ->
            fields = if message.action == :delete, do: message.old_fields, else: message.fields
            Enum.find(fields, &(&1.column_attnum == attnum))
          end)
          |> Enum.filter(& &1)
          |> Enum.map_join(":", &to_string(&1.value))
        else
          default_group_id
        end
    end
  end

  @decorate track_metrics("load_unchanged_toasts")
  defp load_unchanged_toasts(%Context{} = ctx, messages) do
    # Skip if no messages have unchanged TOASTs
    if Enum.any?(messages, &has_unchanged_toast?/1) do
      load_unchanged_toasts_from_old(ctx, messages)
    else
      messages
    end
  end

  defp has_unchanged_toast?(%Message{fields: nil}), do: false

  defp has_unchanged_toast?(%Message{fields: fields}) do
    Enum.any?(fields, fn %Message.Field{value: value} -> value == :unchanged_toast end)
  end

  defp load_unchanged_toasts_from_old(%Context{} = ctx, messages) do
    messages =
      Enum.reduce(messages, [], fn
        %Message{action: :update, old_fields: nil} = message, messages_acc ->
          if has_unchanged_toast?(message) do
            put_unchanged_toast_health_event(ctx, message)
          end

          [message | messages_acc]

        %Message{action: :update, fields: fields, old_fields: old_fields} = message, messages_acc ->
          updated_fields =
            Enum.map(fields, fn
              %Message.Field{value: :unchanged_toast} = field ->
                old_field = Sequin.Enum.find!(old_fields, &(&1.column_attnum == field.column_attnum))
                %{field | value: old_field.value}

              field ->
                field
            end)

          [%{message | fields: updated_fields} | messages_acc]

        %Message{} = message, messages_acc ->
          [message | messages_acc]
      end)

    Enum.reverse(messages)
  end

  defp put_unchanged_toast_health_event(%Context{} = ctx, message) do
    Enum.each(ctx.consumers, fn %SinkConsumer{} = consumer ->
      if Consumers.matches_message?(consumer, message) do
        Health.put_event(consumer, %Event{slug: :toast_columns_detected, status: :warning})
      end
    end)
  end

  @decorate track_metrics("save_test_messages")
  defp save_test_messages(%Context{}, []), do: :ok

  @decorate track_metrics("save_test_messages")
  defp save_test_messages(%Context{} = ctx, messages) do
    # Early out if no sequences need messages
    if TestMessages.needs_test_messages?(ctx.postgres_database.id) do
      # Process each message once
      Enum.each(messages, fn message ->
        # Add message to each matching sequence's test messages
        test_consumer = %SinkConsumer{
          message_kind: :event,
          name: "test-consumer",
          postgres_database: ctx.postgres_database,
          annotations: %{"test_message" => true, "info" => "Test messages are not associated with any sink"}
        }

        message = consumer_event(test_consumer, ctx.postgres_database, message)
        TestMessages.add_test_message(ctx.postgres_database.id, message.table_oid, message)
      end)
    end
  end

  @decorate track_metrics("message_ledgers")
  defp message_ledgers(%SinkConsumer{} = consumer, messages) do
    if env() != :prod or Sequin.random(1..100) == 1 do
      all_wal_cursors = Enum.map(messages, &MessageLedgers.wal_cursor_from_message/1)
      :ok = MessageLedgers.wal_cursors_ingested(consumer.id, all_wal_cursors)
    end
  end

  @decorate track_metrics("wal_events")
  defp wal_events(ctx, messages) do
    Enum.flat_map(messages, fn message ->
      ctx.wal_pipelines
      |> Enum.filter(&Consumers.matches_message?(&1, message))
      |> Enum.map(fn pipeline ->
        wal_event(pipeline, message)
      end)
    end)
  end

  defp env do
    Application.get_env(:sequin, :env)
  end
end

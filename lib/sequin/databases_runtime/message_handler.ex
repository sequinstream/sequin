defmodule Sequin.DatabasesRuntime.SlotProcessor.MessageHandler do
  @moduledoc false
  @behaviour Sequin.DatabasesRuntime.SlotProcessor.MessageHandlerBehaviour

  alias Sequin.Constants
  alias Sequin.Consumers
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerEventData
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.ConsumerRecordData
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.ConsumersRuntime.MessageLedgers
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.DatabasesRuntime.PostgresAdapter.Decoder.Messages.LogicalMessage
  alias Sequin.DatabasesRuntime.SlotMessageStore
  alias Sequin.DatabasesRuntime.SlotProcessor
  alias Sequin.DatabasesRuntime.SlotProcessor.Message
  alias Sequin.DatabasesRuntime.SlotProcessor.MessageHandlerBehaviour
  alias Sequin.DatabasesRuntime.TableReaderServer
  alias Sequin.Error.InvariantError
  alias Sequin.Health
  alias Sequin.Health.Event
  alias Sequin.Replication
  alias Sequin.Replication.PostgresReplicationSlot
  alias Sequin.Replication.WalEvent
  alias Sequin.Replication.WalPipeline
  alias Sequin.Repo

  require Logger

  @max_payload_sizes_by_replication_slot_id %{
    "42df29fa-d2ba-4ef3-9c36-6525af31e598" => 1024 * 1024
  }

  defmodule BatchState do
    @moduledoc false
    use TypedStruct

    typedstruct do
      field :batch_id, String.t()
      field :table_oid, non_neg_integer()
      field :backfill_id, String.t()
      field :commit_lsn, integer()
      field :primary_key_values, MapSet.t(list()), default: MapSet.new()
    end
  end

  defmodule Context do
    @moduledoc false
    use TypedStruct

    typedstruct do
      field :consumers, [Sequin.Consumers.consumer()], default: []
      field :wal_pipelines, [WalPipeline.t()], default: []
      field :replication_slot_id, String.t()
      field :postgres_database, PostgresDatabase.t()
      field :table_reader_batches, [BatchState.t()], default: []
      field :max_pks_per_batch, non_neg_integer(), default: 1_000_000
      field :table_reader_mod, module(), default: TableReaderServer
    end
  end

  def context(%PostgresReplicationSlot{} = pr) do
    pr =
      Repo.preload(pr, [:wal_pipelines, :postgres_database, not_disabled_sink_consumers: [:sequence, :postgres_database]])

    %Context{
      consumers: pr.not_disabled_sink_consumers,
      wal_pipelines: pr.wal_pipelines,
      postgres_database: pr.postgres_database,
      replication_slot_id: pr.id
    }
  end

  @impl MessageHandlerBehaviour
  def handle_messages(%Context{} = ctx, messages) do
    Logger.debug("[MessageHandler] Handling #{length(messages)} message(s)")

    ctx = update_table_reader_batch_pks(ctx, messages)

    {ctx, messages} = load_unchanged_toasts(ctx, messages)

    messages_with_consumer =
      Enum.flat_map(messages, fn message ->
        ctx.consumers
        |> Enum.map(fn consumer ->
          if Consumers.matches_message?(consumer, message) do
            cond do
              consumer.message_kind == :event ->
                {{:insert, consumer_event(consumer, message)}, consumer}

              consumer.message_kind == :record and message.action == :delete ->
                # We do not propagate delete messages to record consumers
                nil

              consumer.message_kind == :record ->
                {{:insert, consumer_record(consumer, message)}, consumer}
            end
          end
        end)
        |> Enum.reject(&is_nil/1)
        |> Enum.reject(&violates_payload_size?(ctx.replication_slot_id, &1))
      end)

    wal_events =
      Enum.flat_map(messages, fn message ->
        ctx.wal_pipelines
        |> Enum.filter(&Consumers.matches_message?(&1, message))
        |> Enum.map(fn pipeline ->
          wal_event(pipeline, message)
        end)
      end)

    matching_pipeline_ids = wal_events |> Enum.map(& &1.wal_pipeline_id) |> Enum.uniq()

    messages_by_consumer =
      Enum.group_by(
        messages_with_consumer,
        # key function
        fn {{_action, _event_or_record}, consumer} -> consumer end,
        # value function
        fn {{_action, event_or_record}, _consumer} -> event_or_record end
      )

    res =
      with {:ok, count} <- call_consumer_message_stores(messages_by_consumer) do
        {:ok, wal_event_count} =
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

        # # Trace Messages
        # messages_with_consumer
        # |> Enum.group_by(
        #   fn {{action, _event_or_record}, consumer} -> {action, consumer} end,
        #   fn {{_action, event_or_record}, _consumer} -> event_or_record end
        # )
        # |> Enum.each(fn
        #   {{:insert, consumer}, messages} -> TracerServer.messages_ingested(consumer, messages)
        #   {{:delete, _consumer}, _messages} -> :ok
        # end)

        {:ok, count + wal_event_count}
      end

    Enum.each(matching_pipeline_ids, fn wal_pipeline_id ->
      :syn.publish(:replication, {:wal_event_inserted, wal_pipeline_id}, :wal_event_inserted)
    end)

    with {:ok, count} <- res do
      {:ok, count, ctx}
    end
  end

  @low_watermark_prefix Constants.backfill_batch_low_watermark()
  @high_watermark_prefix Constants.backfill_batch_high_watermark()

  @impl MessageHandlerBehaviour
  def handle_logical_message(ctx, commit_lsn, %LogicalMessage{prefix: @low_watermark_prefix} = msg) do
    content = Jason.decode!(msg.content)
    %{"batch_id" => batch_id, "table_oid" => table_oid, "backfill_id" => backfill_id} = content

    # Split batches into those matching the backfill_id and others
    {matching_batches, remaining_batches} = Enum.split_with(ctx.table_reader_batches, &(&1.backfill_id == backfill_id))

    if length(matching_batches) > 0 do
      Logger.warning(
        "[MessageHandler] Got unexpected low watermark. Discarding #{length(matching_batches)} existing batch(es) for backfill_id: #{backfill_id}"
      )
    end

    # `replication_slot_id` added later, hence why we access it differently
    # Safe to extract from Jason.decode! in the future
    if Map.get(content, "replication_slot_id") == ctx.replication_slot_id do
      # Create new batch and prepend to remaining batches
      %{
        ctx
        | table_reader_batches: [
            %BatchState{
              batch_id: batch_id,
              table_oid: table_oid,
              backfill_id: backfill_id,
              primary_key_values: MapSet.new(),
              commit_lsn: commit_lsn
            }
            | remaining_batches
          ]
      }
    else
      # If we're sharing the same Postgres database between multiple replication slots,
      # we may receive low watermark messages for other replication slots.
      ctx
    end
  end

  def handle_logical_message(ctx, _commit_lsn, %LogicalMessage{prefix: @high_watermark_prefix} = msg) do
    content = Jason.decode!(msg.content)
    %{"batch_id" => batch_id, "backfill_id" => backfill_id} = content

    # `replication_slot_id` added later, hence why we access it differently
    # Safe to extract from Jason.decode! in the future
    if Map.get(content, "replication_slot_id") == ctx.replication_slot_id do
      case Enum.find(ctx.table_reader_batches, &(&1.batch_id == batch_id)) do
        nil ->
          Logger.error(
            "[MessageHandler] Batch not found in table reader batches (batch_id: #{batch_id}, backfill_id: #{backfill_id})"
          )

          ctx.table_reader_mod.discard_batch(backfill_id, batch_id)
          ctx

        batch ->
          :ok =
            ctx.table_reader_mod.flush_batch(backfill_id, %{
              batch_id: batch_id,
              commit_lsn: batch.commit_lsn,
              drop_pks: batch.primary_key_values
            })

          update_in(ctx.table_reader_batches, fn batches ->
            Enum.reject(batches, &(&1.batch_id == batch_id))
          end)
      end
    else
      # If we're sharing the same Postgres database between multiple replication slots,
      # we may receive low watermark messages for other replication slots.
      ctx
    end
  end

  def handle_logical_message(ctx, _commit_lsn, _msg) do
    ctx
  end

  defp violates_payload_size?(replication_slot_id, {{_action, event_or_record}, _consumer}) do
    max_payload_size_bytes = Map.get(@max_payload_sizes_by_replication_slot_id, replication_slot_id)

    if max_payload_size_bytes do
      event_or_record.payload_size_bytes > max_payload_size_bytes
    else
      event_or_record.payload_size_bytes > 5_000_000
    end
  end

  # Track primary key values that match a backfill batch (a current backfill with an open low watermark)
  defp update_table_reader_batch_pks(%Context{} = ctx, messages) do
    update_in(ctx.table_reader_batches, fn batches ->
      batches
      |> Enum.map(fn batch ->
        matching_msgs = Enum.filter(messages, &(&1.table_oid == batch.table_oid))

        # Create a MapSet of all ids from matching messages and union with existing values

        incoming_pks = MapSet.new(matching_msgs, fn msg -> Enum.map(msg.ids, &to_string/1) end)
        new_pks = MapSet.union(batch.primary_key_values, incoming_pks)

        if MapSet.size(new_pks) > ctx.max_pks_per_batch do
          Logger.error("[MessageHandler] Batch #{batch.batch_id} has too many primary key values. Evicting batch.")
          nil
        else
          %{batch | primary_key_values: MapSet.union(batch.primary_key_values, incoming_pks)}
        end
      end)
      |> Enum.reject(&is_nil/1)
    end)
  end

  defp consumer_event(consumer, message) do
    data = event_data_from_message(message, consumer)
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
      data: event_data_from_message(message, consumer),
      replication_message_trace_id: message.trace_id,
      payload_size_bytes: payload_size
    }
  end

  defp consumer_record(consumer, message) do
    data = record_data_from_message(message, consumer)
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
      data: record_data_from_message(message, consumer),
      replication_message_trace_id: message.trace_id,
      payload_size_bytes: payload_size
    }
  end

  defp event_data_from_message(%Message{action: :insert} = message, consumer) do
    %ConsumerEventData{
      record: fields_to_map(message.fields),
      changes: nil,
      action: :insert,
      metadata: struct(ConsumerEventData.Metadata, metadata(message, consumer))
    }
  end

  defp event_data_from_message(%Message{action: :update} = message, consumer) do
    changes = if message.old_fields, do: filter_changes(message.old_fields, message.fields), else: %{}

    %ConsumerEventData{
      record: fields_to_map(message.fields),
      changes: changes,
      action: :update,
      metadata: struct(ConsumerEventData.Metadata, metadata(message, consumer))
    }
  end

  defp event_data_from_message(%Message{action: :delete} = message, consumer) do
    %ConsumerEventData{
      record: fields_to_map(message.old_fields),
      changes: nil,
      action: :delete,
      metadata: struct(ConsumerEventData.Metadata, metadata(message, consumer))
    }
  end

  defp record_data_from_message(%Message{action: action} = message, consumer) when action in [:insert, :update] do
    %ConsumerRecordData{
      record: fields_to_map(message.fields),
      action: action,
      metadata: struct(ConsumerRecordData.Metadata, metadata(message, consumer))
    }
  end

  defp record_data_from_message(%Message{action: :delete} = message, consumer) do
    %ConsumerRecordData{
      record: fields_to_map(message.old_fields),
      action: :delete,
      metadata: struct(ConsumerRecordData.Metadata, metadata(message, consumer))
    }
  end

  defp metadata(%Message{} = message, consumer) do
    %{
      database_name: consumer.postgres_database.name,
      table_name: message.table_name,
      table_schema: message.table_schema,
      commit_timestamp: message.commit_timestamp,
      consumer: %{
        id: consumer.id,
        name: consumer.name
      }
    }
  end

  defp fields_to_map(fields) do
    Map.new(fields, fn %{column_name: name, value: value} -> {name, value} end)
  end

  defp filter_changes(old_fields, new_fields) do
    old_map = fields_to_map(old_fields)
    new_map = fields_to_map(new_fields)

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

  defp call_consumer_message_stores(messages_by_consumer) do
    res =
      Enum.reduce_while(messages_by_consumer, :ok, fn {consumer, messages}, :ok ->
        all_wal_cursors = Enum.map(messages, &MessageLedgers.wal_cursor_from_message/1)

        # Step 1: Mark all WAL cursors as ingested first
        :ok = MessageLedgers.wal_cursors_ingested(consumer.id, all_wal_cursors)

        # Step 2: Check which ones were already delivered
        {:ok, delivered_wal_cursors} = MessageLedgers.filter_delivered_wal_cursors(consumer.id, all_wal_cursors)

        # Step 3: Re-mark delivered wal cursors as delivered
        :ok = MessageLedgers.wal_cursors_delivered(consumer.id, delivered_wal_cursors)

        # Step 4: Filter out already delivered messages before sending downstream
        delivered_wal_cursors = MapSet.new(delivered_wal_cursors)

        messages_to_ingest =
          Enum.reject(messages, fn message ->
            wal_cursor = Map.take(message, [:commit_lsn, :commit_idx])
            MapSet.member?(delivered_wal_cursors, wal_cursor)
          end)

        case put_messages(consumer, messages_to_ingest) do
          :ok -> {:cont, :ok}
          {:error, _} = error -> {:halt, error}
        end
      end)

    with :ok <- res do
      {:ok, Enum.sum_by(messages_by_consumer, fn {_, messages} -> length(messages) end)}
    end
  end

  @max_backoff_ms :timer.seconds(1)
  @max_attempts 5
  defp put_messages(%SinkConsumer{} = consumer, messages_to_ingest, attempt \\ 1) do
    case SlotMessageStore.put_messages(consumer.id, messages_to_ingest) do
      :ok ->
        Health.put_event(consumer, %Event{slug: :messages_ingested, status: :success})
        :ok

      {:error, %InvariantError{code: :payload_size_limit_exceeded}} when attempt <= @max_attempts ->
        backoff = Sequin.Time.exponential_backoff(50, attempt, @max_backoff_ms)

        Logger.debug(
          "[MessageHandler] Slot message store for consumer #{consumer.id} is full. " <>
            "Backing off for #{backoff}ms before retry #{attempt + 1}/#{@max_attempts}..."
        )

        Process.sleep(backoff)
        put_messages(consumer, messages_to_ingest, attempt + 1)

      {:error, error} ->
        Health.put_event(consumer, %Event{slug: :messages_ingested, status: :fail, error: error})
        {:error, error}
    end
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
    %WalEvent{
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
      source_table_name: message.table_name
    }
  end

  defp get_changes(%Message{action: :update} = message) do
    if message.old_fields, do: filter_changes(message.old_fields, message.fields), else: %{}
  end

  defp get_changes(_), do: nil

  defp get_fields(%Message{action: :insert} = message), do: message.fields
  defp get_fields(%Message{action: :update} = message), do: message.fields
  defp get_fields(%Message{action: :delete} = message), do: message.old_fields

  defp generate_group_id(consumer, message) do
    # This should be way more assertive - we should error if we don't find the source table
    # We have a lot of tests that do not line up consumer source_tables with the message table oid
    if consumer.sequence_filter.group_column_attnums do
      Enum.map_join(consumer.sequence_filter.group_column_attnums, ",", fn attnum ->
        fields = if message.action == :delete, do: message.old_fields, else: message.fields
        field = Sequin.Enum.find!(fields, &(&1.column_attnum == attnum))
        to_string(field.value)
      end)
    else
      Enum.map_join(message.ids, ",", &to_string/1)
    end
  end

  defp load_unchanged_toasts(%Context{} = ctx, messages) do
    # Skip if no messages have unchanged TOASTs
    if Enum.any?(messages, &has_unchanged_toast?/1) do
      load_unchanged_toasts_from_old(ctx, messages)
    else
      {ctx, messages}
    end
  end

  defp has_unchanged_toast?(%Message{fields: nil}), do: false

  defp has_unchanged_toast?(%Message{fields: fields}) do
    Enum.any?(fields, fn %Message.Field{value: value} -> value == :unchanged_toast end)
  end

  defp load_unchanged_toasts_from_old(%Context{} = ctx, messages) do
    {ctx, messages} =
      Enum.reduce(messages, {ctx, []}, fn
        %SlotProcessor.Message{action: :update, old_fields: nil} = message, {ctx, messages_acc} ->
          if has_unchanged_toast?(message) do
            put_unchanged_toast_health_event(ctx, message)
          end

          {ctx, [message | messages_acc]}

        %SlotProcessor.Message{action: :update, fields: fields, old_fields: old_fields} = message, {ctx, messages_acc} ->
          updated_fields =
            Enum.map(fields, fn
              %SlotProcessor.Message.Field{value: :unchanged_toast} = field ->
                old_field = Sequin.Enum.find!(old_fields, &(&1.column_attnum == field.column_attnum))
                %{field | value: old_field.value}

              field ->
                field
            end)

          {ctx, [%{message | fields: updated_fields} | messages_acc]}

        %SlotProcessor.Message{} = message, {ctx, messages_acc} ->
          {ctx, [message | messages_acc]}
      end)

    {ctx, Enum.reverse(messages)}
  end

  defp put_unchanged_toast_health_event(%Context{} = ctx, message) do
    Enum.each(ctx.consumers, fn %SinkConsumer{} = consumer ->
      if Consumers.matches_message?(consumer, message) do
        Health.put_event(consumer, %Event{slug: :toast_columns_detected, status: :warning})
        ctx
      end
    end)
  end
end

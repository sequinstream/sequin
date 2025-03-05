defmodule Sequin.Runtime.SlotProcessor.MessageHandler do
  @moduledoc false
  @behaviour Sequin.Runtime.SlotProcessor.MessageHandlerBehaviour

  alias Sequin.Constants
  alias Sequin.Consumers
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerEventData
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.ConsumerRecordData
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Error.InvariantError
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
  alias Sequin.Runtime.SlotProcessor.MessageHandlerBehaviour
  alias Sequin.Runtime.TableReaderServer
  alias Wasmex.Wasi.WasiP2Options

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
  def before_handle_messages(%Context{} = ctx, messages) do
    # First, filter to only consumers with running TableReaderServers
    backfilling_consumers =
      Enum.filter(ctx.consumers, fn consumer ->
        ctx.table_reader_mod.running_for_consumer?(consumer.id)
      end)

    case backfilling_consumers do
      [] ->
        :ok

      _ ->
        # Get set of table_oids that have running TRSs for efficient lookup
        backfilling_table_oids =
          MapSet.new(backfilling_consumers, & &1.sequence.table_oid)

        # Create filtered map of messages, only including those for tables with running TRSs
        messages_by_table_oid =
          messages
          |> Stream.filter(&MapSet.member?(backfilling_table_oids, &1.table_oid))
          |> Enum.group_by(& &1.table_oid, & &1.ids)
          |> Map.new()

        # Process each active consumer
        Enum.each(backfilling_consumers, fn consumer ->
          if table_oid = consumer.sequence.table_oid do
            if pks = Map.get(messages_by_table_oid, table_oid) do
              ctx.table_reader_mod.pks_seen(consumer.id, pks)
            end
          end
        end)
    end
  end

  @impl MessageHandlerBehaviour
  def handle_messages(%Context{}, []) do
    {:ok, 0}
  end

  def handle_messages(%Context{} = ctx, messages) do
    execute_timed(:handle_messages, fn ->
      Logger.debug("[MessageHandler] Handling #{length(messages)} message(s)")

      messages = load_unchanged_toasts(ctx, messages)

      messages_by_consumer_id =
        execute_timed(:messages_by_consumer_id, fn ->
          messages_by_consumer_id(ctx, messages)
        end)

      dbg(messages_by_consumer_id)

      messages_by_consumer_id =
        Enum.map(messages_by_consumer_id, fn {con_id, messages} ->
          messages =
            Enum.map(messages, fn message ->
              message_data = message.data

              record_data =
                message.data
                |> Map.take([:record, :action])
                |> Map.put(:old, Map.merge(message.data.record, message.data.changes))
                |> Map.update!(
                  :record,
                  &Map.new(&1, fn {key, value} ->
                    case value do
                      %NaiveDateTime{} = dt -> {key, NaiveDateTime.to_iso8601(dt)}
                      _ -> {key, value}
                    end
                  end)
                )
                |> Map.update!(
                  :old,
                  &Map.new(&1, fn {key, value} ->
                    case value do
                      %NaiveDateTime{} = dt -> {key, NaiveDateTime.to_iso8601(dt)}
                      _ -> {key, value}
                    end
                  end)
                )
                |> Map.put(:action, to_string(message_data.action))

              # record_data =
              #   message.data
              #   |> Map.fetch!(:record)
              #   |> Map.new(fn {key, value} ->
              #     case value do
              #       %NaiveDateTime{} = dt -> {key, NaiveDateTime.to_iso8601(dt)}
              #       _ -> {key, value}
              #     end
              #   end)

              # |> Map.take([:record, :metadata, :action])
              # |> Map.put(:old, Map.merge(message.data.record, message.data.changes))

              dbg(record_data)

              bytes = File.read!("/Users/acco/x/sequin/wasm/transform.wasm")

              {time, instance_pid} =
                :timer.tc(fn ->
                  {:ok, instance_pid} =
                    Wasmex.Components.start_link(%{bytes: bytes, wasi: %WasiP2Options{allow_http: true}})

                  instance_pid
                end)

              dbg({"load time (ms)", time / 1000})

              {time, new_data} =
                :timer.tc(fn ->
                  {:ok, new_data} = Wasmex.Components.call_function(instance_pid, "transform", [record_data])
                  new_data
                end)

              dbg({"transform time (ms)", time / 1000})

              %{message | data: new_data}
            end)

          {con_id, messages}
        end)

      dbg(messages_by_consumer_id)

      wal_events =
        execute_timed(:wal_events, fn ->
          Enum.flat_map(messages, fn message ->
            ctx.wal_pipelines
            |> Enum.filter(&Consumers.matches_message?(&1, message))
            |> Enum.map(fn pipeline ->
              wal_event(pipeline, message)
            end)
          end)
        end)

      matching_pipeline_ids = wal_events |> Enum.map(& &1.wal_pipeline_id) |> Enum.uniq()

      res =
        with {:ok, count} <-
               execute_timed(:call_consumer_message_stores, fn ->
                 call_consumer_message_stores(messages_by_consumer_id)
               end) do
          {:ok, wal_event_count} =
            execute_timed(:insert_wal_events, fn ->
              insert_wal_events(ctx, wal_events, matching_pipeline_ids)
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

      res
    end)
  end

  @high_watermark_prefix Constants.backfill_batch_high_watermark()
  @impl MessageHandlerBehaviour
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
      data: data,
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
      data: data,
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
    new_fields = fields_to_map(message.fields)

    changes =
      if message.old_fields do
        filter_changes(fields_to_map(message.old_fields), new_fields)
      else
        %{}
      end

    %ConsumerEventData{
      record: new_fields,
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

  defp call_consumer_message_stores(messages_by_consumer_id) do
    res =
      Enum.reduce_while(messages_by_consumer_id, :ok, fn {consumer_id, messages}, :ok ->
        execute_timed(:message_ledgers, fn ->
          if env() != :prod or Sequin.random(1..100) == 1 do
            all_wal_cursors = Enum.map(messages, &MessageLedgers.wal_cursor_from_message/1)
            :ok = MessageLedgers.wal_cursors_ingested(consumer_id, all_wal_cursors)
          end
        end)

        execute_timed(:put_messages, fn ->
          case put_messages(consumer_id, messages) do
            :ok -> {:cont, :ok}
            {:error, _} = error -> {:halt, error}
          end
        end)
      end)

    with :ok <- res do
      {:ok, Enum.sum_by(messages_by_consumer_id, fn {_, messages} -> length(messages) end)}
    end
  end

  @max_backoff_ms :timer.seconds(1)
  @max_attempts 5
  defp put_messages(consumer_id, messages_to_ingest, attempt \\ 1) do
    case SlotMessageStore.put_messages(consumer_id, messages_to_ingest) do
      :ok ->
        Health.put_event(:sink_consumer, consumer_id, %Event{slug: :messages_ingested, status: :success})
        :ok

      {:error, %InvariantError{code: :payload_size_limit_exceeded}} when attempt <= @max_attempts ->
        backoff = Sequin.Time.exponential_backoff(50, attempt, @max_backoff_ms)

        Logger.debug(
          "[MessageHandler] Slot message store for consumer #{consumer_id} is full. " <>
            "Backing off for #{backoff}ms before retry #{attempt + 1}/#{@max_attempts}..."
        )

        Process.sleep(backoff)
        put_messages(consumer_id, messages_to_ingest, attempt + 1)

      {:error, error} ->
        Health.put_event(:sink_consumer, consumer_id, %Event{slug: :messages_ingested, status: :fail, error: error})
        {:error, error}
    end
  end

  defp messages_by_consumer_id(%Context{} = ctx, messages) do
    Map.new(ctx.consumers, fn consumer ->
      matching_messages =
        execute_timed(:filter_matching_messages, fn ->
          Enum.filter(messages, &Consumers.matches_message?(consumer, &1))
        end)

      consumer_messages =
        execute_timed(:map_to_consumer_messages, fn ->
          Enum.map(matching_messages, fn message ->
            cond do
              consumer.message_kind == :event ->
                consumer_event(consumer, message)

              consumer.message_kind == :record and message.action == :delete ->
                # We do not propagate delete messages to record consumers
                nil

              consumer.message_kind == :record ->
                consumer_record(consumer, message)
            end
          end)
        end)

      messages =
        consumer_messages
        |> Stream.reject(&is_nil/1)
        |> Stream.reject(&violates_payload_size?(ctx.replication_slot_id, &1))
        |> Enum.to_list()

      {consumer.id, messages}
    end)
  end

  defp insert_wal_events(%Context{}, [], _matching_pipeline_ids), do: {:ok, 0}

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
        %SlotProcessor.Message{action: :update, old_fields: nil} = message, messages_acc ->
          if has_unchanged_toast?(message) do
            put_unchanged_toast_health_event(ctx, message)
          end

          [message | messages_acc]

        %SlotProcessor.Message{action: :update, fields: fields, old_fields: old_fields} = message, messages_acc ->
          updated_fields =
            Enum.map(fields, fn
              %SlotProcessor.Message.Field{value: :unchanged_toast} = field ->
                old_field = Sequin.Enum.find!(old_fields, &(&1.column_attnum == field.column_attnum))
                %{field | value: old_field.value}

              field ->
                field
            end)

          [%{message | fields: updated_fields} | messages_acc]

        %SlotProcessor.Message{} = message, messages_acc ->
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

  defp execute_timed(name, fun) do
    {time, result} = :timer.tc(fun)
    # Convert microseconds to milliseconds
    incr_counter(:"#{name}_total_ms", time / 1000)
    incr_counter(:"#{name}_count")
    result
  end

  defp incr_counter(name, amount \\ 1) do
    current = Process.get(name, 0)
    Process.put(name, current + amount)
  end

  defp env do
    Application.get_env(:sequin, :env)
  end
end

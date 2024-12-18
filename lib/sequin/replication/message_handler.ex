defmodule Sequin.Replication.MessageHandler do
  @moduledoc false
  @behaviour Sequin.Extensions.MessageHandlerBehaviour

  alias Sequin.Consumers
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerEventData
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Extensions.MessageHandlerBehaviour
  alias Sequin.Health
  alias Sequin.Replication
  alias Sequin.Replication.Message
  alias Sequin.Replication.PostgresReplicationSlot
  alias Sequin.Replication.WalPipeline
  alias Sequin.Repo
  alias Sequin.Tracer.Server, as: TracerServer

  require Logger

  defmodule Context do
    @moduledoc false
    use TypedStruct

    typedstruct do
      field :consumers, [Sequin.Consumers.consumer()], default: []
      field :wal_pipelines, [WalPipeline.t()], default: []
      field :replication_slot_id, String.t()
      field :messages, [Message.t()], default: []
    end
  end

  def context(%PostgresReplicationSlot{} = pr) do
    pr = Repo.preload(pr, [:wal_pipelines, sink_consumers: [:sequence, :postgres_database]])

    %Context{
      consumers: pr.sink_consumers,
      wal_pipelines: pr.wal_pipelines,
      replication_slot_id: pr.id
    }
  end

  @impl MessageHandlerBehaviour
  def handle_messages(%Context{} = ctx, messages) do
    Logger.debug("[MessageHandler] Handling #{length(messages)} message(s)")
    max_seq = messages |> Enum.map(& &1.seq) |> Enum.max()
    Replication.put_last_processed_seq!(ctx.replication_slot_id, max_seq)

    %Context{ctx | messages: ctx.messages ++ messages}
  end

  @impl MessageHandlerBehaviour
  def produce_messages(%Context{} = ctx, consumer_id, count) do
    consumer = Sequin.Enum.find!(ctx.consumers, &(&1.id == consumer_id))

    messages =
      ctx.messages
      |> Enum.take(count)
      |> Enum.map(fn message ->
        if Consumers.matches_message?(consumer, message) do
          cond do
            consumer.message_kind == :event ->
              {{:insert, consumer_event(consumer, message)}, consumer}

            consumer.message_kind == :record and message.action == :delete ->
              {{:delete, consumer_record(consumer, message)}, consumer}

            consumer.message_kind == :record ->
              {{:insert, consumer_record(consumer, message)}, consumer}
          end
        else
          TracerServer.message_filtered(consumer, message)
          nil
        end
      end)
      |> Enum.reject(&is_nil/1)

    Health.update(consumer, :ingestion, :healthy)

    # Trace Messages
    messages
    |> Enum.group_by(
      fn {{action, _event_or_record}, consumer} -> {action, consumer} end,
      fn {{_action, event_or_record}, _consumer} -> event_or_record end
    )
    |> Enum.each(fn
      {{:insert, consumer}, messages} -> TracerServer.messages_ingested(consumer, messages)
      {{:delete, _consumer}, _messages} -> :ok
    end)

    # Return the messages that were produced
    context = %Context{ctx | messages: Enum.drop(ctx.messages, count)}
    messages = Enum.map(messages, fn {{_action, event_or_record}, _consumer} -> event_or_record end)
    {context, messages}
  end

  defp consumer_event(consumer, message) do
    %ConsumerEvent{
      consumer_id: consumer.id,
      commit_lsn: message.commit_lsn,
      seq: message.seq,
      record_pks: Enum.map(message.ids, &to_string/1),
      table_oid: message.table_oid,
      deliver_count: 0,
      data: event_data_from_message(message, consumer),
      replication_message_trace_id: message.trace_id
    }
  end

  defp consumer_record(consumer, message) do
    %ConsumerRecord{
      consumer_id: consumer.id,
      commit_lsn: message.commit_lsn,
      seq: message.seq,
      record_pks: Enum.map(message.ids, &to_string/1),
      group_id: generate_group_id(consumer, message),
      table_oid: message.table_oid,
      deliver_count: 0,
      replication_message_trace_id: message.trace_id
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

  defp generate_group_id(consumer, message) do
    # This should be way more assertive - we should error if we don't find the source table
    # We have a lot of tests that do not line up consumer source_tables with the message table oid
    source_table = Enum.find(consumer.source_tables, &(&1.oid == message.table_oid))

    if source_table && source_table.group_column_attnums do
      Enum.map_join(source_table.group_column_attnums, ",", fn attnum ->
        field = Sequin.Enum.find!(message.fields, &(&1.column_attnum == attnum))
        to_string(field.value)
      end)
    else
      Enum.join(message.ids, ",")
    end
  end
end

defmodule Sequin.Replication.MessageHandler do
  @moduledoc false
  @behaviour Sequin.Extensions.MessageHandlerBehaviour

  alias Sequin.Consumers
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Extensions.MessageHandlerBehaviour
  alias Sequin.Replication.Message
  alias Sequin.Replication.PostgresReplicationSlot
  alias Sequin.Repo

  require Logger

  defmodule Context do
    @moduledoc false
    use TypedStruct

    typedstruct do
      field :consumers, [Sequin.Consumers.consumer()]
    end
  end

  def context(%PostgresReplicationSlot{} = pr) do
    pr = Repo.preload(pr, [:http_pull_consumers, :http_push_consumers])
    %Context{consumers: pr.http_pull_consumers ++ pr.http_push_consumers}
  end

  @impl MessageHandlerBehaviour
  def handle_messages(%Context{} = ctx, messages) do
    Logger.info("[MessageHandler] Handling #{length(messages)} message(s)")

    messages
    |> Enum.flat_map(fn message ->
      ctx.consumers
      |> Enum.map(fn consumer ->
        if Consumers.matches_message?(consumer, message) do
          Logger.info("[MessageHandler] Matched message to consumer #{consumer.id}")

          cond do
            consumer.message_kind == :event ->
              {:insert, consumer_event(consumer, message)}

            consumer.message_kind == :record and message.action == :delete ->
              {:delete, consumer_record(consumer, message)}

            consumer.message_kind == :record ->
              {:insert, consumer_record(consumer, message)}
          end
        end
      end)
      |> Enum.reject(&is_nil/1)
    end)
    |> insert_or_delete_consumer_messages()
  end

  defp consumer_event(consumer, message) do
    %ConsumerEvent{
      consumer_id: consumer.id,
      commit_lsn: DateTime.to_unix(message.commit_timestamp, :microsecond),
      record_pks: Enum.map(message.ids, &to_string/1),
      table_oid: message.table_oid,
      deliver_count: 0,
      data: event_data_from_message(message, consumer)
    }
  end

  defp consumer_record(consumer, message) do
    %ConsumerRecord{
      consumer_id: consumer.id,
      commit_lsn: DateTime.to_unix(message.commit_timestamp, :microsecond),
      record_pks: Enum.map(message.ids, &to_string/1),
      table_oid: message.table_oid,
      deliver_count: 0
    }
  end

  defp event_data_from_message(%Message{action: :insert} = message, consumer) do
    %{
      record: fields_to_map(message.fields),
      changes: nil,
      action: :insert,
      metadata: metadata(message, consumer)
    }
  end

  defp event_data_from_message(%Message{action: :update} = message, consumer) do
    changes = if message.old_fields, do: filter_changes(message.old_fields, message.fields), else: %{}

    %{
      record: fields_to_map(message.fields),
      changes: changes,
      action: :update,
      metadata: metadata(message, consumer)
    }
  end

  defp event_data_from_message(%Message{action: :delete} = message, consumer) do
    %{
      record: fields_to_map(message.old_fields),
      changes: nil,
      action: :delete,
      metadata: metadata(message, consumer)
    }
  end

  defp metadata(%Message{} = message, consumer) do
    %{
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
      if v == Map.get(new_map, k) do
        acc
      else
        Map.put(acc, k, v)
      end
    end)
    |> Map.new()
  end

  defp insert_or_delete_consumer_messages(messages) do
    Repo.transact(fn ->
      groups =
        Enum.group_by(
          messages,
          fn
            {:insert, %ConsumerEvent{}} -> :events
            {:insert, %ConsumerRecord{}} -> :record_inserts
            {:delete, %ConsumerRecord{}} -> :record_deletes
          end,
          &elem(&1, 1)
        )

      events = Enum.map(Map.get(groups, :events, []), &Sequin.Map.from_ecto/1)
      records = Enum.map(Map.get(groups, :record_inserts, []), &Sequin.Map.from_ecto/1)
      record_deletes = Enum.map(Map.get(groups, :record_deletes, []), &Sequin.Map.from_ecto/1)

      with {:ok, event_count} <- Consumers.insert_consumer_events(events),
           {:ok, record_count} <- Consumers.insert_consumer_records(records),
           {:ok, delete_count} <- Consumers.delete_consumer_records(record_deletes) do
        {:ok, event_count + record_count + delete_count}
      end
    end)
  end
end

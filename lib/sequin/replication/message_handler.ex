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
    messages
    |> Enum.flat_map(fn message ->
      ctx.consumers
      |> Enum.map(fn consumer ->
        if Consumers.matches_message?(consumer, message) do
          case consumer.message_kind do
            :event -> create_consumer_event(consumer, message)
            :record -> create_consumer_record(consumer, message)
          end
        end
      end)
      |> Enum.reject(&is_nil/1)
    end)
    |> insert_consumer_messages()
  end

  defp create_consumer_event(consumer, message) do
    %ConsumerEvent{
      consumer_id: consumer.id,
      commit_lsn: DateTime.to_unix(message.commit_timestamp, :microsecond),
      record_pks: message.ids,
      table_oid: message.table_oid,
      deliver_count: 0,
      data: create_event_data_from_message(message)
    }
  end

  defp create_consumer_record(consumer, message) do
    %ConsumerRecord{
      consumer_id: consumer.id,
      commit_lsn: DateTime.to_unix(message.commit_timestamp, :microsecond),
      record_pks: message.ids,
      table_oid: message.table_oid,
      deliver_count: 0
    }
  end

  defp create_event_data_from_message(%Message{action: :insert} = message) do
    %{
      record: fields_to_map(message.fields),
      changes: nil,
      action: :insert,
      metadata: metadata(message)
    }
  end

  defp create_event_data_from_message(%Message{action: :update} = message) do
    changes = if message.old_fields, do: filter_changes(message.old_fields, message.fields), else: %{}

    %{
      record: fields_to_map(message.fields),
      changes: changes,
      action: :update,
      metadata: metadata(message)
    }
  end

  defp create_event_data_from_message(%Message{action: :delete} = message) do
    %{
      record: fields_to_map(message.old_fields),
      changes: nil,
      action: :delete,
      metadata: metadata(message)
    }
  end

  defp metadata(%Message{} = message) do
    %{
      table: message.table_name,
      schema: message.table_schema,
      commit_timestamp: message.commit_timestamp
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

  defp insert_consumer_messages(messages) do
    Repo.transact(fn ->
      {events, records} = Enum.split_with(messages, &is_struct(&1, ConsumerEvent))

      events_map = Enum.map(events, &Sequin.Map.from_ecto/1)
      records_map = Enum.map(records, &Sequin.Map.from_ecto/1)

      with {:ok, event_count} <- Consumers.insert_consumer_events(events_map),
           {:ok, record_count} <- Consumers.insert_consumer_records(records_map) do
        {:ok, event_count + record_count}
      end
    end)
  end
end

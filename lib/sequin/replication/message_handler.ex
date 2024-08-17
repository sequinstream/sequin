defmodule Sequin.Replication.MessageHandler do
  @moduledoc false
  @behaviour Sequin.Extensions.MessageHandlerBehaviour

  alias Sequin.Consumers
  alias Sequin.Extensions.MessageHandlerBehaviour
  alias Sequin.Replication.Message
  alias Sequin.Replication.PostgresReplicationSlot

  defmodule Context do
    @moduledoc false
    use TypedStruct

    typedstruct do
      field :consumers, [Sequin.Consumers.consumer()]
    end
  end

  def context(%PostgresReplicationSlot{} = pr) do
    %Context{consumers: pr.http_pull_consumers ++ pr.http_push_consumers}
  end

  @impl MessageHandlerBehaviour
  def handle_messages(%Context{} = ctx, messages) do
    messages
    |> Enum.flat_map(fn message ->
      ctx.consumers
      |> Enum.map(fn consumer ->
        if Consumers.matches_message?(consumer, message) do
          Sequin.Map.from_ecto(%Sequin.Consumers.ConsumerEvent{
            consumer_id: consumer.id,
            commit_lsn: DateTime.to_unix(message.commit_timestamp, :microsecond),
            record_pks: message.ids,
            table_oid: message.table_oid,
            deliver_count: 0,
            data: create_data_from_message(message)
          })
        end
      end)
      |> Enum.reject(&is_nil/1)
    end)
    |> Consumers.insert_consumer_events()
  end

  defp create_data_from_message(%Message{action: :insert} = message) do
    %{
      record: fields_to_map(message.fields),
      changes: nil,
      action: :insert,
      metadata: metadata(message)
    }
  end

  defp create_data_from_message(%Message{action: :update} = message) do
    changes = if message.old_fields, do: filter_changes(message.old_fields, message.fields), else: %{}

    %{
      record: fields_to_map(message.fields),
      changes: changes,
      action: :update,
      metadata: metadata(message)
    }
  end

  defp create_data_from_message(%Message{action: :delete} = message) do
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
end

defmodule Sequin.Replication.PostgresReplicationMessageHandler do
  @moduledoc false
  @behaviour Sequin.Extensions.ReplicationMessageHandler

  alias Sequin.Extensions.PostgresAdapter.Changes.DeletedRecord
  alias Sequin.Extensions.PostgresAdapter.Changes.NewRecord
  alias Sequin.Extensions.PostgresAdapter.Changes.UpdatedRecord
  alias Sequin.Extensions.ReplicationMessageHandler
  alias Sequin.Replication.PostgresReplication
  alias Sequin.Streams

  defmodule Context do
    @moduledoc false
    use TypedStruct

    typedstruct do
      field :stream_id, String.t()
      field :key_prefix, String.t()
      field :key_format, :basic | :with_operation
    end
  end

  def context(%PostgresReplication{} = pr) do
    %Context{
      stream_id: pr.stream_id,
      key_prefix: pr.postgres_database.name,
      key_format: pr.key_format
    }
  end

  @impl ReplicationMessageHandler
  def handle_message(%Context{} = ctx, message) do
    message = message_for_upsert(ctx.key_prefix, message, ctx.key_format)
    Streams.upsert_messages(ctx.stream_id, [message])
  end

  defp message_for_upsert(key_prefix, %DeletedRecord{old_record: old_record} = message, key_format) do
    %{
      key: key_from_message(key_prefix, message, message.ids, key_format),
      data:
        Jason.encode!(%{
          data: old_record,
          deleted: true,
          changes: %{},
          action: "delete"
        })
    }
  end

  # InsertRecord
  defp message_for_upsert(key_prefix, %NewRecord{record: record} = message, key_format) do
    %{
      key: key_from_message(key_prefix, message, message.ids, key_format),
      data:
        Jason.encode!(%{
          data: record,
          deleted: false,
          changes: %{},
          action: "insert"
        })
    }
  end

  # UpdateRecord
  defp message_for_upsert(key_prefix, %UpdatedRecord{record: record, old_record: old_record} = message, key_format) do
    changes = if old_record, do: filter_changes(old_record, record), else: %{}

    %{
      key: key_from_message(key_prefix, message, message.ids, key_format),
      data:
        Jason.encode!(%{
          data: record,
          deleted: false,
          changes: changes,
          action: "update"
        })
    }
  end

  defp filter_changes(old_record, new_record) do
    old_record
    |> Enum.reduce(%{}, fn {k, v}, acc ->
      if v == Map.get(new_record, k) do
        acc
      else
        Map.put(acc, k, v)
      end
    end)
    |> Map.new()
  end

  defp key_from_message(key_prefix, message, record_ids, key_format) do
    case key_format do
      :with_operation ->
        Enum.join(
          [
            key_prefix,
            Sequin.Key.to_key_token(message.schema),
            Sequin.Key.to_key_token(message.table),
            action(message),
            Enum.join(record_ids, ".")
          ],
          "."
        )

      :basic ->
        Enum.join(
          [
            key_prefix,
            Sequin.Key.to_key_token(message.schema),
            Sequin.Key.to_key_token(message.table),
            Enum.join(record_ids, ".")
          ],
          "."
        )
    end
  end

  defp action(message) do
    case message do
      %NewRecord{} -> "insert"
      %UpdatedRecord{} -> "update"
      %DeletedRecord{} -> "delete"
    end
  end
end

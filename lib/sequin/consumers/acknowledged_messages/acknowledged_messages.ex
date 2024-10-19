defmodule Sequin.Consumers.AcknowledgedMessages do
  @moduledoc false

  alias Sequin.Consumers.AcknowledgedMessages.AcknowledgedMessage
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Error

  @max_messages 10_000

  @doc """
  Stores messages for a given consumer_id using a Redis sorted set for IDs and a hash for message data.
  Trims to the latest @max_messages.
  """
  @spec store_messages(String.t(), list(ConsumerEvent.t() | ConsumerRecord.t()), non_neg_integer()) ::
          :ok | {:error, Error.t()}
  def store_messages(consumer_id, messages, max_messages \\ @max_messages) do
    now = :os.system_time(:nanosecond)
    sorted_set_key = "acknowledged_messages:#{consumer_id}:ids"
    hash_key = "acknowledged_messages:#{consumer_id}:data"

    commands =
      messages
      |> Enum.map(&to_acknowledged_message/1)
      |> Enum.flat_map(fn message ->
        encoded_message = AcknowledgedMessage.encode(message)

        [
          ["ZADD", sorted_set_key, now, message.id],
          ["HSET", hash_key, message.id, encoded_message]
        ]
      end)

    commands =
      commands ++
        [
          ["ZREMRANGEBYRANK", sorted_set_key, 0, -(max_messages + 1)],
          ["ZRANGE", sorted_set_key, 0, 0],
          ["HKEYS", hash_key]
        ]

    case Redix.pipeline(:redix, commands) do
      {:ok, results} ->
        [_zadd_results, _hset_results, _zremrange_result, [oldest_id], all_hash_keys] = Enum.take(results, -5)
        cleanup_hash(hash_key, all_hash_keys, [oldest_id])
        :ok

      error ->
        handle_response(error)
    end
  end

  @doc """
  Fetches messages for a given consumer_id using the sorted set for IDs and hash for message data.
  """
  @spec fetch_messages(String.t(), non_neg_integer(), non_neg_integer()) ::
          {:ok, list(AcknowledgedMessage.t())} | {:error, Error.t()}
  def fetch_messages(consumer_id, count \\ 100, offset \\ 0) do
    sorted_set_key = "acknowledged_messages:#{consumer_id}:ids"
    hash_key = "acknowledged_messages:#{consumer_id}:data"

    commands = [
      ["ZREVRANGE", sorted_set_key, offset, offset + count - 1],
      ["HMGET", hash_key | Enum.to_list(offset..(offset + count - 1))]
    ]

    case Redix.pipeline(:redix, commands) do
      {:ok, [ids, messages]} ->
        decoded_messages =
          Enum.map(Enum.zip(ids, messages), fn {id, message} ->
            case message do
              nil -> nil
              _ -> AcknowledgedMessage.decode(message)
            end
          end)

        {:ok, Enum.reject(decoded_messages, &is_nil/1)}

      error ->
        handle_response(error)
    end
  end

  @doc """
  Counts the number of acknowledged messages for a given consumer_id.
  """
  @spec count_messages(String.t()) :: {:ok, non_neg_integer()} | {:error, Error.t()}
  def count_messages(consumer_id) do
    key = "acknowledged_messages:#{consumer_id}:ids"

    :redix
    |> Redix.command(["ZCARD", key])
    |> handle_response()
  end

  @spec handle_response(any()) :: {:ok, any()} | {:error, Error.t()}
  defp handle_response({:ok, response}), do: {:ok, response}

  defp handle_response({:error, error}) when is_exception(error) do
    {:error, Error.service(service: :redis, message: Exception.message(error))}
  end

  defp handle_response({:error, error}) do
    {:error, Error.service(service: :redis, message: "Redis error: #{inspect(error)}")}
  end

  defp to_acknowledged_message(%ConsumerRecord{} = record) do
    %AcknowledgedMessage{
      id: record.id,
      consumer_id: record.consumer_id,
      commit_lsn: record.commit_lsn,
      ack_id: record.ack_id,
      deliver_count: record.deliver_count,
      last_delivered_at: record.last_delivered_at,
      record_pks: record.record_pks,
      table_oid: record.table_oid,
      not_visible_until: record.not_visible_until,
      inserted_at: record.inserted_at,
      trace_id: record.replication_message_trace_id
    }
  end

  defp to_acknowledged_message(%ConsumerEvent{} = event) do
    %AcknowledgedMessage{
      id: event.id,
      consumer_id: event.consumer_id,
      commit_lsn: event.commit_lsn,
      ack_id: event.ack_id,
      deliver_count: event.deliver_count,
      last_delivered_at: event.last_delivered_at,
      record_pks: event.record_pks,
      table_oid: event.table_oid,
      not_visible_until: event.not_visible_until,
      inserted_at: event.inserted_at,
      trace_id: event.replication_message_trace_id
    }
  end

  defp cleanup_hash(hash_key, all_hash_keys, ids_to_keep) do
    keys_to_remove = all_hash_keys -- ids_to_keep

    if length(keys_to_remove) > 0 do
      Redix.command(:redix, ["HDEL", hash_key | keys_to_remove])
    end
  end
end

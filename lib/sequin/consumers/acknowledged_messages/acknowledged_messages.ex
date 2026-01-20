defmodule Sequin.Consumers.AcknowledgedMessages do
  @moduledoc false

  alias Sequin.Consumers.AcknowledgedMessages.AcknowledgedMessage
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Error
  alias Sequin.Redis

  @max_messages 1_000

  @doc """
  Stores messages for a given consumer_id in a Redis sorted set and trims to the latest @max_messages.
  """
  @spec store_messages(String.t(), list(ConsumerEvent.t()), non_neg_integer()) ::
          :ok | {:error, Error.t()}
  def store_messages(consumer_id, messages, max_messages \\ @max_messages) do
    # Add messages to the sorted set
    commands =
      messages
      |> Stream.map(&to_acknowledged_message/1)
      |> Enum.flat_map(fn message ->
        score = message.commit_lsn * 1_000_000_000 + message.commit_idx
        [score, AcknowledgedMessage.encode(message)]
      end)

    # Trim sorted set to the latest @max_messages
    commands = [["ZADD", key(consumer_id) | commands]] ++ [["ZREMRANGEBYRANK", key(consumer_id), 0, -(max_messages + 1)]]
    query_name = "acknowledged_messages:store_messages"

    commands
    |> Redis.pipeline(query_name: query_name)
    |> case do
      {:ok, _} -> :ok
      error -> error
    end
  end

  @doc """
  Fetches messages for a given consumer_id from a Redis sorted set, sorted by descending score.
  """
  @spec fetch_messages(String.t(), non_neg_integer(), non_neg_integer()) ::
          {:ok, list(AcknowledgedMessage.t())} | {:error, Error.t()}
  def fetch_messages(consumer_id, count \\ 100, offset \\ 0) do
    ["ZREVRANGE", key(consumer_id), offset, offset + count - 1]
    |> Redis.command(query_name: "acknowledged_messages:fetch")
    |> case do
      {:ok, messages} -> {:ok, Enum.map(messages, &AcknowledgedMessage.decode/1)}
      error -> error
    end
  end

  @doc """
  Counts the number of acknowledged messages for a given consumer_id.
  """
  @spec count_messages(String.t()) :: {:ok, non_neg_integer()} | {:error, Error.t()}
  def count_messages(consumer_id) do
    case Redis.command(["ZCARD", key(consumer_id)], query_name: "acknowledged_messages:count") do
      {:ok, count} -> {:ok, String.to_integer(count)}
      error -> error
    end
  end

  defp key(consumer_id) do
    "acknowledged_messages:v0:#{consumer_id}"
  end

  def to_acknowledged_message(%ConsumerEvent{} = event) do
    deliver_count = if event.deliver_count == 0, do: 1, else: event.deliver_count

    %AcknowledgedMessage{
      id: event.id,
      consumer_id: event.consumer_id,
      seq: event.commit_lsn + event.commit_idx,
      commit_lsn: event.commit_lsn,
      commit_idx: event.commit_idx,
      commit_timestamp: event.data.metadata.commit_timestamp,
      ack_id: event.ack_id,
      deliver_count: deliver_count,
      last_delivered_at: event.last_delivered_at,
      record_pks: event.record_pks,
      table_oid: event.table_oid,
      not_visible_until: event.not_visible_until,
      inserted_at: event.inserted_at,
      trace_id: event.replication_message_trace_id,
      state: event.state,
      table_name: event.data.metadata.table_name,
      table_schema: event.data.metadata.table_schema
    }
  end
end

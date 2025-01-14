defmodule Sequin.Consumers.AcknowledgedMessages do
  @moduledoc false

  alias Sequin.Consumers.AcknowledgedMessages.AcknowledgedMessage
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Error
  alias Sequin.Redis

  @max_messages 1_000

  @doc """
  Stores messages for a given consumer_id in a Redis sorted set and trims to the latest @max_messages.
  """
  @spec store_messages(String.t(), list(ConsumerEvent.t() | ConsumerRecord.t()), non_neg_integer()) ::
          :ok | {:error, Error.t()}
  def store_messages(consumer_id, messages, max_messages \\ @max_messages) do
    key = "acknowledged_messages:#{consumer_id}"
    now = :os.system_time(:nanosecond)

    # Add messages to the sorted set
    commands =
      messages
      |> Stream.map(&to_acknowledged_message/1)
      |> Stream.map(&AcknowledgedMessage.encode/1)
      |> Enum.map(fn message -> ["ZADD", key, now, message] end)

    # Trim sorted set to the latest @max_messages
    commands = commands ++ [["ZREMRANGEBYRANK", key, 0, -(max_messages + 1)]]

    commands
    |> Redis.pipeline()
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
    key = "acknowledged_messages:#{consumer_id}"

    ["ZREVRANGE", key, offset, offset + count - 1]
    |> Redis.command()
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
    key = "acknowledged_messages:#{consumer_id}"

    case Redis.command(["ZCARD", key]) do
      {:ok, count} -> {:ok, String.to_integer(count)}
      error -> error
    end
  end

  def to_acknowledged_message(%ConsumerRecord{} = record) do
    %AcknowledgedMessage{
      id: record.id,
      consumer_id: record.consumer_id,
      seq: record.seq,
      commit_lsn: record.commit_lsn,
      commit_timestamp: record.data.metadata.commit_timestamp,
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

  def to_acknowledged_message(%ConsumerEvent{} = event) do
    %AcknowledgedMessage{
      id: event.id,
      consumer_id: event.consumer_id,
      seq: event.seq,
      commit_lsn: event.commit_lsn,
      commit_timestamp: event.data.metadata.commit_timestamp,
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
end

defmodule Sequin.Runtime.MessageLedgers do
  @moduledoc """
  Handles idempotency and at-least-once delivery guarantees for consumers.

  For idempotency:
  After successfully delivering messages to a sink destination, the first thing we do
  is mark the messages as delivered via this module.

  The last thing we do *before* delivering a message is to check if we have previously marked
  the message as delivered. If so, we skip deliverying it and consider it delivered.

  We trim the set of acknowledged messages below some threshold to reclaim memory.

  For at-least-once delivery:
  As messages are fanned out to SlotMessageStores, WAL cursors are written to a Redis sorted set
  with the commit timestamp as the score. When messages are acknowledged, the corresponding WAL
  cursors are removed.

  Remaining WAL cursors in the sorted set that are older than a certain threshold may indicate
  missed deliveries, which can be verified against the ConsumerEvent tables.
  """

  alias Sequin.Error
  alias Sequin.Redis
  alias Sequin.Replication

  require Logger

  @type consumer_id :: String.t()

  @doc """
  Called when WAL cursors enter a consumer's buffer. We use these later to track/verify ALO delivery.
  """
  @spec wal_cursors_ingested(consumer_id(), [Replication.wal_cursor()]) :: :ok | {:error, Error.t()}
  def wal_cursors_ingested(_, []), do: :ok

  def wal_cursors_ingested(consumer_id, wal_cursors) do
    if subsample?() do
      ingested_at = Sequin.utc_now()

      score_value_pairs =
        Enum.flat_map(wal_cursors, fn wal_cursor ->
          [
            DateTime.to_unix(ingested_at, :second),
            member_from_wal_cursor(wal_cursor)
          ]
        end)

      query_name = "message_ledgers:wal_cursors_ingested"

      case Redis.command(["ZADD", undelivered_cursors_key(consumer_id) | score_value_pairs], query_name: query_name) do
        {:ok, _results} -> :ok
        {:error, error} -> {:error, error}
      end
    else
      :ok
    end
  end

  @doc """
  Called when WAL cursors are delivered to a consumer. We:
  - Remove the WAL cursor from the ingested set
  - Add the WAL cursor to the delivered set
  """
  @spec wal_cursors_delivered(consumer_id(), [Replication.wal_cursor()]) :: :ok | {:error, Error.t()}
  def wal_cursors_delivered(_, []), do: :ok

  def wal_cursors_delivered(consumer_id, wal_cursors) do
    query_name = "message_ledgers:wal_cursors_delivered"

    members = Enum.map(wal_cursors, &member_from_wal_cursor/1)
    score_member_pairs = Enum.flat_map(wal_cursors, &[score_from_wal_cursor(&1), member_from_wal_cursor(&1)])

    [
      ["ZREM", undelivered_cursors_key(consumer_id) | members],
      ["ZADD", delivered_cursors_key(consumer_id) | score_member_pairs]
    ]
    |> Redis.pipeline(query_name: query_name)
    |> case do
      {:ok, _} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Takes a list of wal_cursors, and filters them out based on which ones have been delivered.

  The point of maintaining the delivered wal cursors set is to power this function.
  This function is what gives us idempotency / ~exactly-once delivery.
  """
  @spec filter_delivered_wal_cursors(consumer_id(), [Replication.wal_cursor()]) ::
          {:ok, [Replication.wal_cursor()]} | {:error, Error.t()}
  def filter_delivered_wal_cursors(_, []), do: {:ok, []}

  def filter_delivered_wal_cursors(consumer_id, wal_cursors) do
    encoded_wal_cursors = Enum.map(wal_cursors, &member_from_wal_cursor/1)
    query_name = "message_ledgers:filter_delivered_wal_cursors"

    with {:ok, results} <-
           Redis.command(["ZMSCORE", delivered_cursors_key(consumer_id) | encoded_wal_cursors], query_name: query_name) do
      # Convert results to integers, filtering out nils (non-delivered messages)
      delivered_wal_cursors =
        results
        |> Enum.zip(wal_cursors)
        |> Stream.reject(fn {result, _} -> is_nil(result) end)
        |> Enum.map(fn {_result, wal_cursor} -> wal_cursor end)

      {:ok, delivered_wal_cursors}
    end
  end

  @doc """
  Trims the delivered_cursors set to a certain WAL cursor.

  It is safe to trim this set whenever we advanced the replication slot. That's because we won't see a replay of any already-delivered message for any LSN/wal cursor from before the new cursor in the replication slot. We'll only see:
  - Messages *after* that LSN
  - Older messages before that LSN, but are undelivered and flushed to disk (and therefore, as they are undelivered, are safe to deliver!)
  """
  @spec trim_delivered_cursors_set(consumer_id(), Replication.wal_cursor()) :: :ok | {:error, Error.t()}
  def trim_delivered_cursors_set(consumer_id, wal_cursor) do
    key = delivered_cursors_key(consumer_id)
    query_name = "message_ledgers:trim_delivered_cursors_set"

    with {:ok, initial_size} <- Redis.command(["ZCARD", key], query_name: query_name),
         {:ok, trimmed} <-
           Redis.command(["ZREMRANGEBYSCORE", key, "-inf", "(#{score_from_wal_cursor(wal_cursor)}"],
             query_name: query_name
           ),
         {:ok, final_size} <- Redis.command(["ZCARD", key], query_name: query_name) do
      Logger.debug("[MessageLedgers] Trimmed delivered_cursors set to: #{inspect(wal_cursor)}",
        commit: wal_cursor,
        consumer_id: consumer_id,
        initial_size: initial_size,
        records_removed: trimmed,
        final_size: final_size
      )

      {:ok, String.to_integer(trimmed)}
    else
      {:error, error} when is_exception(error) ->
        Logger.error("[MessageLedgers] Error trimming delivered_cursors set", error: error)
    end
  end

  @doc """
  We track ingested cursors in order to verify the ALO properties of our system.

  An undelivered cursor means one of three things:

  1. We just ingested it, but haven't delivered it yet.
  2. We ingested a message a while ago, but it's failing to deliver.
  3. There is a bad bug in our system that we must fix immediately. (An ingested message was dropped on the floor at some point.)

  If an undelivered cursor is *stale* (older than some timestamp), then it is either in bucket 2 or 3.

  We can use this function to find all stale undelivered cursors. Then, we can filter out cursors that are in bucket 2 by filtering out any cursors that are in the ConsumerEvent tables (i.e. failing to deliver).

  Any remaining cursors are in bucket 3, and are undelivered because of a bug. The hope is that we can use this functionality to catch these in QA and fix before shipping to prod. Worst case, we catch them via prod monitoring, and fix ASAP.
  """
  @spec count_undelivered_wal_cursors(consumer_id()) :: {:ok, non_neg_integer()} | {:error, Error.t()}
  def count_undelivered_wal_cursors(consumer_id) do
    query_name = "message_ledgers:count_undelivered_wal_cursors"

    case Redis.command(["ZCARD", undelivered_cursors_key(consumer_id)], query_name: query_name) do
      {:ok, count} ->
        {:ok, String.to_integer(count)}

      {:error, error} ->
        {:error, error}
    end
  end

  @spec count_undelivered_wal_cursors(consumer_id(), DateTime.t()) :: {:ok, non_neg_integer()} | {:error, Error.t()}
  def count_undelivered_wal_cursors(consumer_id, older_than_timestamp) do
    older_than_timestamp = DateTime.to_unix(older_than_timestamp, :second)
    query_name = "message_ledgers:count_undelivered_wal_cursors"

    case Redis.command(["ZCOUNT", undelivered_cursors_key(consumer_id), "-inf", older_than_timestamp],
           query_name: query_name
         ) do
      {:ok, count} ->
        {:ok, String.to_integer(count)}

      {:error, error} ->
        {:error, error}
    end
  end

  @doc """
  Trims the ingested cursors set to a certain timestamp.

  See above for more about stale cursors. We can trim this set after we've audited the stale cursors with `list_undelivered_wal_cursors`.
  """
  @spec trim_stale_undelivered_wal_cursors(consumer_id(), DateTime.t()) :: :ok | {:error, Error.t()}
  def trim_stale_undelivered_wal_cursors(consumer_id, older_than_timestamp) do
    key = undelivered_cursors_key(consumer_id)
    older_than_timestamp = DateTime.to_unix(older_than_timestamp, :second)
    query_name = "message_ledgers:trim_stale_undelivered_wal_cursors"

    with {:ok, initial_size} <- Redis.command(["ZCARD", key], query_name: query_name),
         {:ok, trimmed} <- Redis.command(["ZREMRANGEBYSCORE", key, "-inf", older_than_timestamp], query_name: query_name),
         {:ok, final_size} <- Redis.command(["ZCARD", key], query_name: query_name) do
      Logger.info("[MessageLedgers] Trimmed commits older than #{older_than_timestamp}",
        consumer_id: consumer_id,
        initial_size: initial_size,
        records_removed: trimmed,
        final_size: final_size
      )

      :ok
    else
      {:error, error} when is_exception(error) ->
        Logger.error("[MessageLedgers] Error trimming commits", error: error)
        {:error, error}
    end
  end

  @spec count_delivered_cursors_set(consumer_id()) :: {:ok, non_neg_integer()} | {:error, Error.t()}
  def count_delivered_cursors_set(consumer_id) do
    key = delivered_cursors_key(consumer_id)
    query_name = "message_ledgers:count_delivered_cursors_set"

    case Redis.command(["ZCARD", key], query_name: query_name) do
      {:ok, count} -> {:ok, String.to_integer(count)}
      {:error, error} -> {:error, error}
    end
  end

  @spec drop_delivered_cursors_set(consumer_id()) :: :ok | {:error, Error.t()}
  def drop_delivered_cursors_set(consumer_id) do
    key = delivered_cursors_key(consumer_id)
    query_name = "message_ledgers:drop_delivered_cursors_set"

    with {:ok, _} <- Redis.command(["DEL", key], query_name: query_name) do
      :ok
    end
  end

  @spec drop_verification_set(consumer_id()) :: :ok | {:error, Error.t()}
  def drop_verification_set(consumer_id) do
    key = undelivered_cursors_key(consumer_id)
    query_name = "message_ledgers:drop_verification_set"

    with {:ok, _} <- Redis.command(["DEL", key], query_name: query_name) do
      :ok
    end
  end

  @spec drop_for_consumer(consumer_id()) :: :ok | {:error, Error.t()}
  def drop_for_consumer(consumer_id) do
    with :ok <- drop_delivered_cursors_set(consumer_id) do
      drop_verification_set(consumer_id)
    end
  end

  defp member_from_wal_cursor(%{commit_lsn: commit_lsn, commit_idx: commit_idx}), do: "#{commit_lsn}:#{commit_idx}"
  defp score_from_wal_cursor(%{commit_lsn: commit_lsn, commit_idx: commit_idx}), do: commit_lsn + commit_idx

  defp delivered_cursors_key(consumer_id), do: "consumer:#{consumer_id}:consumer_idempotency"
  defp undelivered_cursors_key(consumer_id), do: "consumer:#{consumer_id}:commit_verification"

  @doc """
  Called when WAL cursors reach a specific checkpoint in processing. Similar to wal_cursors_ingested,
  but allows tracking cursors at arbitrary checkpoints.
  """
  @spec wal_cursors_reached_checkpoint(consumer_id(), String.t(), [Replication.wal_cursor()]) :: :ok | {:error, Error.t()}
  def wal_cursors_reached_checkpoint(_, _, []), do: :ok

  def wal_cursors_reached_checkpoint(consumer_id, checkpoint, wal_cursors) do
    if env() == :dev and auditing?() do
      reached_at = Sequin.utc_now()
      query_name = "message_ledgers:wal_cursors_reached_checkpoint"

      score_value_pairs =
        Enum.flat_map(wal_cursors, fn wal_cursor ->
          [
            DateTime.to_unix(reached_at, :second),
            member_from_wal_cursor(wal_cursor)
          ]
        end)

      case Redis.command(["ZADD", checkpoint_key(consumer_id, checkpoint) | score_value_pairs], query_name: query_name) do
        {:ok, _results} -> :ok
        {:error, error} -> {:error, error}
      end
    else
      :ok
    end
  end

  @doc """
  Returns the count of WAL cursors at a specific checkpoint for a consumer.
  """
  @spec count_in_checkpoint(consumer_id(), String.t()) :: {:ok, non_neg_integer()} | {:error, Error.t()}
  def count_in_checkpoint(consumer_id, checkpoint) do
    key = checkpoint_key(consumer_id, checkpoint)

    case Redis.command(["ZCARD", key]) do
      {:ok, nil} -> {:ok, Error.not_found(entity: "checkpoint", params: %{checkpoint: checkpoint})}
      {:ok, count} -> {:ok, String.to_integer(count)}
      {:error, error} -> {:error, error}
    end
  end

  def clear_checkpoint_set(consumer_id, checkpoint) do
    key = checkpoint_key(consumer_id, checkpoint)

    with {:ok, _} <- Redis.command(["DEL", key]) do
      :ok
    end
  end

  def wal_cursor_from_message(message), do: Map.take(message, [:commit_lsn, :commit_idx])

  defp checkpoint_key(consumer_id, checkpoint), do: "consumer:#{consumer_id}:checkpoint:#{checkpoint}"

  defp subsample?, do: env() != :prod or Enum.random(1..10) == 1

  # set to true to enable verbose ledger checkpoints (dev only)
  defp auditing?, do: false
  defp env, do: Application.get_env(:sequin, :env)
end

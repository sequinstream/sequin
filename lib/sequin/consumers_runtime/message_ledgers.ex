defmodule Sequin.ConsumersRuntime.MessageLedgers do
  @moduledoc """
  Handles idempotency for consumers at "the leaf", i.e. at delivery time.

  After successfully delivering messages to a sink destination, the first thing we do
  is mark the messages as delivered via this module.

  The last thing we do *before* delivering a message is to check if we have previously marked
  the message as delivered. If so, we skip deliverying it and consider it delivered.

  We trim the set of acknowledged messages below some threshold to reclaim memory.
  """

  @moduledoc """
  Verifies at-least-once delivery guarantees by tracking commit tuples in Redis.

  As messages are fanned out to SlotMessageStores, commit tuples are written to a Redis sorted set
  with the commit timestamp as the score. When messages are acknowledged, the corresponding commit
  tuples are removed.

  Remaining commit tuples in the sorted set that are older than a certain threshold may indicate
  missed deliveries, which can be verified against the ConsumerEvent/ConsumerRecord tables.
  """

  alias Sequin.Error
  alias Sequin.Redis

  require Logger

  @type consumer_id :: String.t()
  @type commit :: %{commit_lsn: integer(), commit_idx: integer()}

  @spec mark_messages_delivered(consumer_id(), [commit()]) :: :ok | {:error, Error.t()}
  def mark_messages_delivered(_, []), do: :ok

  def mark_messages_delivered(consumer_id, commits) do
    commits
    |> Enum.map(fn commit ->
      ["ZADD", delivered_key(consumer_id), score_from_commit(commit), member_from_commit(commit)]
    end)
    |> Redis.pipeline()
    |> case do
      {:ok, _} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  @spec delivered_messages(consumer_id(), [commit()]) :: {:ok, [commit()]} | {:error, Error.t()}
  def delivered_messages(_, []), do: {:ok, []}

  def delivered_messages(consumer_id, commits) do
    encoded_commits = Enum.map(commits, &member_from_commit/1)

    with {:ok, results} <- Redis.command(["ZMSCORE", delivered_key(consumer_id) | encoded_commits]) do
      # Convert results to integers, filtering out nils (non-delivered messages)
      delivered_commits =
        results
        |> Enum.zip(commits)
        |> Stream.reject(fn {result, _} -> is_nil(result) end)
        |> Enum.map(fn {_result, commit} -> commit end)

      {:ok, delivered_commits}
    end
  end

  @spec trim(consumer_id(), commit()) :: :ok | {:error, Error.t()}
  def trim(consumer_id, commit) do
    key = delivered_key(consumer_id)

    with {:ok, initial_size} <- Redis.command(["ZCARD", key]),
         {:ok, trimmed} <- Redis.command(["ZREMRANGEBYSCORE", key, "-inf", "(#{score_from_commit(commit)}"]),
         {:ok, final_size} <- Redis.command(["ZCARD", key]) do
      Logger.info("[ConsumerIdempotency] Trimmed set to: #{inspect(commit)}",
        commit: commit,
        consumer_id: consumer_id,
        initial_size: initial_size,
        records_removed: trimmed,
        final_size: final_size
      )

      {:ok, String.to_integer(trimmed)}
    else
      {:error, error} when is_exception(error) ->
        Logger.error("[ConsumerIdempotency] Error trimming set", error: error)
    end
  end

  defp delivered_key(consumer_id), do: "consumer:#{consumer_id}:consumer_idempotency"
  defp member_from_commit(%{commit_lsn: commit_lsn, commit_idx: commit_idx}), do: "#{commit_lsn}:#{commit_idx}"
  defp score_from_commit(%{commit_lsn: commit_lsn, commit_idx: commit_idx}), do: commit_lsn + commit_idx

  ## ALO

  @type consumer_id :: String.t()
  @type commit_timestamp :: DateTime.t()
  @type commit :: %{commit_lsn: integer(), commit_idx: integer(), commit_timestamp: commit_timestamp()}

  @spec record_commit_tuples(consumer_id(), [commit()]) :: :ok | {:error, Error.t()}
  def record_commit_tuples(_, []), do: :ok

  def record_commit_tuples(consumer_id, commits) do
    key = commit_key(consumer_id)

    commands =
      Enum.map(commits, fn commit ->
        ["ZADD", key, DateTime.to_unix(commit.commit_timestamp, :second), "#{commit.commit_lsn}:#{commit.commit_idx}"]
      end)

    case Redis.pipeline(commands) do
      {:ok, _results} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  @spec remove_commit_tuples(consumer_id(), [commit()]) :: :ok | {:error, Error.t()}
  def remove_commit_tuples(_, []), do: :ok

  def remove_commit_tuples(consumer_id, commits) do
    key = commit_key(consumer_id)

    commands = Enum.map(commits, fn commit -> ["ZREM", key, "#{commit.commit_lsn}:#{commit.commit_idx}"] end)

    case Redis.pipeline(commands) do
      {:ok, _results} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  @spec get_unverified_commit_tuples(consumer_id(), commit_timestamp()) :: {:ok, [commit()]} | {:error, Error.t()}
  def get_unverified_commit_tuples(consumer_id, older_than_timestamp) do
    key = commit_key(consumer_id)
    older_than_timestamp = DateTime.to_unix(older_than_timestamp, :second)

    with {:ok, results} <- Redis.command(["ZRANGEBYSCORE", key, "-inf", older_than_timestamp, "WITHSCORES"]) do
      commits =
        results
        |> Enum.chunk_every(2)
        |> Enum.map(fn [member, score] ->
          [lsn, idx] = String.split(member, ":")

          %{
            commit_lsn: String.to_integer(lsn),
            commit_idx: String.to_integer(idx),
            commit_timestamp: DateTime.from_unix!(String.to_integer(score), :second)
          }
        end)

      {:ok, commits}
    end
  end

  @spec trim_commit_tuples(consumer_id(), commit_timestamp()) :: :ok | {:error, Error.t()}
  def trim_commit_tuples(consumer_id, older_than_timestamp) do
    key = commit_key(consumer_id)
    older_than_timestamp = DateTime.to_unix(older_than_timestamp, :second)

    with {:ok, initial_size} <- Redis.command(["ZCARD", key]),
         {:ok, trimmed} <- Redis.command(["ZREMRANGEBYSCORE", key, "-inf", older_than_timestamp]),
         {:ok, final_size} <- Redis.command(["ZCARD", key]) do
      Logger.info("[AtLeastOnceVerification] Trimmed commits older than #{older_than_timestamp}",
        consumer_id: consumer_id,
        initial_size: initial_size,
        records_removed: trimmed,
        final_size: final_size
      )

      :ok
    else
      {:error, error} when is_exception(error) ->
        Logger.error("[AtLeastOnceVerification] Error trimming commits", error: error)
        {:error, error}
    end
  end

  @spec all_commit_tuples(consumer_id()) :: {:ok, [commit()]} | {:error, Error.t()}
  def all_commit_tuples(consumer_id) do
    key = commit_key(consumer_id)

    with {:ok, results} <- Redis.command(["ZRANGE", key, 0, -1, "WITHSCORES"]) do
      commits =
        results
        |> Enum.chunk_every(2)
        |> Enum.map(fn [member, score] ->
          [lsn, idx] = String.split(member, ":")

          %{
            commit_lsn: String.to_integer(lsn),
            commit_idx: String.to_integer(idx),
            commit_timestamp: DateTime.from_unix!(String.to_integer(score), :second)
          }
        end)

      {:ok, commits}
    end
  end

  @spec count_commit_tuples(consumer_id()) :: {:ok, non_neg_integer()} | {:error, Error.t()}
  def count_commit_tuples(consumer_id) do
    key = commit_key(consumer_id)

    case Redis.command(["ZCARD", key]) do
      {:ok, count} -> {:ok, String.to_integer(count)}
      {:error, error} -> {:error, error}
    end
  end

  @spec delete_commit_tuples(consumer_id()) :: :ok | {:error, Error.t()}
  def delete_commit_tuples(consumer_id) do
    key = commit_key(consumer_id)

    with {:ok, _} <- Redis.command(["DEL", key]) do
      :ok
    end
  end

  defp commit_key(consumer_id), do: "consumer:#{consumer_id}:commit_verification"
end

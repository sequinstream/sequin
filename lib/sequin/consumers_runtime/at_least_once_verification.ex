defmodule Sequin.ConsumersRuntime.AtLeastOnceVerification do
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
  # {xid, lsn}
  @type commit_tuple :: {integer(), integer()}
  @type commit_timestamp :: integer()

  @spec record_commit(consumer_id(), commit_tuple(), commit_timestamp()) :: :ok | {:error, Error.t()}
  def record_commit(consumer_id, {xid, lsn}, timestamp) do
    key = commit_key(consumer_id)
    member = "#{xid}:#{lsn}"

    case Redis.command(["ZADD", key, timestamp, member]) do
      {:ok, _} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  @spec remove_commit(consumer_id(), commit_tuple()) :: :ok | {:error, Error.t()}
  def remove_commit(consumer_id, {xid, lsn}) do
    key = commit_key(consumer_id)
    member = "#{xid}:#{lsn}"

    case Redis.command(["ZREM", key, member]) do
      {:ok, _} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  @spec get_unverified_commits(consumer_id(), commit_timestamp()) ::
          {:ok, [{commit_tuple(), commit_timestamp()}]} | {:error, Error.t()}
  def get_unverified_commits(consumer_id, older_than_timestamp) do
    key = commit_key(consumer_id)

    with {:ok, results} <- Redis.command(["ZRANGEBYSCORE", key, "-inf", older_than_timestamp, "WITHSCORES"]) do
      commits =
        results
        |> Enum.chunk_every(2)
        |> Enum.map(fn [member, score] ->
          [xid, lsn] = String.split(member, ":")
          {{String.to_integer(xid), String.to_integer(lsn)}, String.to_integer(score)}
        end)

      {:ok, commits}
    end
  end

  @spec trim_commits(consumer_id(), commit_timestamp()) :: :ok | {:error, Error.t()}
  def trim_commits(consumer_id, older_than_timestamp) do
    key = commit_key(consumer_id)

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

  @spec all_commits(consumer_id()) :: {:ok, [{commit_tuple(), commit_timestamp()}]} | {:error, Error.t()}
  def all_commits(consumer_id) do
    key = commit_key(consumer_id)

    with {:ok, results} <- Redis.command(["ZRANGE", key, 0, -1, "WITHSCORES"]) do
      commits =
        results
        |> Enum.chunk_every(2)
        |> Enum.map(fn [member, score] ->
          [xid, lsn] = String.split(member, ":")
          {{String.to_integer(xid), String.to_integer(lsn)}, String.to_integer(score)}
        end)

      {:ok, commits}
    end
  end

  @spec count_commits(consumer_id()) :: {:ok, non_neg_integer()} | {:error, Error.t()}
  def count_commits(consumer_id) do
    key = commit_key(consumer_id)
    Redis.command(["ZCARD", key])
  end

  defp commit_key(consumer_id), do: "consumer:#{consumer_id}:commit_verification"
end

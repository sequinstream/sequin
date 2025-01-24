defmodule Sequin.ConsumersRuntime.ConsumerIdempotency do
  @moduledoc """
  Handles idempotency for consumers at "the leaf", i.e. at delivery time.

  After successfully delivering messages to a sink destination, the first thing we do
  is mark the messages as delivered via this module.

  The last thing we do *before* delivering a message is to check if we have previously marked
  the message as delivered. If so, we skip deliverying it and consider it delivered.

  We trim the set of acknowledged messages below some threshold to reclaim memory.
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
end

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
  @type commit_tuple :: {integer(), integer()}

  @spec mark_messages_delivered(consumer_id(), [commit_tuple()]) :: :ok | {:error, Error.t()}
  def mark_messages_delivered(_, []), do: :ok

  def mark_messages_delivered(consumer_id, commit_tuples) do
    commit_tuples
    |> Enum.map(fn commit_tuple ->
      ["ZADD", delivered_key(consumer_id), score_from_tuple(commit_tuple), member_from_tuple(commit_tuple)]
    end)
    |> Redis.pipeline()
    |> case do
      {:ok, _} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  @spec delivered_messages(consumer_id(), [commit_tuple()]) :: {:ok, [commit_tuple()]} | {:error, Error.t()}
  def delivered_messages(_, []), do: {:ok, []}

  def delivered_messages(consumer_id, commit_tuples) do
    encoded_commit_tuples = Enum.map(commit_tuples, &member_from_tuple/1)

    with {:ok, results} <- Redis.command(["ZMSCORE", delivered_key(consumer_id) | encoded_commit_tuples]) do
      # Convert results to integers, filtering out nils (non-delivered messages)
      delivered_commit_tuples =
        results
        |> Enum.zip(commit_tuples)
        |> Stream.reject(fn {result, _} -> is_nil(result) end)
        |> Enum.map(fn {_result, {lsn, idx}} ->
          {lsn, idx}
        end)

      {:ok, delivered_commit_tuples}
    end
  end

  @spec trim(consumer_id(), commit_tuple()) :: :ok | {:error, Error.t()}
  def trim(consumer_id, commit_tuple) do
    key = delivered_key(consumer_id)

    with {:ok, initial_size} <- Redis.command(["ZCARD", key]),
         {:ok, trimmed} <- Redis.command(["ZREMRANGEBYSCORE", key, "-inf", "(#{score_from_tuple(commit_tuple)}"]),
         {:ok, final_size} <- Redis.command(["ZCARD", key]) do
      Logger.info("[ConsumerIdempotency] Trimmed set to: #{inspect(commit_tuple)}",
        commit_tuple: inspect(commit_tuple),
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
  defp member_from_tuple({lsn, idx}), do: "#{lsn}:#{idx}"
  defp score_from_tuple({lsn, idx}), do: lsn + idx
end

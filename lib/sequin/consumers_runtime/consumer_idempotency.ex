defmodule Sequin.ConsumersRuntime.ConsumerIdempotency do
  @moduledoc """
  Handles idempotency for consumers at "the leaf", i.e. at the delivery time.

  After successfully delivering messages to a sink destination, the first thing we do
  is mark the messages as delivered via this module.

  The last thing we do before deliverying a message is to check if we have previously marked
  the message as delivered. If so, we skip deliverying it and consider it delivered.

  We rely on the ConsumerMessageStore to trim the set of ackknowledted messages
  when it flushes to disk.
  """

  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Error
  alias Sequin.Redis

  @type consumer_id :: String.t()
  @type seq :: integer()
  @type message :: ConsumerRecord.t() | ConsumerEvent.t()

  @spec mark_messages_delivered(consumer_id(), [message()]) :: :ok | {:error, Error.t()}
  def mark_messages_delivered(_, []), do: :ok

  def mark_messages_delivered(consumer_id, messages) do
    messages
    |> Enum.map(fn message -> ["ZADD", delivered_key(consumer_id), message.seq, message.seq] end)
    |> Redis.pipeline()
    |> case do
      {:ok, _} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  @spec delivered_messages(consumer_id(), [message()]) :: {:ok, [seq()]} | {:error, Error.t()}
  def delivered_messages(_, []), do: {:ok, []}

  def delivered_messages(consumer_id, messages) do
    seqs = Enum.map(messages, & &1.seq)

    with {:ok, results} <- Redis.command(["ZMSCORE", delivered_key(consumer_id) | seqs]) do
      # Convert results to integers, filtering out nils (non-delivered messages)
      delivered_seqs =
        results
        |> Stream.reject(&is_nil/1)
        |> Enum.map(&String.to_integer/1)

      {:ok, delivered_seqs}
    end
  end

  @spec trim(consumer_id(), seq()) :: :ok | {:error, Error.t()}
  def trim(consumer_id, seq) do
    with {:ok, _} <- Redis.command(["ZREMRANGEBYSCORE", delivered_key(consumer_id), "-inf", seq]) do
      :ok
    end
  end

  defp delivered_key(consumer_id), do: "consumer:#{consumer_id}:consumer_idempotency"
end

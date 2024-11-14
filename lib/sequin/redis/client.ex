defmodule Sequin.Redis.Client do
  @moduledoc false
  @behaviour Sequin.Redis

  alias Sequin.Consumers.ConsumerEventData
  alias Sequin.Consumers.ConsumerRecordData
  alias Sequin.Consumers.RedisDestination
  alias Sequin.Error
  alias Sequin.Redis.ConnectionCache

  @impl Sequin.Redis
  def send_messages(%RedisDestination{} = destination, messages) do
    with {:ok, connection} <- ConnectionCache.connection(destination) do
      commands =
        Enum.map(messages, fn
          %ConsumerRecordData{} = message ->
            [
              "XADD",
              destination.stream_key,
              "*",
              "record",
              Jason.encode!(message.record),
              "metadata",
              Jason.encode!(message.metadata)
            ]

          %ConsumerEventData{} = message ->
            [
              "XADD",
              destination.stream_key,
              "*",
              "record",
              Jason.encode!(message.record),
              "changes",
              Jason.encode!(message.changes),
              "action",
              message.action,
              "metadata",
              Jason.encode!(message.metadata)
            ]
        end)

      case Redix.transaction_pipeline(connection, commands) do
        {:ok, _} -> :ok
        {:error, error} -> {:error, to_sequin_error(error)}
      end
    end
  end

  @impl Sequin.Redis
  def message_count(%RedisDestination{} = destination) do
    with {:ok, connection} <- ConnectionCache.connection(destination) do
      case Redix.command(connection, ["XLEN", destination.stream_key]) do
        {:ok, count} -> {:ok, count}
        {:error, error} -> {:error, to_sequin_error(error)}
      end
    end
  end

  defp to_sequin_error(error) do
    case error do
      %Redix.Error{} = error ->
        Error.service(service: :redis, message: "Redis error: #{Exception.message(error)}")

      %Redix.ConnectionError{} = error ->
        Error.service(service: :redis, message: "Redis connection error: #{Exception.message(error)}")
    end
  end
end

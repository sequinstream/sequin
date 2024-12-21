defmodule Sequin.Sinks.Redis.Client do
  @moduledoc false
  @behaviour Sequin.Sinks.Redis

  alias Sequin.Consumers.ConsumerEventData
  alias Sequin.Consumers.ConsumerRecordData
  alias Sequin.Consumers.RedisSink
  alias Sequin.Error
  alias Sequin.NetworkUtils
  alias Sequin.Sinks.Redis
  alias Sequin.Sinks.Redis.ConnectionCache

  @impl Redis
  def send_messages(%RedisSink{} = sink, messages) do
    with {:ok, connection} <- ConnectionCache.connection(sink) do
      commands =
        Enum.map(messages, fn
          %ConsumerRecordData{} = message ->
            [
              "XADD",
              sink.stream_key,
              "*",
              "record",
              Jason.encode!(message.record),
              "metadata",
              Jason.encode!(message.metadata)
            ]

          %ConsumerEventData{} = message ->
            [
              "XADD",
              sink.stream_key,
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

      case :eredis.qp(connection, commands) do
        {:error, error} -> {:error, to_sequin_error(error)}
        _res -> :ok
      end
    end
  end

  @impl Redis
  def message_count(%RedisSink{} = sink) do
    with {:ok, connection} <- ConnectionCache.connection(sink) do
      case :eredis.q(connection, ["XLEN", sink.stream_key]) do
        {:ok, count} -> {:ok, String.to_integer(count)}
        {:error, error} -> {:error, to_sequin_error(error)}
      end
    end
  end

  @impl Redis
  def client_info(%RedisSink{} = sink) do
    with {:ok, connection} <- ConnectionCache.connection(sink) do
      case :eredis.q(connection, ["INFO"]) do
        {:ok, info} -> {:ok, info}
        {:error, error} -> {:error, to_sequin_error(error)}
      end
    end
  end

  @impl Redis
  def test_connection(%RedisSink{} = sink) do
    with {:ok, ipv6} <- NetworkUtils.check_ipv6(sink.host),
         :ok <-
           NetworkUtils.test_tcp_reachability(sink.host, sink.port, ipv6, :timer.seconds(10)),
         {:ok, connection} <- ConnectionCache.connection(sink) do
      case :eredis.q(connection, ["PING"]) do
        {:ok, "PONG"} ->
          :ok

        {:error, error} ->
          # Clear the cache
          ConnectionCache.invalidate_connection(sink)
          {:error, to_sequin_error(error)}
      end
    end
  end

  defp to_sequin_error(:no_connection) do
    Error.service(service: :redis, code: :no_connection, message: "No connection to Redis")
  end

  defp to_sequin_error(error) when is_binary(error) do
    Error.service(service: :redis, message: error, code: :command_failed)
  end
end

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

  require Logger

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

      {time, res} = :timer.tc(fn -> qp(connection, commands) end)
      time = round(time / 1000)

      if time > 200 do
        Logger.warning("[Sequin.Sinks.Redis] Slow command execution", duration_ms: time)
      end

      res
    end
  end

  @impl Redis
  def message_count(%RedisSink{} = sink) do
    with {:ok, connection} <- ConnectionCache.connection(sink) do
      case q(connection, ["XLEN", sink.stream_key]) do
        {:ok, count} -> {:ok, String.to_integer(count)}
        {:error, error} -> {:error, error}
      end
    end
  end

  @impl Redis
  def client_info(%RedisSink{} = sink) do
    with {:ok, connection} <- ConnectionCache.connection(sink) do
      q(connection, ["INFO"])
    end
  end

  @impl Redis
  def test_connection(%RedisSink{} = sink) do
    with {:ok, ipv6} <- NetworkUtils.check_ipv6(sink.host),
         :ok <-
           NetworkUtils.test_tcp_reachability(sink.host, sink.port, ipv6, :timer.seconds(10)),
         {:ok, connection} <- ConnectionCache.connection(sink) do
      case q(connection, ["PING"]) do
        {:ok, "PONG"} ->
          :ok

        {:error, error} ->
          # Clear the cache
          ConnectionCache.invalidate_connection(sink)
          {:error, error}
      end
    end
  end

  defp qp(connection, commands) do
    case :eredis.qp(connection, commands, :timer.seconds(15)) do
      {:error, error} -> {:error, handle_error(error)}
      _res -> :ok
    end
  catch
    :exit, {error, _} ->
      {:error, handle_error(error)}
  end

  defp q(connection, command) do
    case :eredis.q(connection, command, :timer.seconds(15)) do
      {:ok, res} -> {:ok, res}
      {:error, error} -> {:error, handle_error(error)}
    end
  catch
    :exit, {error, _} ->
      {:error, handle_error(error)}
  end

  defp handle_error(:no_connection) do
    Logger.error("[Sequin.Sinks.Redis] No connection to Redis")
    Error.service(service: :redis_sink, code: :no_connection, message: "No connection to Redis")
  end

  defp handle_error(:timeout) do
    Logger.error("[Sequin.Sinks.Redis] Timeout sending messages to Redis")
    Error.timeout(source: :redis_sink, timeout_ms: :timer.seconds(5))
  end

  # Not sure if we hit this clause
  defp handle_error(error) when is_exception(error) do
    Logger.error("[Sequin.Sinks.Redis] Error sending messages to Redis", error: error)
    Error.service(service: :redis_sink, code: :command_failed, message: Exception.message(error))
  end

  defp handle_error(error) do
    Logger.error("[Sequin.Sinks.Redis] Unknown error", error: error)
    Error.service(service: :redis_sink, code: :command_failed, message: inspect(error))
  end
end

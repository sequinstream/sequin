defmodule Sequin.Sinks.Redis.Client do
  @moduledoc false
  @behaviour Sequin.Sinks.Redis

  import Sequin.Consumers.Guards, only: [is_redis_sink: 1]

  alias Sequin.Consumers.RedisStreamSink
  alias Sequin.Consumers.RedisStringSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Error
  alias Sequin.NetworkUtils
  alias Sequin.Runtime.Routing.RoutedMessage
  alias Sequin.Sinks.Redis
  alias Sequin.Sinks.Redis.ConnectionCache

  require Logger

  @impl Redis
  def send_messages(%SinkConsumer{sink: %RedisStreamSink{} = sink} = consumer, messages) do
    with {:ok, connection} <- ConnectionCache.connection(sink) do
      commands = xadd_commands(consumer, messages)

      {time, res} = :timer.tc(fn -> qp(connection, commands) end, :millisecond)

      if time > 200 do
        Logger.warning("[Sequin.Sinks.Redis] Slow command execution", duration_ms: time)
      end

      res
    end
  end

  @impl Redis
  def set_messages(%RedisStringSink{} = sink, messages) do
    with {:ok, connection} <- ConnectionCache.connection(sink) do
      commands =
        Enum.map(messages, fn
          %RoutedMessage{routing_info: %{action: "del", key: key}} ->
            ["DEL", key]

          %RoutedMessage{
            routing_info: %{action: "set", key: key, expire_ms: expire_ms},
            transformed_message: transformed_message
          } ->
            payload =
              case transformed_message do
                message when is_binary(message) or is_number(message) -> message
                message -> Jason.encode!(message)
              end

            case expire_ms do
              nil ->
                ["SET", key, payload]

              expire_ms ->
                ["SET", key, payload, "PX", expire_ms]
            end
        end)

      qp(connection, commands)
    end
  end

  @impl Redis
  def message_count(%RedisStreamSink{} = sink) do
    with {:ok, connection} <- ConnectionCache.connection(sink) do
      case q(connection, ["XLEN", sink.stream_key]) do
        {:ok, count} -> {:ok, String.to_integer(count)}
        {:error, error} -> {:error, error}
      end
    end
  end

  @impl Redis
  def client_info(redis_sink) when is_redis_sink(redis_sink) do
    with {:ok, connection} <- ConnectionCache.connection(redis_sink) do
      q(connection, ["INFO"])
    end
  end

  @impl Redis
  def test_connection(redis_sink) when is_redis_sink(redis_sink) do
    with {:ok, ipv6} <- NetworkUtils.check_ipv6(redis_sink.host),
         :ok <-
           NetworkUtils.test_tcp_reachability(redis_sink.host, redis_sink.port, ipv6, to_timeout(second: 10)),
         {:ok, connection} <- ConnectionCache.connection(redis_sink) do
      case q(connection, ["PING"]) do
        {:ok, "PONG"} ->
          :ok

        {:error, error} ->
          # Clear the cache
          ConnectionCache.invalidate_connection(redis_sink)
          {:error, error}
      end
    end
  end

  defp qp(connection, commands) do
    case :eredis.qp(connection, commands, to_timeout(second: 15)) do
      {:error, error} -> {:error, handle_error(error)}
      _res -> :ok
    end
  catch
    :exit, {error, _} ->
      {:error, handle_error(error)}
  end

  defp q(connection, command) do
    case :eredis.q(connection, command, to_timeout(second: 15)) do
      {:ok, res} -> {:ok, res}
      {:error, error} -> {:error, handle_error(error)}
    end
  catch
    :exit, {error, _} ->
      {:error, handle_error(error)}
  end

  defp xadd_commands(%SinkConsumer{}, messages) do
    Enum.map(messages, fn %RoutedMessage{routing_info: %{stream_key: stream_key}, transformed_message: data} ->
      if !is_map(data) do
        raise Error.validation(
                summary: "Message data must be a map for Redis Stream sinks",
                details: "Got #{inspect(data)}"
              )
      end

      fields =
        data
        |> Map.to_list()
        |> Enum.flat_map(fn {key, value} ->
          value = if is_binary(value), do: value, else: Jason.encode!(value)
          [key, value]
        end)

      ["XADD", stream_key, "*" | fields]
    end)
  end

  defp handle_error(:no_connection) do
    Logger.error("[Sequin.Sinks.Redis] No connection to Redis")
    Error.service(service: :redis_stream_sink, code: :no_connection, message: "No connection to Redis")
  end

  defp handle_error(:timeout) do
    Logger.error("[Sequin.Sinks.Redis] Timeout sending messages to Redis")
    Error.timeout(source: :redis_stream_sink, timeout_ms: to_timeout(second: 5))
  end

  # Not sure if we hit this clause
  defp handle_error(error) when is_exception(error) do
    Logger.error("[Sequin.Sinks.Redis] Error sending messages to Redis", error: error)
    Error.service(service: :redis_stream_sink, code: :command_failed, message: Exception.message(error))
  end

  defp handle_error(error) do
    Logger.error("[Sequin.Sinks.Redis] Unknown error", error: error)
    Error.service(service: :redis_stream_sink, code: :command_failed, message: inspect(error))
  end
end

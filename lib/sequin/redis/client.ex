defmodule Sequin.Redis.Client do
  @moduledoc false
  @behaviour Sequin.Redis

  alias Sequin.Consumers.ConsumerEventData
  alias Sequin.Consumers.ConsumerRecordData
  alias Sequin.Consumers.RedisSink
  alias Sequin.Error
  alias Sequin.NetworkUtils
  alias Sequin.Redis.ConnectionCache

  @impl Sequin.Redis
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

      case Redix.transaction_pipeline(connection, commands) do
        {:ok, _} -> :ok
        {:error, error} -> {:error, to_sequin_error(error)}
      end
    end
  end

  @impl Sequin.Redis
  def message_count(%RedisSink{} = sink) do
    with {:ok, connection} <- ConnectionCache.connection(sink) do
      case Redix.command(connection, ["XLEN", sink.stream_key]) do
        {:ok, count} -> {:ok, count}
        {:error, error} -> {:error, to_sequin_error(error)}
      end
    end
  end

  @impl Sequin.Redis
  def client_info(%RedisSink{} = sink) do
    with {:ok, connection} <- ConnectionCache.connection(sink) do
      case Redix.command(connection, ["INFO"]) do
        {:ok, info} -> {:ok, info}
        {:error, error} -> {:error, to_sequin_error(error)}
      end
    end
  end

  @impl Sequin.Redis
  def test_connection(%RedisSink{} = sink) do
    with :ok <-
           NetworkUtils.test_tcp_reachability(sink.host, sink.port, sink.tls, :timer.seconds(10)),
         {:ok, connection} <- ConnectionCache.connection(sink) do
      case Redix.command(connection, ["PING"]) do
        {:ok, "PONG"} -> :ok
        {:error, error} -> {:error, to_sequin_error(error)}
      end
    end
  catch
    :exit, {:redix_exited_during_call, error} ->
      {:error, to_sequin_error(error)}
  end

  defp to_sequin_error(error) do
    case error do
      %Redix.Error{} = error ->
        Error.service(service: :redis, message: "Redis error: #{Exception.message(error)}")

      %Redix.ConnectionError{} = error ->
        Error.service(service: :redis, message: "Redis connection error: #{Exception.message(error)}")

      {%Redix.Protocol.ParseError{}, _} ->
        Error.service(
          service: :redis,
          message: "Redis protocol error. Are you sure this is a Redis instance?"
        )
    end
  end
end

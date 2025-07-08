defmodule Sequin.Sinks.RabbitMq.Client do
  @moduledoc false
  @behaviour Sequin.Sinks.RabbitMq

  alias AMQP.Basic
  alias Sequin.Consumers.RabbitMqSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Error
  alias Sequin.NetworkUtils
  alias Sequin.Runtime.Routing.Consumers.Rabbitmq
  alias Sequin.Runtime.Routing.RoutedMessage
  alias Sequin.Sinks.RabbitMq
  alias Sequin.Sinks.RabbitMq.ConnectionCache

  require Logger

  @impl RabbitMq
  def send_messages(%SinkConsumer{sink: %RabbitMqSink{} = sink}, routed_messages) when is_list(routed_messages) do
    with {:ok, connection} <- ConnectionCache.connection(sink) do
      Enum.reduce_while(routed_messages, :ok, fn routed_message, :ok ->
        case publish_message(routed_message, connection) do
          :ok ->
            {:cont, :ok}

          {:error, error} ->
            {:halt, {:error, error}}
        end
      end)
    end
  end

  @impl RabbitMq
  def test_connection(%RabbitMqSink{} = sink) do
    with :ok <-
           NetworkUtils.test_tcp_reachability(sink.host, sink.port, RabbitMqSink.ipv6?(sink), to_timeout(second: 10)),
         {:ok, _connection} <- ConnectionCache.connection(sink) do
      # TODO: figure out how to test connection
      # Check https://www.rabbitmq.com/docs/troubleshooting-networking
      :ok
    else
      {:error, error} ->
        {:error, to_sequin_error(error)}
    end
  catch
    :exit, error ->
      {:error, to_sequin_error(error)}
  end

  defp publish_message(%RoutedMessage{} = routed_message, %AMQP.Channel{} = channel) do
    # https://hexdocs.pm/amqp/AMQP.Basic.html#publish/5-options
    %RoutedMessage{
      transformed_message: transformed_message,
      routing_info: %Rabbitmq{
        exchange: exchange,
        headers: headers,
        routing_key: routing_key,
        message_id: message_id
      }
    } = routed_message

    opts = [message_id: message_id, content_type: "application/json", headers: Map.to_list(headers)]

    try do
      case Basic.publish(channel, exchange, routing_key, Jason.encode!(transformed_message), opts) do
        :ok -> :ok
        {:error, reason} -> {:error, to_sequin_error(reason)}
      end
    catch
      error ->
        {:error, to_sequin_error(error)}
    end
  end

  defp to_sequin_error(error) do
    case error do
      # Errors returned as char strings
      error when is_binary(error) or is_atom(error) ->
        Error.service(service: :rabbitmq, message: "RabbitMQ error: #{error}")

      {:server_sent_malformed_header, _header} ->
        Error.service(
          service: :rabbitmq,
          message:
            "RabbitMQ server sent malformed header. It's possible your SSL setting is incorrect. Should SSL be enabled?"
        )

      {:auth_failure, _error} ->
        Error.service(
          service: :rabbitmq,
          message: "Authentication failed. Please check your credentials and try again."
        )

      _ ->
        Logger.warning("Unknown RabbitMQ error: #{inspect(error)}", error: error)
        Error.service(service: :rabbitmq, message: "Unknown RabbitMQ error")
    end
  end
end

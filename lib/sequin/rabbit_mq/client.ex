defmodule Sequin.RabbitMq.Client do
  @moduledoc false
  @behaviour Sequin.RabbitMq

  alias AMQP.Basic
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.RabbitMqSink
  alias Sequin.Error
  alias Sequin.NetworkUtils
  alias Sequin.RabbitMq.ConnectionCache

  @impl Sequin.RabbitMq
  def send_messages(%RabbitMqSink{} = sink, messages) when is_list(messages) do
    with {:ok, connection} <- ConnectionCache.connection(sink) do
      Enum.reduce_while(messages, :ok, fn message, :ok ->
        case publish_message(message, connection, sink) do
          :ok ->
            {:cont, :ok}

          {:error, error} ->
            {:halt, {:error, error}}
        end
      end)
    end
  end

  @impl Sequin.RabbitMq
  def test_connection(%RabbitMqSink{} = sink) do
    with :ok <- NetworkUtils.test_tcp_reachability(sink.host, sink.port, RabbitMqSink.ipv6?(sink), :timer.seconds(10)),
         {:ok, _connection} <- ConnectionCache.connection(sink) do
      # TODO: figure out how to test connection
      # Check https://www.rabbitmq.com/docs/troubleshooting-networking
      :ok
    end
  catch
    :exit, error ->
      {:error, to_sequin_error(error)}
  end

  defp publish_message(message, %AMQP.Channel{} = channel, sink) do
    # https://hexdocs.pm/amqp/AMQP.Basic.html#publish/5-options
    opts = [message_id: to_string(message.id), content_type: "application/json"]
    payload = to_payload(message)
    # TODO: figure out how to get the routing key
    routing_key = "sequin.#{sink.connection_id}"

    try do
      with {:error, reason} <- Basic.publish(channel, sink.exchange, routing_key, Jason.encode!(payload), opts) do
        {:error, to_sequin_error(reason)}
      end
    catch
      error ->
        {:error, to_sequin_error(error)}
    end
  end

  defp to_sequin_error(error) do
    case error do
      error when is_binary(error) or is_atom(error) ->
        Error.service(service: :rabbitmq, message: "RabbitMQ error: #{error}")

      _ ->
        Error.service(service: :rabbitmq, message: "Unknown RabbitMQ error")
    end
  end

  defp to_payload(%ConsumerEvent{} = message) do
    %{
      record: message.data.record,
      metadata: message.data.metadata
    }
  end

  defp to_payload(%ConsumerRecord{} = message) do
    %{
      record: message.data.record,
      changes: message.data.changes,
      action: message.data.action,
      metadata: message.data.metadata
    }
  end
end

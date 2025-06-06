defmodule Sequin.Sinks.RabbitMq.Client do
  @moduledoc false
  @behaviour Sequin.Sinks.RabbitMq

  alias AMQP.Basic
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerEventData
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.ConsumerRecordData
  alias Sequin.Consumers.RabbitMqSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Error
  alias Sequin.NetworkUtils
  alias Sequin.Sinks.RabbitMq
  alias Sequin.Sinks.RabbitMq.ConnectionCache

  require Logger

  @impl RabbitMq
  def send_messages(%SinkConsumer{sink: %RabbitMqSink{} = sink} = consumer, messages) when is_list(messages) do
    with {:ok, connection} <- ConnectionCache.connection(sink) do
      Enum.reduce_while(messages, :ok, fn message, :ok ->
        case publish_message(consumer, message, connection) do
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
    with :ok <- NetworkUtils.test_tcp_reachability(sink.host, sink.port, RabbitMqSink.ipv6?(sink), :timer.seconds(10)),
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

  defp publish_message(%SinkConsumer{sink: %RabbitMqSink{} = sink} = consumer, message, %AMQP.Channel{} = channel) do
    # https://hexdocs.pm/amqp/AMQP.Basic.html#publish/5-options
    # FIXME: message.id should not be relied on?
    opts = [message_id: to_string(message.id), content_type: "application/json"]
    payload = to_payload(consumer, message)
    routing_key = routing_key(message)

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

  defp to_payload(%SinkConsumer{} = consumer, message) do
    Sequin.Transforms.Message.to_external(consumer, message)
  end

  defp routing_key(%ConsumerEvent{data: %ConsumerEventData{} = data}) do
    %{metadata: %{database_name: database_name, table_schema: table_schema, table_name: table_name}} = data
    "sequin.changes.#{database_name}.#{table_schema}.#{table_name}.#{data.action}"
  end

  defp routing_key(%ConsumerRecord{data: %ConsumerRecordData{} = data}) do
    %{metadata: %{database_name: database_name, table_schema: table_schema, table_name: table_name}} = data
    "sequin.rows.#{database_name}.#{table_schema}.#{table_name}"
  end
end

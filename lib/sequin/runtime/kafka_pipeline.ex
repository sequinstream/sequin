defmodule Sequin.Runtime.KafkaPipeline do
  @moduledoc false
  @behaviour Sequin.Runtime.SinkPipeline

  alias Sequin.Consumers.KafkaSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Error
  alias Sequin.Runtime.Routing
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Sinks.Kafka

  require Logger

  @impl SinkPipeline
  def init(context, _opts) do
    context
  end

  @impl SinkPipeline
  def batchers_config(%SinkConsumer{batch_size: batch_size}) do
    [
      default: [
        concurrency: 160,
        batch_size: batch_size,
        batch_timeout: 5
      ]
    ]
  end

  @impl SinkPipeline
  def handle_message(message, context) do
    %{consumer: consumer, test_pid: test_pid} = context
    setup_allowances(test_pid)
    %Routing.Consumers.Kafka{topic: topic} = Routing.route_message(consumer, message.data)
    context = maybe_put_partition_count(topic, context)

    # Only prepare the message with its partition information
    msg = message.data
    data = Sequin.Transforms.Message.to_external(consumer, msg)
    encoded_data = Jason.encode!(data)
    msg = %{msg | encoded_data: encoded_data, encoded_data_size_bytes: byte_size(encoded_data)}

    partition = partition_from_message(consumer, msg, context.partition_count[topic])

    message =
      message
      |> Broadway.Message.put_data(msg)
      |> Broadway.Message.put_batch_key({topic, partition})

    {:ok, message, context}
  catch
    {:failed_to_connect, error} ->
      Logger.error("[KafkaPipeline] Failed to connect to Kafka: #{inspect(error)}", error: error)
      {:error, Error.service(service: :kafka, message: "failed to connect")}
  end

  @impl SinkPipeline
  def handle_batch(:default, messages, %{batch_key: {topic, partition}}, context) do
    %{consumer: %SinkConsumer{sink: %KafkaSink{}} = consumer, test_pid: test_pid} = context
    setup_allowances(test_pid)

    msgs = Enum.map(messages, & &1.data)

    case Kafka.publish(consumer, topic, partition, msgs) do
      :ok ->
        {:ok, messages, context}

      {:error, error} when is_exception(error) ->
        {:error, error}
    end
  catch
    {:failed_to_connect, error} ->
      Logger.error("[KafkaPipeline] Failed to connect to Kafka: #{inspect(error)}", error: error)
      {:error, Error.service(service: :kafka, message: "failed to connect")}
  end

  def maybe_put_partition_count(topic, %{consumer: consumer, test_pid: test_pid} = context) do
    if get_in(context, [:partition_count, topic]) do
      context
    else
      setup_allowances(test_pid)

      partition_count = get_partition_count!(consumer, topic)

      context = Map.put_new(context, :partition_count, %{})
      put_in(context, [:partition_count, topic], partition_count)
    end
  end

  defp partition_from_message(%SinkConsumer{sink: %KafkaSink{}} = consumer, message, partition_count)
       when is_integer(partition_count) do
    case Kafka.message_key(consumer, message) do
      "" -> Enum.random(1..partition_count)
      group_id when is_binary(group_id) -> :erlang.phash2(group_id, partition_count)
    end
  end

  defp get_partition_count!(%SinkConsumer{sink: %KafkaSink{} = sink}, topic) do
    case Kafka.get_partition_count(sink, topic) do
      {:ok, partition_count} ->
        partition_count

      {:error, error} when is_exception(error) ->
        error = Error.invariant(message: "Failed to get partition count for Kafka sink: #{Exception.message(error)}")
        Logger.warning("[KafkaPipeline] #{Exception.message(error)}")
        raise error

      {:error, error} ->
        error = Error.invariant(message: "Failed to get partition count for Kafka sink: #{inspect(error)}")
        Logger.warning("[KafkaPipeline] #{Exception.message(error)}")
        raise error
    end
  end

  defp setup_allowances(nil), do: :ok

  defp setup_allowances(test_pid) do
    Mox.allow(Sequin.Sinks.KafkaMock, test_pid, self())
    Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
  end
end

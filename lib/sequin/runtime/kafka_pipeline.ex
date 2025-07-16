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
  def handle_message(broadway_message, context) do
    %{consumer: consumer, test_pid: test_pid} = context
    setup_allowances(test_pid)

    %Routing.Consumers.Kafka{topic: topic, message_key: message_key} =
      Routing.route_message(consumer, broadway_message.data)

    message_key = message_key || Kafka.message_key(consumer, broadway_message.data)
    context = maybe_put_partition_count(topic, context)

    consumer_message = broadway_message.data
    data = Sequin.Transforms.Message.to_external(consumer, consumer_message)
    encoded_data = Jason.encode!(data)
    consumer_message = %{consumer_message | encoded_data: encoded_data, encoded_data_size_bytes: byte_size(encoded_data)}

    partition = partition_from_message(consumer, message_key, context.partition_count[topic])

    broadway_message =
      broadway_message
      |> Broadway.Message.put_data(consumer_message)
      |> Broadway.Message.put_batch_key({topic, partition})

    {:ok, broadway_message, context}
  catch
    {:failed_to_connect, error} ->
      Logger.error("[KafkaPipeline] Failed to connect to Kafka: #{inspect(error)}", error: error)
      {:error, Error.service(service: :kafka, message: "failed to connect")}
  end

  @impl SinkPipeline
  def handle_batch(:default, messages, %{batch_key: {topic, partition}}, context) do
    %{consumer: %SinkConsumer{sink: %KafkaSink{}} = consumer, test_pid: test_pid} = context
    setup_allowances(test_pid)

    kafka_messages =
      Enum.map(messages, fn %Broadway.Message{data: consumer_message} ->
        # TODO: Ideally we would not need to re-execute message_key logic here. It is difficult, however, to pass the message_key
        # from handle_message to handle_batch because:
        # 1. The message_key should not impact batching, so we can't put it in batch_info
        # 2. The broadway_message.data must be a consumer_message for SinkPipeline compatibility, and so we can't put Kafka specific data there
        %Routing.Consumers.Kafka{message_key: message_key} = Routing.route_message(consumer, consumer_message)
        message_key = message_key || Kafka.message_key(consumer, consumer_message)

        %Kafka.Message{key: message_key, value: consumer_message.encoded_data}
      end)

    # case Kafka.publish(consumer, topic, partition, kafka_messages) do
    #   :ok ->
    {:ok, messages, context}

    #   {:error, error} when is_exception(error) ->
    #     {:error, error}
    # end
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

  defp partition_from_message(%SinkConsumer{sink: %KafkaSink{}}, message_key, partition_count)
       when is_integer(partition_count) do
    case message_key do
      "" -> Enum.random(0..(partition_count - 1))
      message_key when is_binary(message_key) -> :erlang.phash2(message_key, partition_count)
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

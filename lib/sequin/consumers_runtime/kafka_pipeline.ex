defmodule Sequin.ConsumersRuntime.KafkaPipeline do
  @moduledoc false
  @behaviour Sequin.ConsumersRuntime.SinkPipeline

  alias Sequin.Consumers.KafkaSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.ConsumersRuntime.SinkPipeline
  alias Sequin.Error
  alias Sequin.Sinks.Kafka

  require Logger

  @impl SinkPipeline
  def init(context, _opts) do
    context
  end

  @impl SinkPipeline
  def processors_config(%SinkConsumer{}) do
    [
      default: [
        concurrency: 100,
        max_demand: 10,
        min_demand: 5
      ]
    ]
  end

  @impl SinkPipeline
  def batchers_config(%SinkConsumer{batch_size: batch_size}) do
    [
      default: [
        concurrency: 20,
        batch_size: batch_size,
        batch_timeout: 50
      ]
    ]
  end

  @impl SinkPipeline
  def handle_message(message, context) do
    %{consumer: consumer, test_pid: test_pid, partition_count: partition_count} = context
    setup_allowances(test_pid)

    # Only prepare the message with its partition information
    [msg] = message.data
    encoded_data = Jason.encode!(msg.data)
    msg = %{msg | encoded_data: encoded_data, encoded_data_size_bytes: byte_size(encoded_data)}

    partition = partition_from_message(consumer, msg, partition_count)

    message
    |> Broadway.Message.put_data([msg])
    |> Broadway.Message.put_batch_key(partition)
  end

  @impl SinkPipeline
  def handle_batch(:default, messages, %{batch_key: partition}, context) do
    %{consumer: %SinkConsumer{sink: %KafkaSink{}} = consumer, test_pid: test_pid} = context
    setup_allowances(test_pid)

    msgs = Enum.flat_map(messages, fn %Broadway.Message{data: [message]} -> [message] end)

    case Kafka.publish(consumer, partition, msgs) do
      :ok -> SinkPipeline.on_success(context, messages)
      {:error, error} when is_exception(error) -> SinkPipeline.on_failure(context, error, messages)
    end
  end

  @impl SinkPipeline
  def runtime_context(%{partition_count: partition_count} = context) when is_integer(partition_count) do
    context
  end

  def runtime_context(%{consumer: consumer, test_pid: test_pid} = context) do
    setup_allowances(test_pid)

    partition_count = get_partition_count!(consumer)

    Map.put(context, :partition_count, partition_count)
  end

  defp partition_from_message(%SinkConsumer{sink: %KafkaSink{}} = consumer, message, partition_count) do
    case Kafka.message_key(consumer, message) do
      "" -> Enum.random(1..partition_count)
      group_id when is_binary(group_id) -> :erlang.phash2(group_id, partition_count)
    end
  end

  defp get_partition_count!(%SinkConsumer{sink: %KafkaSink{} = sink}) do
    case Kafka.get_partition_count(sink) do
      {:ok, partition_count} ->
        partition_count

      {:error, error} ->
        error = Error.invariant(message: "Failed to get partition count for Kafka sink: #{Exception.message(error)}")
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

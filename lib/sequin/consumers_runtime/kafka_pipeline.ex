defmodule Sequin.ConsumersRuntime.KafkaPipeline do
  @moduledoc false
  use Broadway

  alias Sequin.Consumers.KafkaSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.ConsumersRuntime.ConsumerProducer
  alias Sequin.Error
  alias Sequin.Health
  alias Sequin.Health.Event
  alias Sequin.Repo
  alias Sequin.Sinks.Kafka

  require Logger

  def start_link(opts) do
    %SinkConsumer{} =
      consumer =
      opts
      |> Keyword.fetch!(:consumer)
      |> Repo.lazy_preload([:sequence, :postgres_database])

    producer = Keyword.get(opts, :producer, Sequin.ConsumersRuntime.ConsumerProducer)
    test_pid = Keyword.get(opts, :test_pid)

    Broadway.start_link(__MODULE__,
      name: via_tuple(consumer.id),
      producer: [
        module: {producer, [consumer: consumer, test_pid: test_pid, batch_size: 1]}
      ],
      processors: [
        default: [
          concurrency: System.schedulers_online(),
          max_demand: 400,
          min_demand: 200
        ]
      ],
      context: %{
        consumer: consumer,
        test_pid: test_pid
      },
      batchers: [
        default: [
          concurrency: 80,
          batch_size: consumer.batch_size,
          batch_timeout: 10
        ]
      ]
    )
  end

  def via_tuple(consumer_id) do
    {:via, :syn, {:consumers, {__MODULE__, consumer_id}}}
  end

  # Used by Broadway to name processes in topology according to our registry
  @impl Broadway
  def process_name({:via, :syn, {:consumers, {__MODULE__, id}}}, base_name) do
    {:via, :syn, {:consumers, {__MODULE__, {base_name, id}}}}
  end

  @impl Broadway
  # `data` is either a [ConsumerRecord] or a [ConsumerEvent]
  @spec handle_message(any(), Broadway.Message.t(), map()) :: Broadway.Message.t()
  def handle_message(_, %Broadway.Message{data: [message]} = broadway_message, %{
        consumer: %SinkConsumer{sink: %KafkaSink{}} = consumer,
        test_pid: test_pid
      }) do
    setup_allowances(test_pid)

    Logger.metadata(
      account_id: consumer.account_id,
      consumer_id: consumer.id
    )

    encoded_data = Jason.encode!(message.data)
    message = %{message | encoded_data: encoded_data, encoded_data_size_bytes: byte_size(encoded_data)}

    partition = partition_from_message(consumer, message)

    broadway_message
    |> Broadway.Message.put_data([message])
    |> Broadway.Message.put_batch_key(partition)
  end

  @impl Broadway
  def handle_batch(:default, broadway_messages, %{batch_key: partition}, %{
        consumer: %SinkConsumer{sink: %KafkaSink{}} = consumer,
        test_pid: test_pid
      }) do
    setup_allowances(test_pid)

    Logger.metadata(
      account_id: consumer.account_id,
      consumer_id: consumer.id
    )

    messages = Enum.map(broadway_messages, fn %Broadway.Message{data: [message]} -> message end)

    case Kafka.publish(consumer, partition, messages) do
      :ok ->
        :ok = ConsumerProducer.pre_ack_delivered_messages(consumer, broadway_messages)
        Health.put_event(consumer, %Event{slug: :messages_delivered, status: :success})

        broadway_messages

      {:error, error} when is_exception(error) ->
        Logger.warning("Failed to publish message to Kafka: #{Exception.message(error)}")

        Health.put_event(consumer, %Event{slug: :messages_delivered, status: :fail, error: error})

        Sequin.Logs.log_for_consumer_message(
          :error,
          consumer.account_id,
          consumer.id,
          Enum.map(broadway_messages, fn %Broadway.Message{data: [message]} ->
            message.replication_message_trace_id
          end),
          "Failed to publish message to Kafka: #{Exception.message(error)}"
        )

        Enum.map(broadway_messages, &Broadway.Message.failed(&1, error))
    end
  end

  defp partition_from_message(%SinkConsumer{sink: %KafkaSink{}} = consumer, message) do
    partition_count = get_cached_partition_count!(consumer)

    case Kafka.message_key(consumer, message) do
      "" -> Enum.random(1..partition_count)
      group_id when is_binary(group_id) -> :erlang.phash2(group_id, partition_count)
    end
  end

  defp get_cached_partition_count!(%SinkConsumer{sink: %KafkaSink{}} = consumer) do
    case Process.get(:cached_partition_count) do
      nil ->
        partition_count = get_partition_count!(consumer)
        Process.put(:cached_partition_count, partition_count)
        partition_count

      partition_count when is_integer(partition_count) ->
        partition_count
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

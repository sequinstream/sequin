defmodule Sequin.ConsumersRuntime.KafkaPipeline do
  @moduledoc false
  use Broadway

  alias Sequin.Consumers.KafkaSink
  alias Sequin.Consumers.SinkConsumer
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
        module: {producer, [consumer: consumer, test_pid: test_pid]}
      ],
      processors: [
        default: [
          concurrency: consumer.max_waiting,
          max_demand: 1
        ]
      ],
      context: %{
        consumer: consumer,
        test_pid: test_pid
      }
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
  def handle_message(_, %Broadway.Message{data: [consumer_record_or_event]} = message, %{
        consumer: %SinkConsumer{sink: %KafkaSink{}} = consumer,
        test_pid: test_pid
      }) do
    setup_allowances(test_pid)

    Logger.metadata(
      account_id: consumer.account_id,
      consumer_id: consumer.id
    )

    case Kafka.publish(consumer, consumer_record_or_event) do
      :ok ->
        Health.put_event(consumer, %Event{slug: :messages_delivered, status: :success})

        Sequin.Logs.log_for_consumer_message(
          :info,
          consumer.account_id,
          consumer_record_or_event.replication_message_trace_id,
          "Published message to Kafka successfully"
        )

        message

      {:error, error} when is_exception(error) ->
        Logger.warning("Failed to publish message to Kafka: #{Exception.message(error)}")

        Health.put_event(consumer, %Event{slug: :messages_delivered, status: :fail, error: error})

        Sequin.Logs.log_for_consumer_message(
          :error,
          consumer.account_id,
          consumer_record_or_event.replication_message_trace_id,
          "Failed to publish message to Kafka: #{Exception.message(error)}"
        )

        Broadway.Message.failed(message, error)
    end
  end

  defp setup_allowances(nil), do: :ok

  defp setup_allowances(test_pid) do
    Mox.allow(Sequin.Sinks.KafkaMock, test_pid, self())
    Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
  end
end

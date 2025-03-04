defmodule Sequin.Runtime.RabbitMqPipeline do
  @moduledoc false
  use Broadway

  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Error
  alias Sequin.Health
  alias Sequin.Health.Event
  alias Sequin.Runtime.ConsumerProducer
  alias Sequin.Sinks.RabbitMq

  require Logger

  def start_link(opts) do
    %SinkConsumer{} = consumer = Keyword.fetch!(opts, :consumer)

    producer = Keyword.get(opts, :producer, Sequin.Runtime.ConsumerProducer)

    Broadway.start_link(__MODULE__,
      name: via_tuple(consumer.id),
      producer: [
        module: {producer, [consumer: consumer]}
      ],
      processors: [
        default: [
          concurrency: consumer.max_waiting,
          max_demand: 10,
          min_demand: 5
        ]
      ],
      context: %{
        consumer: consumer,
        test_pid: Keyword.get(opts, :test_pid)
      }
    )
  end

  def via_tuple(consumer_id) do
    {:via, :syn, {:consumers, {__MODULE__, consumer_id}}}
  end

  @impl Broadway
  def process_name({:via, :syn, {:consumers, {__MODULE__, id}}}, base_name) do
    {:via, :syn, {:consumers, {__MODULE__, {base_name, id}}}}
  end

  @impl Broadway
  def handle_message(_, %Broadway.Message{data: messages} = broadway_message, %{consumer: consumer} = ctx) do
    setup_allowances(ctx)

    Logger.metadata(
      account_id: consumer.account_id,
      consumer_id: consumer.id
    )

    case RabbitMq.send_messages(consumer.sink, messages) do
      :ok ->
        :ok = ConsumerProducer.pre_ack_delivered_messages(consumer, [broadway_message])

        Health.put_event(consumer, %Event{slug: :messages_delivered, status: :success})

        broadway_message

      {:error, error} ->
        reason =
          Error.service(
            service: :rabbitmq,
            code: "publish_error",
            message: "RabbitMQ publish failed",
            details: %{error: error}
          )

        Logger.warning("Failed to push message to RabbitMQ: #{inspect(reason)}")

        Health.put_event(consumer, %Event{slug: :messages_delivered, status: :fail, error: reason})

        Sequin.Logs.log_for_consumer_message(
          :error,
          consumer.account_id,
          consumer.id,
          Enum.map(messages, & &1.replication_message_trace_id),
          "Failed to push message to RabbitMQ: #{inspect(reason)}"
        )

        Broadway.Message.failed(broadway_message, reason)
    end
  end

  defp setup_allowances(%{test_pid: nil}), do: :ok

  defp setup_allowances(%{test_pid: test_pid}) do
    Mox.allow(Sequin.Sinks.RabbitMqMock, test_pid, self())
    Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
  end
end

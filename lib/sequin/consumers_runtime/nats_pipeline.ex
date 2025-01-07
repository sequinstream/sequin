defmodule Sequin.ConsumersRuntime.NatsPipeline do
  @moduledoc false
  use Broadway

  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Error
  alias Sequin.Health
  alias Sequin.Health.Event
  alias Sequin.Sinks.Nats

  require Logger

  def start_link(opts) do
    %SinkConsumer{} = consumer = Keyword.fetch!(opts, :consumer)

    producer = Keyword.get(opts, :producer, Sequin.ConsumersRuntime.ConsumerProducer)

    Broadway.start_link(__MODULE__,
      name: via_tuple(consumer.id),
      producer: [
        module: {producer, [consumer: consumer]}
      ],
      processors: [
        default: [
          concurrency: consumer.max_waiting,
          max_demand: 10
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

  # Used by Broadway to name processes in topology according to our registry
  @impl Broadway
  def process_name({:via, :syn, {:consumers, {__MODULE__, id}}}, base_name) do
    {:via, :syn, {:consumers, {__MODULE__, {base_name, id}}}}
  end

  @impl Broadway
  # `data` is either a [ConsumerRecord] or a [ConsumerEvent]
  @spec handle_message(any(), Broadway.Message.t(), map()) :: Broadway.Message.t()
  def handle_message(_, %Broadway.Message{data: messages} = message, %{consumer: consumer} = ctx) do
    setup_allowances(ctx)

    Logger.metadata(
      account_id: consumer.account_id,
      consumer_id: consumer.id
    )

    case Nats.send_messages(consumer.sink, messages) do
      :ok ->
        Health.put_event(consumer, %Event{slug: :messages_delivered, status: :success})

        Enum.each(
          messages,
          &Sequin.Logs.log_for_consumer_message(
            :info,
            consumer.account_id,
            &1.replication_message_trace_id,
            "Pushed message to NATS successfully"
          )
        )

        message

      {:error, error} ->
        reason =
          Error.service(
            service: :nats,
            code: "publish_error",
            message: "NATS publish failed",
            details: %{error: error}
          )

        Logger.warning("Failed to push message to NATS: #{inspect(reason)}")

        Health.put_event(consumer, %Event{slug: :messages_delivered, status: :fail, error: reason})

        Enum.each(
          messages,
          &Sequin.Logs.log_for_consumer_message(
            :error,
            consumer.account_id,
            &1.replication_message_trace_id,
            "Failed to push message to NATS: #{inspect(reason)}"
          )
        )

        Broadway.Message.failed(message, reason)
    end
  end

  defp setup_allowances(%{test_pid: nil}), do: :ok

  defp setup_allowances(%{test_pid: test_pid}) do
    Mox.allow(Sequin.Sinks.NatsMock, test_pid, self())
    Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
  end
end

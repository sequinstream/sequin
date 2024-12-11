defmodule Sequin.ConsumersRuntime.NatsPipeline do
  @moduledoc false
  use Broadway

  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Error
  alias Sequin.Health
  alias Sequin.Nats.Client
  alias Sequin.Repo

  require Logger

  def start_link(opts) do
    %SinkConsumer{} =
      consumer =
      opts
      |> Keyword.fetch!(:consumer)
      |> Repo.lazy_preload([:sequence, :postgres_database])

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
        consumer: consumer
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
  def handle_message(_, %Broadway.Message{data: messages} = message, %{consumer: consumer}) do
    Logger.metadata(
      account_id: consumer.account_id,
      consumer_id: consumer.id
    )

    case Client.send_messages(consumer.sink, messages) do
      :ok ->
        Health.update(consumer, :push, :healthy)

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

        Health.update(consumer, :push, :error, reason)

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
end

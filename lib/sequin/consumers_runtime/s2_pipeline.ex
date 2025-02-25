defmodule Sequin.ConsumersRuntime.S2Pipeline do
  @moduledoc false
  use Broadway

  alias Sequin.Consumers.S2Sink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Health
  alias Sequin.Health.Event
  alias Sequin.Repo
  alias Sequin.Sinks.S2

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
        module: {producer, [consumer: consumer, batch_size: 100]}
      ],
      processors: [
        default: [
          concurrency: consumer.max_waiting,
          max_demand: 1
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

  @impl Broadway
  def process_name({:via, :syn, {:consumers, {__MODULE__, id}}}, base_name) do
    {:via, :syn, {:consumers, {__MODULE__, {base_name, id}}}}
  end

  @impl Broadway
  @spec handle_message(any(), Broadway.Message.t(), map()) :: Broadway.Message.t()
  def handle_message(_, %Broadway.Message{data: messages} = message, %{
        consumer: %SinkConsumer{sink: %S2Sink{} = sink} = consumer
      }) do
    Logger.metadata(
      account_id: consumer.account_id,
      consumer_id: consumer.id
    )

    case S2.send_messages(sink, messages) do
      :ok ->
        Health.put_event(consumer, %Event{slug: :messages_delivered, status: :success})

        Enum.each(messages, fn msg ->
          Sequin.Logs.log_for_consumer_message(
            :info,
            consumer.account_id,
            consumer.id,
            msg.replication_message_trace_id,
            "Pushed message to S2 successfully"
          )
        end)

        message

      {:error, error} when is_exception(error) ->
        Logger.warning("Failed to push message to S2: #{Exception.message(error)}")

        Health.put_event(consumer, %Event{slug: :messages_delivered, status: :fail, error: error})

        Enum.each(messages, fn msg ->
          Sequin.Logs.log_for_consumer_message(
            :error,
            consumer.account_id,
            consumer.id,
            msg.replication_message_trace_id,
            "Failed to push message to S2: #{Exception.message(error)}"
          )
        end)

        Broadway.Message.failed(message, error)
    end
  end
end

defmodule Sequin.ConsumersRuntime.RedisPipeline do
  @moduledoc false
  use Broadway

  alias Sequin.Consumers.RedisSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Health
  alias Sequin.Health.Event
  alias Sequin.Repo
  alias Sequin.Sinks.Redis

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
        module: {producer, [consumer: consumer, test_pid: test_pid, batch_size: 20]}
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
  def handle_message(_, %Broadway.Message{data: messages} = message, %{
        consumer: %SinkConsumer{sink: %RedisSink{} = sink} = consumer,
        test_pid: test_pid
      }) do
    setup_allowances(test_pid)

    Logger.metadata(
      account_id: consumer.account_id,
      consumer_id: consumer.id
    )

    redis_messages = Enum.map(messages, & &1.data)

    case Redis.send_messages(sink, redis_messages) do
      :ok ->
        Health.put_event(consumer, %Event{slug: :messages_delivered, status: :success})
        # Metrics.incr_sqs_throughput(consumer.sink)

        Enum.each(messages, fn msg ->
          Sequin.Logs.log_for_consumer_message(
            :info,
            consumer.account_id,
            msg.replication_message_trace_id,
            "Pushed message to Redis successfully"
          )
        end)

        message

      {:error, error} when is_exception(error) ->
        Logger.warning("Failed to push message to Redis: #{Exception.message(error)}")

        Health.put_event(consumer, %Event{slug: :messages_delivered, status: :fail, error: error})

        Enum.each(messages, fn msg ->
          Sequin.Logs.log_for_consumer_message(
            :error,
            consumer.account_id,
            msg.replication_message_trace_id,
            "Failed to push message to Redis: #{Exception.message(error)}"
          )
        end)

        Broadway.Message.failed(message, error)
    end
  end

  defp setup_allowances(nil), do: :ok

  defp setup_allowances(test_pid) do
    Mox.allow(Sequin.Sinks.RedisMock, test_pid, self())
    Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
  end
end

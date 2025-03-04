defmodule Sequin.Runtime.RedisPipeline do
  @moduledoc false
  use Broadway

  alias Sequin.Consumers.RedisSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Health
  alias Sequin.Health.Event
  alias Sequin.Repo
  alias Sequin.Runtime.MessageLedgers
  alias Sequin.Runtime.SlotMessageProducer
  alias Sequin.Sinks.Redis

  require Logger

  def start_link(opts) do
    %SinkConsumer{} =
      consumer =
      opts
      |> Keyword.fetch!(:consumer)
      |> Repo.lazy_preload([:sequence, :postgres_database])

    producer = Keyword.get(opts, :producer, Sequin.Runtime.SlotMessageProducer)
    test_pid = Keyword.get(opts, :test_pid)

    Broadway.start_link(__MODULE__,
      name: producer.via_tuple(consumer.id),
      producer: [
        module: {producer, [consumer: consumer, test_pid: test_pid, batch_size: consumer.batch_size]}
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
        test_pid: test_pid
      }
    )
  end

  # Used by Broadway to name processes in topology according to our registry
  @impl Broadway
  def process_name({:via, :syn, {:consumers, {__MODULE__, id}}}, base_name) do
    {:via, :syn, {:consumers, {__MODULE__, {base_name, id}}}}
  end

  @impl Broadway
  # `data` is either a [ConsumerRecord] or a [ConsumerEvent]
  @spec handle_message(any(), Broadway.Message.t(), map()) :: Broadway.Message.t()
  def handle_message(_, %Broadway.Message{data: messages} = broadway_message, %{
        consumer: %SinkConsumer{sink: %RedisSink{} = sink} = consumer,
        test_pid: test_pid
      }) do
    setup_allowances(test_pid)

    Logger.metadata(
      account_id: consumer.account_id,
      consumer_id: consumer.id
    )

    messages
    |> Enum.map(&MessageLedgers.wal_cursor_from_message/1)
    |> then(&MessageLedgers.wal_cursors_reached_checkpoint(consumer.id, "redis_pipeline.handle_message", &1))

    redis_messages = Enum.map(messages, & &1.data)

    case Redis.send_messages(sink, redis_messages) do
      :ok ->
        :ok = SlotMessageProducer.pre_ack_delivered_messages(consumer, [broadway_message])

        Health.put_event(consumer, %Event{slug: :messages_delivered, status: :success})

        messages
        |> Enum.map(&MessageLedgers.wal_cursor_from_message/1)
        |> then(&MessageLedgers.wal_cursors_reached_checkpoint(consumer.id, "redis_pipeline.sent", &1))

        broadway_message

      {:error, error} when is_exception(error) ->
        Logger.warning("Failed to push message to Redis: #{Exception.message(error)}")

        Health.put_event(consumer, %Event{slug: :messages_delivered, status: :fail, error: error})

        Sequin.Logs.log_for_consumer_message(
          :error,
          consumer.account_id,
          consumer.id,
          Enum.map(messages, & &1.replication_message_trace_id),
          "Failed to push message to Redis: #{Exception.message(error)}"
        )

        Broadway.Message.failed(broadway_message, error)
    end
  end

  defp setup_allowances(nil), do: :ok

  defp setup_allowances(test_pid) do
    Mox.allow(Sequin.Sinks.RedisMock, test_pid, self())
    Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
  end
end

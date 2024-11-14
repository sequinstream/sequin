defmodule Sequin.ConsumersRuntime.RedisPipeline do
  @moduledoc false
  use Broadway

  alias Sequin.Consumers.DestinationConsumer
  alias Sequin.Consumers.RedisDestination
  alias Sequin.Health
  alias Sequin.Redis
  alias Sequin.Repo

  require Logger

  def start_link(opts) do
    %DestinationConsumer{} =
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
          max_demand: consumer.batch_size
        ]
      ],
      context: %{
        consumer: consumer,
        test_pid: test_pid
      }
    )
  end

  def via_tuple(consumer_id) do
    Sequin.Registry.via_tuple({__MODULE__, consumer_id})
  end

  # Used by Broadway to name processes in topology according to our registry
  @impl Broadway
  def process_name({:via, Registry, {Sequin.Registry, {__MODULE__, id}}}, base_name) do
    Sequin.Registry.via_tuple({__MODULE__, {base_name, id}})
  end

  @impl Broadway
  # `data` is either a [ConsumerRecord] or a [ConsumerEvent]
  @spec handle_message(any(), Broadway.Message.t(), map()) :: Broadway.Message.t()
  def handle_message(_, %Broadway.Message{data: messages} = message, %{
        consumer: %DestinationConsumer{destination: %RedisDestination{} = destination} = consumer,
        test_pid: test_pid
      }) do
    setup_allowances(test_pid)

    Logger.metadata(
      account_id: consumer.account_id,
      consumer_id: consumer.id
    )

    redis_messages = Enum.map(messages, & &1.data)

    case Redis.send_messages(destination, redis_messages) do
      :ok ->
        Health.update(consumer, :push, :healthy)
        # Metrics.incr_sqs_throughput(consumer.destination)

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

        Health.update(consumer, :push, :error, error)

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
    Mox.allow(Sequin.RedisMock, test_pid, self())
  end
end

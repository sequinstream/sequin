defmodule Sequin.ConsumersRuntime.SqsPipeline do
  @moduledoc false
  use Broadway

  alias Sequin.Aws.SQS
  alias Sequin.Consumers
  alias Sequin.Consumers.ConsumerEventData
  alias Sequin.Consumers.ConsumerRecordData
  alias Sequin.Consumers.DestinationConsumer
  alias Sequin.Consumers.SqsDestination
  alias Sequin.Error
  alias Sequin.Health
  alias Sequin.Repo

  require Logger

  def start_link(opts) do
    %DestinationConsumer{} =
      consumer =
      opts
      |> Keyword.fetch!(:consumer)
      |> Repo.lazy_preload(sequence: [:postgres_database])

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
        sqs_client: SqsDestination.aws_client(consumer.destination)
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
  def handle_message(_, %Broadway.Message{data: messages} = message, %{consumer: consumer, sqs_client: sqs_client}) do
    Logger.metadata(
      account_id: consumer.account_id,
      consumer_id: consumer.id
    )

    sqs_messages = Enum.map(messages, &build_sqs_message(consumer, &1.data))

    case SQS.send_messages(sqs_client, consumer.destination.queue_url, sqs_messages) do
      :ok ->
        Health.update(consumer, :push, :healthy)
        # Metrics.incr_sqs_throughput(consumer.destination)

        Enum.each(messages, fn msg ->
          Sequin.Logs.log_for_consumer_message(
            :info,
            consumer.account_id,
            msg.replication_message_trace_id,
            "Pushed message to SQS successfully"
          )
        end)

        message

      {:error, error} ->
        reason = format_error(error)
        Logger.warning("Failed to push message to SQS: #{inspect(reason)}")

        Health.update(consumer, :push, :error, reason)

        Enum.each(messages, fn msg ->
          Sequin.Logs.log_for_consumer_message(
            :error,
            consumer.account_id,
            msg.replication_message_trace_id,
            "Failed to push message to SQS: #{inspect(reason)}"
          )
        end)

        Broadway.Message.failed(message, reason)
    end
  end

  @spec build_sqs_message(DestinationConsumer.t(), ConsumerRecordData.t() | ConsumerEventData.t()) :: map()
  defp build_sqs_message(consumer, record_or_event_data) do
    message = %{
      message_body: record_or_event_data,
      id: UUID.uuid4()
    }

    if consumer.destination.is_fifo do
      group_id =
        consumer
        |> Consumers.group_column_values(record_or_event_data)
        |> Enum.join(",")

      Map.put(message, :message_group_id, group_id)
      # TODO: Implement deduplication -
      # |> Map.put(:message_deduplication_id, message.id)
    else
      message
    end
  end

  defp format_error(error) do
    Error.service(
      service: :sqs,
      code: "batch_error",
      message: "SQS batch send failed",
      details: %{error: error}
    )
  end
end

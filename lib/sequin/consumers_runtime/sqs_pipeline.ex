defmodule Sequin.ConsumersRuntime.SqsPipeline do
  @moduledoc false
  use Broadway

  alias AWS.Client
  alias Sequin.Aws.HttpClient
  alias Sequin.Aws.SQS
  alias Sequin.Consumers.DestinationConsumer
  alias Sequin.Databases.PostgresDatabaseTable
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

    # TODO: Make this ready for multiple sequences
    table = Sequin.Enum.find!(consumer.sequence.postgres_database.tables, &(&1.oid == consumer.sequence.table_oid))
    group_column_attnums = consumer.sequence_filter.group_column_attnums
    group_column_names = PostgresDatabaseTable.column_attnums_to_names(table, group_column_attnums)

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
        group_column_names: group_column_names,
        sqs_client: build_sqs_client(consumer.destination)
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
  def handle_message(_, %Broadway.Message{data: messages} = message, %{
        consumer: consumer,
        sqs_client: sqs_client,
        group_column_names: group_column_names
      }) do
    Logger.metadata(
      account_id: consumer.account_id,
      consumer_id: consumer.id
    )

    sqs_messages = Enum.map(messages, &build_sqs_message(&1.data, group_column_names))

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

  defp build_sqs_client(destination) do
    destination.access_key_id
    |> Client.create(destination.access_key_secret, destination.region)
    |> Map.put(:endpoint, destination.endpoint)
    |> HttpClient.put_client()
  end

  defp build_sqs_message(message_data, group_column_names) do
    message = %{
      message_body: message_data
    }

    if message_data.destination.is_fifo do
      group_id =
        Enum.map_join(group_column_names, ",", fn col_name ->
          Map.get(message_data.record, col_name)
        end)

      Map.put(message, :message_group_id, group_id)
      # TODO: Implement deduplication -
      # |> Map.put(:message_deduplication_id, message_data.id)
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

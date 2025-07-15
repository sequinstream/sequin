defmodule Sequin.Runtime.SqsPipeline do
  @moduledoc false
  @behaviour Sequin.Runtime.SinkPipeline

  alias Sequin.Aws.SQS
  alias Sequin.AwsMock
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Consumers.SqsSink
  alias Sequin.Error
  alias Sequin.Runtime.Routing
  alias Sequin.Runtime.SinkPipeline

  require Logger

  @impl SinkPipeline
  def init(context, _opts) do
    %{consumer: consumer, test_pid: test_pid} = context

    setup_allowances(test_pid)

    case SqsSink.aws_client(consumer.sink) do
      {:ok, client} ->
        Map.put(context, :sqs_client, client)

      {:error, reason} ->
        raise "Failed to initialize SQS client: #{inspect(reason)}"
    end
  end

  @impl SinkPipeline
  def batchers_config(consumer) do
    # SQS has a maximum of 10 messages per batch request
    batch_size = min(consumer.batch_size, 10)
    batch_timeout = consumer.batch_timeout_ms || 5

    # Calculate concurrency based on system resources
    # Can be overridden by DEFAULT_WORKERS_PER_SINK env var
    concurrency = min(System.schedulers_online() * 2, 80)

    [
      default: [
        concurrency: concurrency,
        batch_size: batch_size,
        batch_timeout: batch_timeout
      ]
    ]
  end

  @impl SinkPipeline
  def handle_message(message, context) do
    %{consumer: consumer, test_pid: test_pid} = context
    setup_allowances(test_pid)

    %Routing.Consumers.Sqs{queue_url: queue_url} = Routing.route_message(consumer, message.data)

    # For FIFO queues with message grouping enabled, we need to batch by both queue_url and group_id
    # to ensure messages with the same MessageGroupId are in the same batch
    batch_key =
      if consumer.sink.is_fifo and consumer.message_grouping do
        {queue_url, message.data.group_id}
      else
        {queue_url, nil}
      end

    message = Broadway.Message.put_batch_key(message, batch_key)

    {:ok, message, context}
  end

  @impl SinkPipeline
  def handle_batch(:default, messages, %{batch_key: {queue_url, _group_id}}, context) do
    %{
      consumer: %SinkConsumer{} = consumer,
      sqs_client: sqs_client,
      test_pid: test_pid
    } = context

    setup_allowances(test_pid)

    sqs_messages =
      Enum.map(messages, fn %{data: data} ->
        build_sqs_message(consumer, data)
      end)

    case SQS.send_messages(sqs_client, queue_url, sqs_messages) do
      :ok ->
        {:ok, messages, context}

      {:error, error} ->
        {:error, Error.service(service: "sqs_pipeline", code: :unknown_error, message: inspect(error))}
    end
  end

  @spec build_sqs_message(SinkConsumer.t(), term()) :: map()
  defp build_sqs_message(consumer, record_or_event) do
    message = %{
      message_body: Sequin.Transforms.Message.to_external(consumer, record_or_event),
      id: UUID.uuid4()
    }

    # TODO Consider moving this to routing layer
    if consumer.sink.is_fifo do
      message
      |> Map.put(
        :message_deduplication_id,
        Sequin.Aws.message_deduplication_id(record_or_event.data.metadata.idempotency_key)
      )
      |> maybe_add_message_group_id(consumer, record_or_event)
    else
      message
    end
  end

  defp maybe_add_message_group_id(message, consumer, record_or_event) do
    if consumer.message_grouping do
      Map.put(message, :message_group_id, Sequin.Aws.message_group_id(record_or_event.group_id))
    else
      # When message grouping is disabled, use a constant group ID
      # This ensures all messages go to the same queue order but without grouping
      Map.put(message, :message_group_id, "default")
    end
  end

  defp setup_allowances(nil), do: :ok

  defp setup_allowances(test_pid) do
    Req.Test.allow(Sequin.Aws.HttpClient, test_pid, self())
    Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
    Mox.allow(AwsMock, test_pid, self())
  end
end

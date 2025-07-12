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
  def batchers_config(_consumer) do
    [
      default: [
        concurrency: 400,
        batch_size: 10,
        batch_timeout: 1
      ]
    ]
  end

  @impl SinkPipeline
  def handle_message(message, context) do
    %{consumer: consumer, test_pid: test_pid} = context
    setup_allowances(test_pid)

    %Routing.Consumers.Sqs{queue_url: queue_url} = Routing.route_message(consumer, message.data)

    message = Broadway.Message.put_batch_key(message, queue_url)

    {:ok, message, context}
  end

  @impl SinkPipeline
  def handle_batch(:default, messages, %{batch_key: queue_url}, context) do
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
      |> Map.put(:message_group_id, Sequin.Aws.message_group_id(record_or_event.group_id))
    else
      message
    end
  end

  defp setup_allowances(nil), do: :ok

  defp setup_allowances(test_pid) do
    Req.Test.allow(Sequin.Aws.HttpClient, test_pid, self())
    Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
    Mox.allow(AwsMock, test_pid, self())
  end
end

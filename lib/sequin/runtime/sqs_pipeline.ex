defmodule Sequin.Runtime.SqsPipeline do
  @moduledoc false
  @behaviour Sequin.Runtime.SinkPipeline

  alias Sequin.Aws.SQS
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Consumers.SqsSink
  alias Sequin.Error
  alias Sequin.Runtime.SinkPipeline

  require Logger

  @impl SinkPipeline
  def init(context, _opts) do
    %{consumer: consumer} = context
    Map.put(context, :sqs_client, SqsSink.aws_client(consumer.sink))
  end

  @impl SinkPipeline
  def batchers_config(_consumer) do
    concurrency = min(System.schedulers_online() * 2, 80)

    [
      default: [
        concurrency: concurrency,
        batch_size: 10,
        batch_timeout: 50
      ]
    ]
  end

  @impl SinkPipeline
  def handle_batch(:default, messages, _batch_info, context) do
    %{
      consumer: %SinkConsumer{sink: sink} = consumer,
      sqs_client: sqs_client,
      test_pid: test_pid
    } = context

    setup_allowances(test_pid)

    sqs_messages =
      Enum.map(messages, fn %{data: data} ->
        build_sqs_message(consumer, data)
      end)

    case SQS.send_messages(sqs_client, sink.queue_url, sqs_messages) do
      :ok ->
        {:ok, messages, context}

      {:error, error, _} ->
        {:error, Error.service(service: "sqs_pipeline", code: :unknown_error, message: inspect(error))}
    end
  end

  @spec build_sqs_message(SinkConsumer.t(), term()) :: map()
  defp build_sqs_message(consumer, record_or_event) do
    message = %{
      message_body: Sequin.Transforms.Message.to_external(consumer, record_or_event),
      id: UUID.uuid4()
    }

    if consumer.sink.is_fifo do
      message
      |> Map.put(:message_deduplication_id, record_or_event.data.metadata.idempotency_key)
      |> Map.put(:message_group_id, record_or_event.group_id)
    else
      message
    end
  end

  defp setup_allowances(nil), do: :ok

  defp setup_allowances(test_pid) do
    Req.Test.allow(Sequin.Aws.HttpClient, test_pid, self())
    Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
  end
end

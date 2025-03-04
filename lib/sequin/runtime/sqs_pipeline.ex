defmodule Sequin.Runtime.SqsPipeline do
  @moduledoc false
  @behaviour Sequin.Runtime.SinkPipeline

  alias Sequin.Aws.SQS
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Consumers.SqsSink
  alias Sequin.Runtime.SinkPipeline

  require Logger

  @impl SinkPipeline
  def init(context, _opts) do
    %{consumer: consumer} = context
    Map.put(context, :sqs_client, SqsSink.aws_client(consumer.sink))
  end

  @impl SinkPipeline
  def processors_config(%SinkConsumer{max_waiting: max_waiting}) do
    [
      default: [
        concurrency: max_waiting,
        max_demand: 10,
        min_demand: 5
      ]
    ]
  end

  @impl SinkPipeline
  def batchers_config(%SinkConsumer{batch_size: batch_size}) do
    [
      default: [
        concurrency: 1,
        batch_size: batch_size,
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
        build_sqs_message(consumer, data.data)
      end)

    case SQS.send_messages(sqs_client, sink.queue_url, sqs_messages) do
      :ok ->
        {:ok, messages, context}

      {:error, error} ->
        {:error, error}
    end
  end

  @spec build_sqs_message(SinkConsumer.t(), term()) :: map()
  defp build_sqs_message(consumer, record_or_event_data) do
    message = %{
      message_body: record_or_event_data,
      id: UUID.uuid4()
    }

    if consumer.sink.is_fifo do
      group_id =
        consumer
        |> Sequin.Consumers.group_column_values(record_or_event_data)
        |> Enum.join(",")

      Map.put(message, :message_group_id, group_id)
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

defmodule Sequin.Runtime.SnsPipeline do
  @moduledoc false
  @behaviour Sequin.Runtime.SinkPipeline

  alias Sequin.Aws.SNS
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Consumers.SnsSink
  alias Sequin.Runtime.SinkPipeline

  require Logger

  @impl SinkPipeline
  def init(context, _opts) do
    %{consumer: consumer} = context
    Map.put(context, :sns_client, SnsSink.aws_client(consumer.sink))
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
      sns_client: sns_client,
      test_pid: test_pid
    } = context

    setup_allowances(test_pid)

    sns_messages =
      Enum.map(messages, fn %{data: data} ->
        build_sns_message(consumer, data)
      end)

    case SNS.publish_messages(sns_client, sink.topic_arn, sns_messages) do
      :ok ->
        {:ok, messages, context}

      {:error, error} ->
        {:error, error}
    end
  end

  @spec build_sns_message(SinkConsumer.t(), term()) :: map()
  defp build_sns_message(consumer, record_or_event) do
    message = %{
      message: Sequin.Transforms.Message.to_external(consumer, record_or_event),
      message_id: UUID.uuid4()
    }

    if consumer.sink.is_fifo do
      group_id =
        consumer
        |> Sequin.Consumers.group_column_values(record_or_event.data)
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

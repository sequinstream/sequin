defmodule Sequin.Runtime.SnsPipeline do
  @moduledoc false
  @behaviour Sequin.Runtime.SinkPipeline

  alias Sequin.Aws.SNS
  alias Sequin.AwsMock
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Consumers.SnsSink
  alias Sequin.Runtime.Routing
  alias Sequin.Runtime.SinkPipeline

  require Logger

  @impl SinkPipeline
  def init(context, _opts) do
    %{consumer: consumer, test_pid: test_pid} = context

    setup_allowances(test_pid)

    case SnsSink.aws_client(consumer.sink) do
      {:ok, client} ->
        Map.put(context, :sns_client, client)

      {:error, reason} ->
        raise "Failed to initialize SNS client: #{inspect(reason)}"
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

    %Routing.Consumers.Sns{topic_arn: topic_arn} = Routing.route_message(consumer, message.data)

    message = Broadway.Message.put_batch_key(message, topic_arn)

    {:ok, message, context}
  end

  @impl SinkPipeline
  def handle_batch(:default, messages, %{batch_key: topic_arn}, context) do
    %{
      consumer: %SinkConsumer{} = consumer,
      sns_client: sns_client,
      test_pid: test_pid
    } = context

    setup_allowances(test_pid)

    sns_messages =
      Enum.map(messages, fn message ->
        build_sns_message(consumer, message)
      end)

    case SNS.publish_messages(sns_client, topic_arn, sns_messages) do
      :ok ->
        {:ok, messages, context}

      {:error, error} ->
        {:error, error}
    end
  end

  @spec build_sns_message(
          SinkConsumer.t(),
          Sequin.Consumers.ConsumerEvent.t() | Sequin.Consumers.ConsumerRecord.t()
        ) :: map()
  defp build_sns_message(consumer, record_or_event) do
    consumer_data = record_or_event.data

    message = %{
      message_id: UUID.uuid4(),
      message: Sequin.Transforms.Message.to_external(consumer, consumer_data)
    }

    if consumer.sink.is_fifo do
      message
      |> Map.put(:message_group_id, Sequin.Aws.message_group_id(consumer_data.group_id))
      |> Map.put(
        :message_deduplication_id,
        Sequin.Aws.message_deduplication_id(consumer_data.data.metadata.idempotency_key)
      )
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

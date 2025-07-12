defmodule Sequin.Runtime.KinesisPipeline do
  @moduledoc false
  @behaviour Sequin.Runtime.SinkPipeline

  alias Sequin.Aws.Kinesis
  alias Sequin.AwsMock
  alias Sequin.Consumers.KinesisSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Runtime.Routing
  alias Sequin.Runtime.SinkPipeline

  @impl SinkPipeline
  def init(context, _opts) do
    %{consumer: consumer, test_pid: test_pid} = context

    setup_allowances(test_pid)

    case KinesisSink.aws_client(consumer.sink) do
      {:ok, client} ->
        Map.put(context, :kinesis_client, client)

      {:error, reason} ->
        raise "Failed to initialize Kinesis client: #{inspect(reason)}"
    end
  end

  @impl SinkPipeline
  def batchers_config(_consumer) do
    [
      default: [
        concurrency: 400,
        batch_size: 10,
        batch_timeout: 5
      ]
    ]
  end

  @impl SinkPipeline
  def handle_message(message, context) do
    %{consumer: consumer, test_pid: test_pid} = context
    setup_allowances(test_pid)

    %Routing.Consumers.Kinesis{stream_arn: stream_arn} = Routing.route_message(consumer, message.data)

    message = Broadway.Message.put_batch_key(message, stream_arn)

    {:ok, message, context}
  end

  @impl SinkPipeline
  def handle_batch(:default, messages, %{batch_key: stream_arn}, context) do
    %{
      consumer: %SinkConsumer{} = consumer,
      kinesis_client: kinesis_client,
      test_pid: test_pid
    } = context

    setup_allowances(test_pid)

    records =
      Enum.map(messages, fn message -> build_kinesis_record(consumer, message) end)

    case Kinesis.put_records(kinesis_client, stream_arn, records) do
      :ok -> {:ok, messages, context}
      {:error, error} -> {:error, error}
    end
  end

  @spec build_kinesis_record(
          SinkConsumer.t(),
          Sequin.Consumers.ConsumerEvent.t() | Sequin.Consumers.ConsumerRecord.t()
        ) :: map()
  defp build_kinesis_record(consumer, message) do
    consumer_data = message.data

    %{
      "Data" => Base.encode64(Jason.encode!(Sequin.Transforms.Message.to_external(consumer, consumer_data))),
      "PartitionKey" => consumer_data.group_id
    }
  end

  defp setup_allowances(nil), do: :ok

  defp setup_allowances(test_pid) do
    Req.Test.allow(Sequin.Aws.HttpClient, test_pid, self())
    Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
    Mox.allow(AwsMock, test_pid, self())
  end
end

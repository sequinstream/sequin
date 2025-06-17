defmodule Sequin.Runtime.KinesisPipeline do
  @moduledoc false
  @behaviour Sequin.Runtime.SinkPipeline

  alias Sequin.Aws.Kinesis
  alias Sequin.Consumers.KinesisSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Runtime.SinkPipeline

  @impl SinkPipeline
  def init(context, _opts) do
    %{consumer: consumer} = context
    Map.put(context, :kinesis_client, KinesisSink.aws_client(consumer.sink))
  end

  @impl SinkPipeline
  def batchers_config(_consumer) do
    [
      default: [
        concurrency: 400,
        batch_size: 10,
        batch_timeout: 50
      ]
    ]
  end

  @impl SinkPipeline
  def handle_batch(:default, messages, _batch_info, context) do
    %{consumer: %SinkConsumer{sink: sink} = consumer, kinesis_client: kinesis_client, test_pid: test_pid} = context

    setup_allowances(test_pid)

    records =
      Enum.map(messages, fn %{data: data} ->
        %{
          "Data" => Base.encode64(Jason.encode!(Sequin.Transforms.Message.to_external(consumer, data))),
          "PartitionKey" => data.group_id
        }
      end)

    case Kinesis.put_records(kinesis_client, sink.stream_arn, records) do
      :ok -> {:ok, messages, context}
      {:error, error} -> {:error, error}
    end
  end

  defp setup_allowances(nil), do: :ok

  defp setup_allowances(test_pid) do
    Req.Test.allow(Sequin.Aws.HttpClient, test_pid, self())
    Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
  end
end

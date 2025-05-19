defmodule Sequin.Runtime.KinesisPipeline do
  @moduledoc false
  @behaviour Sequin.Runtime.SinkPipeline

  alias Sequin.Aws.Kinesis
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Consumers.KinesisSink
  alias Sequin.Runtime.SinkPipeline

  @impl SinkPipeline
  def init(context, _opts) do
    %{consumer: consumer} = context
    Map.put(context, :kinesis_client, KinesisSink.aws_client(consumer.sink))
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
    %{consumer: %SinkConsumer{sink: sink} = consumer, kinesis_client: kinesis_client, test_pid: test_pid} = context

    setup_allowances(test_pid)

    records =
      Enum.map(messages, fn %{data: data} ->
        partition_key =
          consumer
          |> Sequin.Consumers.group_column_values(data)
          |> Enum.join(",")
          |> case do
            "" -> UUID.uuid4()
            key -> key
          end

        %{
          "Data" => Jason.encode!(Sequin.Transforms.Message.to_external(consumer, data)),
          "PartitionKey" => partition_key
        }
      end)

    case Kinesis.put_records(kinesis_client, sink.stream_name, records) do
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

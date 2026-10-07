defmodule Sequin.Runtime.KinesisPipeline do
  @moduledoc false
  @behaviour Sequin.Runtime.SinkPipeline

  alias Sequin.Aws.Kinesis
  alias Sequin.AwsMock
  alias Sequin.Consumers.KinesisSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Runtime.Routing
  alias Sequin.Runtime.SinkPipeline

  require Logger

  # Kinesis PartitionKey has a maximum of 256 bytes
  @max_partition_key_bytes 256

  # aws_credentials refreshes temporary credentials 5 minutes before they expire,
  # so rebuilding the client at most once a minute always picks up a valid token.
  @default_task_role_client_refresh_seconds 60

  @impl SinkPipeline
  def init(context, _opts) do
    %{consumer: consumer, test_pid: test_pid} = context

    setup_allowances(test_pid)

    case KinesisSink.aws_client(consumer.sink) do
      {:ok, client} ->
        context
        |> Map.put(:kinesis_client, client)
        |> Map.put(:kinesis_client_built_at, now_seconds())

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
      test_pid: test_pid
    } = context

    setup_allowances(test_pid)

    # With task role (ECS task role, IRSA, instance profile) the credentials are
    # temporary. aws_credentials refreshes them before expiry, but the client built
    # in init/2 keeps the original token, so rebuild it periodically.
    context = maybe_refresh_client(context)

    records =
      Enum.map(messages, fn message -> build_kinesis_record(consumer, message) end)

    case Kinesis.put_records(context.kinesis_client, stream_arn, records) do
      :ok -> {:ok, messages, context}
      {:error, error} -> {:error, error}
    end
  end

  @spec build_kinesis_record(
          SinkConsumer.t(),
          Sequin.Consumers.ConsumerEvent.t()
        ) :: map()
  defp build_kinesis_record(consumer, message) do
    consumer_data = message.data

    %{
      "Data" => Base.encode64(Jason.encode!(Sequin.Transforms.Message.to_external(consumer, consumer_data))),
      "PartitionKey" => Sequin.String.truncate_with_hash(consumer_data.group_id, @max_partition_key_bytes)
    }
  end

  defp maybe_refresh_client(%{consumer: %SinkConsumer{sink: %KinesisSink{use_task_role: true} = sink}} = context) do
    built_at = Map.get(context, :kinesis_client_built_at)

    # monotonic time can be negative, so a missing timestamp is checked with is_nil
    if is_nil(built_at) or now_seconds() - built_at >= task_role_client_refresh_seconds() do
      case KinesisSink.aws_client(sink) do
        {:ok, client} ->
          context
          |> Map.put(:kinesis_client, client)
          |> Map.put(:kinesis_client_built_at, now_seconds())

        {:error, reason} ->
          Logger.warning("[KinesisPipeline] Failed to refresh AWS client for task role: #{inspect(reason)}")
          context
      end
    else
      context
    end
  end

  defp maybe_refresh_client(context), do: context

  defp now_seconds, do: System.monotonic_time(:second)

  defp task_role_client_refresh_seconds do
    Keyword.get(
      Application.get_env(:sequin, __MODULE__, []),
      :task_role_client_refresh_seconds,
      @default_task_role_client_refresh_seconds
    )
  end

  defp setup_allowances(nil), do: :ok

  defp setup_allowances(test_pid) do
    Req.Test.allow(Sequin.Aws.HttpClient, test_pid, self())
    Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
    Mox.allow(AwsMock, test_pid, self())
  end
end

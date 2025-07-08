defmodule Sequin.Runtime.S2Pipeline do
  @moduledoc false
  @behaviour Sequin.Runtime.SinkPipeline

  alias Sequin.Consumers.S2Sink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Runtime.Routing
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Sinks.S2.Client
  alias Sequin.Transforms.Message

  require Logger

  @impl SinkPipeline
  def init(context, _opts) do
    context
  end

  @impl SinkPipeline
  def batchers_config(_consumer) do
    concurrency = min(System.schedulers_online() * 2, 80)

    [
      default: [
        concurrency: concurrency,
        batch_size: 10,
        batch_timeout: 5
      ]
    ]
  end

  @impl SinkPipeline
  def handle_message(message, context) do
    %{consumer: consumer} = context

    %Routing.Consumers.S2{basin: basin, stream: stream} =
      Routing.route_message(consumer, message.data)

    message = Broadway.Message.put_batch_key(message, {basin, stream})

    {:ok, message, context}
  end

  @impl SinkPipeline
  def handle_batch(:default, messages, %{batch_key: {basin, stream}}, context) do
    %{consumer: %SinkConsumer{sink: %S2Sink{} = sink} = consumer, test_pid: test_pid} = context

    sink = %{sink | basin: basin, stream: stream}

    setup_allowances(test_pid)

    records =
      Enum.map(messages, fn %{data: data} ->
        %{"body" => Jason.encode!(Message.to_external(consumer, data))}
      end)

    case Client.append_records(sink, records) do
      :ok -> {:ok, messages, context}
      {:error, error} -> {:error, error}
    end
  end

  defp setup_allowances(nil), do: :ok

  defp setup_allowances(test_pid) do
    Req.Test.allow(Client, test_pid, self())
    Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
  end
end

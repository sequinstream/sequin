defmodule Sequin.Runtime.S2Pipeline do
  @moduledoc false
  @behaviour Sequin.Runtime.SinkPipeline

  alias Sequin.Consumers.S2Sink
  alias Sequin.Consumers.SinkConsumer
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
        batch_timeout: 50
      ]
    ]
  end

  @impl SinkPipeline
  def handle_batch(:default, messages, _batch_info, context) do
    %{consumer: %SinkConsumer{sink: %S2Sink{} = sink} = consumer, test_pid: test_pid} = context

    setup_allowances(test_pid)

    records =
      Enum.map(messages, fn %{data: data} ->
        # Body has to be serialized as a string
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

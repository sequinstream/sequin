defmodule Sequin.Runtime.NatsPipeline do
  @moduledoc false
  @behaviour Sequin.Runtime.SinkPipeline

  alias Sequin.Error
  alias Sequin.Runtime.Routing
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Sinks.Nats

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
    %{consumer: consumer, test_pid: test_pid} = context
    setup_allowances(test_pid)

    routed_messages = Routing.route_and_transform_messages(consumer, messages)

    case Nats.send_messages(consumer, routed_messages) do
      :ok ->
        {:ok, messages, context}

      {:error, error} ->
        reason =
          Error.service(
            service: :nats,
            code: "publish_error",
            message: "NATS publish failed",
            details: %{error: error}
          )

        {:error, reason}
    end
  end

  defp setup_allowances(nil), do: :ok

  defp setup_allowances(test_pid) do
    Mox.allow(Sequin.Sinks.NatsMock, test_pid, self())
    Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
  end
end

defmodule Sequin.Runtime.NatsPipeline do
  @moduledoc false
  @behaviour Sequin.Runtime.SinkPipeline

  alias Sequin.Consumers.Function
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Error
  alias Sequin.Functions.MiniElixir
  alias Sequin.Runtime.RoutingInfo
  alias Sequin.Runtime.RoutingInfo.RoutedMessage
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Sinks.Nats
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
  def apply_routing(consumer, rinfo) do
    RoutingInfo.apply_routing(consumer.type, rinfo)
  end

  @impl SinkPipeline
  def handle_batch(:default, messages, _batch_info, context) do
    %{consumer: consumer, test_pid: test_pid} = context
    setup_allowances(test_pid)

    # Transform messages and apply routing
    nats_messages =
      Enum.map(messages, fn %{data: message} ->
        maybe_apply_routing(consumer, message)
      end)

    case Nats.send_messages(consumer, nats_messages) do
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

  defp maybe_apply_routing(%SinkConsumer{routing: %Function{} = routing} = consumer, message) do
    routing_info = MiniElixir.run_compiled(routing, message.data)
    validated_routing = apply_routing(consumer, routing_info)

    payload = Message.to_external(consumer, message)

    %RoutedMessage{
      original_message: message,
      routing_info: validated_routing,
      payload: payload
    }
  end

  defp maybe_apply_routing(%SinkConsumer{routing: nil}, message) do
    message
  end

  defp setup_allowances(nil), do: :ok

  defp setup_allowances(test_pid) do
    Mox.allow(Sequin.Sinks.NatsMock, test_pid, self())
    Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
  end
end

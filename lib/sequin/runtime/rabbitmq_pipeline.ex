defmodule Sequin.Runtime.RabbitMqPipeline do
  @moduledoc false
  @behaviour Sequin.Runtime.SinkPipeline

  alias Sequin.Error
  alias Sequin.Runtime.Routing
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Sinks.RabbitMq

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
  def handle_batch(:default, messages, _batch_info, context) do
    %{consumer: consumer, test_pid: test_pid} = context
    setup_allowances(test_pid)

    routed_messages = Routing.route_and_transform_messages(consumer, Enum.map(messages, & &1.data))

    case RabbitMq.send_messages(consumer, routed_messages) do
      :ok ->
        {:ok, messages, context}

      {:error, error} ->
        reason =
          Error.service(
            service: :rabbitmq,
            code: "publish_error",
            message: "RabbitMQ publish failed",
            details: %{error: error}
          )

        {:error, reason}
    end
  end

  defp setup_allowances(nil), do: :ok

  defp setup_allowances(test_pid) do
    Mox.allow(Sequin.Sinks.RabbitMqMock, test_pid, self())
    Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
  end
end

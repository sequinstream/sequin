defmodule Sequin.Runtime.RabbitMqPipeline do
  @moduledoc false
  @behaviour Sequin.Runtime.SinkPipeline

  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Error
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Sinks.RabbitMq

  require Logger

  @impl SinkPipeline
  def init(context, _opts) do
    context
  end

  @impl SinkPipeline
  def processors_config(%SinkConsumer{max_waiting: max_waiting}) do
    [
      default: [
        concurrency: max_waiting,
        max_demand: 10,
        min_demand: 5
      ]
    ]
  end

  @impl SinkPipeline
  def batchers_config(%SinkConsumer{batch_size: batch_size}) do
    [
      default: [
        concurrency: 1,
        batch_size: batch_size,
        batch_timeout: 50
      ]
    ]
  end

  @impl SinkPipeline
  def handle_batch(:default, messages, _batch_info, context) do
    %{consumer: consumer, test_pid: test_pid} = context
    setup_allowances(test_pid)

    messages_to_send = Enum.map(messages, & &1.data)

    case RabbitMq.send_messages(consumer.sink, messages_to_send) do
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

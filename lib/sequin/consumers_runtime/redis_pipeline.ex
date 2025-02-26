defmodule Sequin.ConsumersRuntime.RedisPipeline do
  @moduledoc false
  @behaviour Sequin.ConsumersRuntime.SinkPipeline

  alias Sequin.Consumers.RedisSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.ConsumersRuntime.SinkPipeline
  alias Sequin.Sinks.Redis

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
  def handle_message(message, _context) do
    # Just pass the message through - all work happens in handle_batch
    message
  end

  @impl SinkPipeline
  def handle_batch(:default, messages, _batch_info, context) do
    %{consumer: %SinkConsumer{sink: %RedisSink{} = sink}, test_pid: test_pid} = context
    setup_allowances(test_pid)

    redis_messages =
      Enum.flat_map(messages, fn message ->
        Enum.map(message.data, & &1.data)
      end)

    case Redis.send_messages(sink, redis_messages) do
      :ok ->
        SinkPipeline.on_success(context, messages)

      {:error, error} when is_exception(error) ->
        SinkPipeline.on_failure(context, error, messages)
    end
  end

  defp setup_allowances(nil), do: :ok

  defp setup_allowances(test_pid) do
    Mox.allow(Sequin.Sinks.RedisMock, test_pid, self())
    Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
  end
end

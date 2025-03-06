defmodule Sequin.Runtime.RedisPipeline do
  @moduledoc false
  @behaviour Sequin.Runtime.SinkPipeline

  alias Sequin.Consumers.RedisSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Sinks.Redis

  require Logger

  @impl SinkPipeline
  def init(context, _opts) do
    context
  end

  @impl SinkPipeline
  def handle_batch(:default, messages, _batch_info, context) do
    %{consumer: %SinkConsumer{sink: %RedisSink{} = sink}, test_pid: test_pid} = context
    setup_allowances(test_pid)

    redis_messages = Enum.map(messages, & &1.data.data)

    case Redis.send_messages(sink, redis_messages) do
      :ok ->
        {:ok, messages, context}

      {:error, error} when is_exception(error) ->
        {:error, error}
    end
  end

  defp setup_allowances(nil), do: :ok

  defp setup_allowances(test_pid) do
    Mox.allow(Sequin.Sinks.RedisMock, test_pid, self())
    Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
  end
end

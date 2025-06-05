defmodule Sequin.Runtime.RedisStreamPipeline do
  @moduledoc false
  @behaviour Sequin.Runtime.SinkPipeline

  alias Sequin.Consumers.RedisStreamSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Sinks.Redis
  alias Sequin.Transforms.Message

  require Logger

  @impl SinkPipeline
  def init(context, _opts) do
    context
  end

  @impl SinkPipeline
  def handle_batch(:default, messages, _batch_info, context) do
    %{consumer: %SinkConsumer{sink: %RedisStreamSink{}} = consumer, test_pid: test_pid} = context
    setup_allowances(test_pid)

    redis_messages =
      Enum.map(messages, fn %{data: message} ->
        transformed_data = Message.to_external(consumer, message)
        %{message | data: transformed_data}
      end)

    case Redis.send_messages(consumer, redis_messages) do
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

defmodule Sequin.Runtime.RedisStringPipeline do
  @moduledoc false
  @behaviour Sequin.Runtime.SinkPipeline

  alias Sequin.Consumers.RedisStringSink
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
    %{consumer: %SinkConsumer{sink: %RedisStringSink{} = sink} = consumer, test_pid: test_pid} = context
    setup_allowances(test_pid)

    # TODO: use a router
    redis_messages =
      Enum.map(messages, fn %{data: data} ->
        record_pks = data.record_pks
        table_schema = data.data.metadata.table_schema
        table_name = data.data.metadata.table_name

        pks = Enum.join(record_pks, "-")
        key = "sequin:#{table_schema}:#{table_name}:#{pks}"

        message =
          case Sequin.Transforms.Message.to_external(consumer, data) do
            message when is_binary(message) or is_number(message) -> message
            message -> Jason.encode!(message)
          end

        %{key: key, value: message, expire_ms: sink.expire_ms}
      end)

    case Redis.set_messages(sink, redis_messages) do
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

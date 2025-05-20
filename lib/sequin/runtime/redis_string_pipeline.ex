defmodule Sequin.Runtime.RedisStringPipeline do
  @moduledoc false
  @behaviour Sequin.Runtime.SinkPipeline

  alias Sequin.Consumers.Function
  alias Sequin.Consumers.RedisStringSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Error
  alias Sequin.Functions.MiniElixir
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Sinks.Redis

  require Logger

  defmodule RoutingInfo do
    @moduledoc false
    @derive Jason.Encoder
    defstruct key: ""
  end

  @impl SinkPipeline
  def init(context, _opts) do
    context
  end

  @impl SinkPipeline
  def apply_routing(_consumer, rinfo) when is_map(rinfo) do
    struct!(RoutingInfo, rinfo)
  rescue
    KeyError ->
      expected_keys =
        RoutingInfo.__struct__()
        |> Map.keys()
        |> Enum.reject(&(&1 == :__struct__))
        |> Enum.join(", ")

      raise Error.invariant(
              message: "Invalid routing response. Expected a map with keys: #{expected_keys}, got: #{inspect(rinfo)}"
            )
  end

  @impl SinkPipeline
  def handle_batch(:default, messages, _batch_info, context) do
    %{consumer: %SinkConsumer{sink: %RedisStringSink{} = sink} = consumer, test_pid: test_pid} = context
    setup_allowances(test_pid)

    redis_messages =
      Enum.map(messages, fn %{data: message} ->
        %{key: key} = maybe_apply_routing(consumer, message)

        action =
          case message.data do
            %{action: :insert} -> :set
            %{action: :update} -> :set
            %{action: :delete} -> :del
            %{action: :read} -> :set
          end

        message =
          case Sequin.Transforms.Message.to_external(consumer, message) do
            message when is_binary(message) or is_number(message) -> message
            message -> Jason.encode!(message)
          end

        %{action: action, key: key, value: message, expire_ms: sink.expire_ms}
      end)

    case Redis.set_messages(sink, redis_messages) do
      :ok ->
        {:ok, messages, context}

      {:error, error} when is_exception(error) ->
        {:error, error}
    end
  end

  defp maybe_apply_routing(%SinkConsumer{routing: %Function{} = routing} = consumer, message) do
    res = MiniElixir.run_compiled(routing, message.data)
    apply_routing(consumer, res)
  end

  defp maybe_apply_routing(%SinkConsumer{routing: nil}, message) do
    record_pks = message.record_pks
    table_name = message.data.metadata.table_name
    pks = Enum.join(record_pks, "-")

    %{key: "sequin:#{table_name}:#{pks}"}
  end

  defp setup_allowances(nil), do: :ok

  defp setup_allowances(test_pid) do
    Mox.allow(Sequin.Sinks.RedisMock, test_pid, self())
    Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
  end
end

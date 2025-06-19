defmodule Sequin.Runtime.RoutingInfo do
  @moduledoc """
  Defines routing info structs for different sink types.
  These structs are used to validate and type-check routing function responses.
  """

  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Functions.MiniElixir

  defmodule RoutedMessage do
    @moduledoc """
    Generic wrapper for routed messages contains the sink-specific routing_info
    and the transformed and encoded payload.
    """
    defstruct [:routing_info, :payload]

    @type t :: %__MODULE__{
            routing_info: struct(),
            payload: any()
          }
  end

  defmodule RoutedSink do
    @moduledoc false
    @doc """
    Behavior for routing sink modules

    Implementing modules should define a struct and implement the route/4 and encode/1 callbacks.
    The route/4 function should return a struct of the implementing module's type.
    """
    defmacro __using__(struct_attrs) do
      quote do
        @behaviour RoutedSink
        defstruct unquote(struct_attrs)

        defimpl Jason.Encoder do
          def encode(message, opts) do
            message
            |> Map.from_struct()
            |> Jason.Encode.map(opts)
          end
        end
      end
    end

    # TODO Keep the callback spec specific with the module's struct
    @callback route(action :: atom(), record :: struct(), changes :: struct(), metadata :: struct()) :: struct()
    @callback apply_consumer_config(routing_info :: struct(), consumer :: struct()) :: struct()
    @callback encode(message :: struct()) :: any()
  end

  defmodule HttpPush do
    @moduledoc false
    use RoutedSink, method: "POST", endpoint_path: "/", headers: %{}

    def route(_action, _record, _changes, metadata) do
      struct(__MODULE__, %{
        method: "POST",
        endpoint_path: "/",
        headers: %{"Idempotency-Key" => metadata.idempotency_key}
      })
    end

    def apply_consumer_config(routing_info, %{sink: sink}) do
      # Apply consumer-configured settings like http_endpoint_path
      %{routing_info | endpoint_path: sink.http_endpoint_path || routing_info.endpoint_path}
    end

    def encode(message) do
      Jason.encode!(message)
    end
  end

  defmodule RedisString do
    @moduledoc false
    use RoutedSink, [:key, :action]

    def route(action, _record, _changes, metadata) do
      table_name = metadata.table_name
      pks = Enum.join(metadata.record_pks, "-")

      redis_action =
        case action do
          :insert -> :set
          :update -> :set
          :delete -> :del
          :read -> :set
        end

      struct(__MODULE__, %{key: "sequin:#{table_name}:#{pks}", action: redis_action})
    end

    def apply_consumer_config(routing_info, _consumer) do
      # RedisString doesn't currently use consumer-specific configuration
      routing_info
    end

    def encode(message) when is_binary(message) or is_number(message), do: message
    def encode(message), do: Jason.encode!(message)
  end

  defmodule Nats do
    @moduledoc false
    use RoutedSink, [:subject, :headers]

    def route(action, _record, _changes, metadata) do
      struct(
        __MODULE__,
        %{
          subject: "sequin.changes.#{metadata.database_name}.#{metadata.table_schema}.#{metadata.table_name}.#{action}",
          headers: %{"Nats-Msg-Id" => metadata.idempotency_key}
        }
      )
    end

    def apply_consumer_config(routing_info, _consumer) do
      # Nats doesn't currently use consumer-specific configuration
      routing_info
    end

    def encode(message), do: Jason.encode_to_iodata!(message)
  end

  def route_message(%SinkConsumer{} = consumer, message) do
    [head | _tail] = route_messages(consumer, [message])
    head
  end

  def route_messages(%SinkConsumer{} = consumer, messages) do
    case routing_module(consumer.type) do
      nil ->
        raise "The Sink type #{consumer.type} does not yet support the new Routing mechanism"

      routing_module ->
        Enum.map(messages, fn message ->
          inner_route_message(routing_module, consumer, message)
        end)
    end
  end

  def route_and_encode_messages(%SinkConsumer{} = consumer, messages) do
    case routing_module(consumer.type) do
      nil ->
        raise "The Sink type #{consumer.type} does not yet support the new Routing mechanism"

      routing_module ->
        Enum.map(messages, fn message ->
          routing_info = inner_route_message(routing_module, consumer, message)
          payload = inner_encode_message(routing_module, message)
          %RoutedMessage{routing_info: routing_info, payload: payload}
        end)
    end
  end

  # Private functions

  defp merge_with_defaults(default_routing, user_routing) when is_struct(default_routing) and is_map(user_routing) do
    default_map = Map.from_struct(default_routing)

    # Only include user values that are not nil
    filtered_user_map = user_routing |> Enum.filter(fn {_key, value} -> not is_nil(value) end) |> Map.new()

    # TODO Only override if shape/type matches with defaults

    # Merge and convert back to struct
    merged_map = Map.merge(default_map, filtered_user_map)
    struct(default_routing.__struct__, merged_map)
  end

  defp routing_module(sink_type) do
    case sink_type do
      :http_push -> HttpPush
      :redis_string -> RedisString
      :nats -> Nats
      _ -> nil
    end
  end

  defp inner_route_message(routing_module, consumer, %Broadway.Message{data: event_message}) do
    inner_route_message(routing_module, consumer, event_message)
  end

  defp inner_route_message(routing_module, consumer, %ConsumerEvent{data: message}) do
    %{action: action, record: record, metadata: metadata, changes: changes} = message

    # TODO Memoize default_routing if possible
    default_routing = routing_module.route(action, record, changes, metadata)

    case consumer.routing do
      nil ->
        # Does not have a routing function, apply default routing with
        # consumer-configured settings (like http_endpoint_path)
        routing_module.apply_consumer_config(default_routing, consumer)

      routing ->
        # Has a routing function, calculate default routing and override with evaluated function

        # TODO Consider moving this check somewhere else
        # If function is not persisted (via function edit) run interpreted
        # If function is persisted (during runtime) run compiled
        user_routing =
          case consumer.routing.id do
            nil ->
              MiniElixir.run_interpreted(routing, message)

            _id ->
              MiniElixir.run_compiled(routing, message)
          end

        # Merge user routing with defaults, user values override defaults
        merge_with_defaults(default_routing, user_routing)
    end

    # TODO Validate that the default or merged result fits the matched structure and validation constraints
  end

  defp inner_encode_message(routing_module, %Broadway.Message{data: event_message}) do
    inner_encode_message(routing_module, event_message)
  end

  defp inner_encode_message(routing_module, %ConsumerEvent{data: message}) do
    routing_module.encode(message)
  end
end

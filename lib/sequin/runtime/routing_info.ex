defmodule Sequin.Runtime.RoutingInfo do
  @moduledoc """
  Defines routing info structs for different sink types.
  These structs are used to validate and type-check routing function responses.
  """

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

    # TODO Keep the callback spec specific with the module's struct
    @callback route(action :: atom(), record :: struct(), changes :: struct(), metadata :: struct()) :: struct()
    @callback encode(message :: struct()) :: any()
  end

  # defmodule HttpPush do
  #   @moduledoc false
  #   use RoutedSink

  #   defstruct method: "POST", endpoint_path: "/"

  #   # The default is just a struct with static defaults
  #   def route(_action, _record, _changes, _metadata) do
  #     struct(__MODULE__)
  #   end
  # end

  defmodule RedisString do
    @moduledoc false
    @behaviour RoutedSink

    defstruct [:key, :action]

    def route(action, _record, _changes, metadata) do
      table_name = metadata.table_name
      pks = Enum.join(metadata.record_pks, "-")

      # TODO Consider having this outside of routing
      redis_action =
        case action do
          :insert -> :set
          :update -> :set
          :delete -> :del
          :read -> :set
        end

      struct(__MODULE__, %{key: "sequin:#{table_name}:#{pks}", action: redis_action})
    end

    def encode(message) when is_binary(message) or is_number(message), do: message
    def encode(message), do: Jason.encode!(message)
  end

  defmodule Nats do
    @moduledoc false
    @behaviour RoutedSink

    defstruct [:subject, :headers]

    def route(action, _record, _changes, metadata) do
      struct(
        __MODULE__,
        %{
          subject: "sequin.changes.#{metadata.database_name}.#{metadata.table_schema}.#{metadata.table_name}.#{action}",
          headers: [{"Nats-Msg-Id", metadata.idempotency_key}]
        }
      )
    end

    def encode(message), do: Jason.encode_to_iodata!(message)
  end

  defp merge_with_defaults(default_routing, user_routing) when is_struct(default_routing) and is_map(user_routing) do
    default_map = Map.from_struct(default_routing)

    # Only include user values that are not nil
    filtered_user_map = user_routing |> Enum.filter(fn {_key, value} -> not is_nil(value) end) |> Map.new()

    # Merge and convert back to struct
    merged_map = Map.merge(default_map, filtered_user_map)
    struct(default_routing.__struct__, merged_map)
  end

  @doc """
  Returns the routing info struct module for a given sink type.
  """
  def routing_module(sink_type) do
    case sink_type do
      # :http_push -> HttpPush
      :redis_string -> RedisString
      :nats -> Nats
      _ -> nil
    end
  end

  @doc """
  Applies routing info validation for a given sink type.

  Takes a sink type atom and routing info map, validates the structure
  and returns the appropriate routing info struct.
  """
  def apply_routing(%SinkConsumer{} = consumer, message, routing_module) do
    %{action: action, record: record, metadata: metadata, changes: changes} = message.data

    case consumer.routing do
      nil ->
        routing_module.route(action, record, changes, metadata)

      routing ->
        # TODO Memoize default_routing if possible
        default_routing = routing_module.route(action, record, changes, metadata)
        user_routing = MiniElixir.run_compiled(routing, message.data)

        # Merge user routing with defaults, user values take precedence
        merge_with_defaults(default_routing, user_routing)
    end

    # TODO Validate that the result fits the matched structure and validation constraints
  end

  def encode_message(message, routing_module) do
    routing_module.encode(message)
  end

  def prepare_messages(%SinkConsumer{} = consumer, messages) do
    case routing_module(consumer.type) do
      nil ->
        raise "The Sink type #{consumer.type} does not yet support the new Routing mechanism"

      routing_module ->
        Enum.map(messages, fn %{data: message} ->
          routing_info = apply_routing(consumer, message, routing_module)
          external_message = Sequin.Transforms.Message.to_external(consumer, message)
          encoded_message = encode_message(external_message, routing_module)
          %RoutedMessage{routing_info: routing_info, payload: encoded_message}
        end)
    end
  end
end

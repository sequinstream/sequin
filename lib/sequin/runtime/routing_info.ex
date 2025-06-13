defmodule Sequin.Runtime.RoutingInfo do
  @moduledoc """
  Defines routing info structs for different sink types.
  These structs are used to validate and type-check routing function responses.
  """

  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord

  defmodule RoutedMessage do
    @moduledoc """
    Generic wrapper for routed messages that preserves the original ConsumerEvent/ConsumerRecord
    while adding routing information and transformed payload.
    """
    defstruct [:original_message, :routing_info, :payload]

    @type t :: %__MODULE__{
            original_message: ConsumerEvent.t() | ConsumerRecord.t(),
            routing_info: struct(),
            payload: any()
          }
  end

  defmodule HttpPush do
    @moduledoc false
    defstruct method: "POST", endpoint_path: "/"
  end

  defmodule RedisString do
    @moduledoc false
    defstruct key: ""
  end

  defmodule Nats do
    @moduledoc false
    defstruct subject: ""
  end

  @doc """
  Returns the routing info struct module for a given sink type.
  """
  def routing_info_module(sink_type) do
    case sink_type do
      :http_push -> HttpPush
      :redis_string -> RedisString
      :nats -> Nats
    end
  end

  @doc """
  Applies routing info validation for a given sink type.

  Takes a sink type atom and routing info map, validates the structure
  and returns the appropriate routing info struct.
  """
  def apply_routing(sink_type, rinfo) when is_map(rinfo) do
    routing_module = routing_info_module(sink_type)
    struct!(routing_module, rinfo)
  rescue
    KeyError ->
      routing_module = routing_info_module(sink_type)

      expected_keys =
        routing_module.__struct__()
        |> Map.keys()
        |> Enum.reject(&(&1 == :__struct__))
        |> Enum.join(", ")

      raise Sequin.Error.invariant(
              message:
                "Invalid routing response for #{sink_type}. Expected a map with keys: #{expected_keys}, got: #{inspect(rinfo)}"
            )
  end

  def apply_routing(sink_type, v) do
    raise "Routing function for #{sink_type} must return a map! Got: #{inspect(v)}"
  end
end

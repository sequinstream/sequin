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

    ## Schema Definition

    When using this module you must define a schema:

    ```elixir
    use RoutedSink, schema: [
      field1: [type: :string, required: true],
      field2: [type: :integer, default: 0]
    ]
    ```

    The schema definition supports:
    - `type`: Ecto field type (:string, :integer, :boolean, :map, etc.)
    - `required`: Whether the field is required (true/false)
    - `default`: Default value for the field

    Custom validations should be implemented in the changeset/2 function.
    """
    defmacro __using__(opts) do
      schema_definition = Keyword.get(opts, :schema)

      quote do
        @behaviour RoutedSink

        use Ecto.Schema
        use TypedEctoSchema

        import Ecto.Changeset

        @schema_definition unquote(schema_definition)

        @primary_key false
        typed_embedded_schema do
          unquote(build_schema_fields(schema_definition))
        end

        def changeset(struct, params) do
          struct
          |> cast(params, unquote(get_field_names(schema_definition)))
          # TODO Consider setting all fields as required without static defaults
          |> validate_required(unquote(get_required_fields(schema_definition)))
          |> apply_validations()
        end

        def apply_validations(changeset) do
          changeset
        end

        def route_with_consumer_config(routing_info, _consumer) do
          routing_info
        end

        defoverridable apply_validations: 1, route_with_consumer_config: 2

        def validate_attributes(attrs) when is_map(attrs) do
          %__MODULE__{}
          |> changeset(attrs)
          |> apply_action(:validate)
        end

        defimpl Jason.Encoder do
          def encode(message, opts) do
            message
            |> Map.from_struct()
            |> Jason.Encode.map(opts)
          end
        end
      end
    end

    @callback route(action :: atom(), record :: struct(), changes :: struct(), metadata :: struct()) :: struct()
    @callback encode(message :: struct()) :: any()
    @callback validate_attributes(attrs :: map()) :: {:ok, struct()} | {:error, term()}

    # Private helper functions for macro expansion

    defp build_schema_fields(schema_definition) do
      Enum.map(schema_definition, fn {field_name, field_opts} ->
        field_type = Keyword.get(field_opts, :type, :string)
        field_default = Keyword.get(field_opts, :default)

        if field_default do
          quote do: field(unquote(field_name), unquote(field_type), default: unquote(field_default))
        else
          quote do: field(unquote(field_name), unquote(field_type))
        end
      end)
    end

    defp get_field_names(schema_definition) do
      Enum.map(schema_definition, fn {field_name, _opts} -> field_name end)
    end

    defp get_required_fields(schema_definition) do
      schema_definition
      |> Enum.filter(fn {_field_name, field_opts} ->
        Keyword.get(field_opts, :required, false)
      end)
      |> Enum.map(fn {field_name, _opts} -> field_name end)
    end
  end

  defmodule HttpPush do
    @moduledoc false
    use RoutedSink,
      schema: [
        method: [
          type: :string,
          default: "POST",
          required: true
        ],
        endpoint_path: [
          type: :string,
          default: "/",
          required: true
        ],
        headers: [
          type: :map,
          default: %{},
          required: false
        ]
      ]

    def apply_validations(changeset) do
      changeset
      |> validate_inclusion(:method, ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"],
        message: "is invalid, valid values are: GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS"
      )
      |> validate_format(:endpoint_path, ~r/^\//, message: "must start with '/'")
      |> validate_length(:endpoint_path, max: 2000)
    end

    def route(_action, _record, _changes, metadata) do
      struct(__MODULE__, %{
        method: "POST",
        endpoint_path: "/",
        headers: %{"Idempotency-Key" => metadata.idempotency_key}
      })
    end

    def route_with_consumer_config(routing_info, %{sink: sink}) do
      # Apply consumer-configured settings like http_endpoint_path
      %{routing_info | endpoint_path: sink.http_endpoint_path || routing_info.endpoint_path}
    end

    def encode(message) do
      Jason.encode!(message)
    end
  end

  defmodule RedisString do
    @moduledoc false
    use RoutedSink,
      schema: [
        key: [
          type: :string,
          required: true
        ],
        action: [
          type: :string,
          required: true
        ]
      ]

    def apply_validations(changeset) do
      changeset
      |> validate_length(:key, min: 1, max: 512)
      |> validate_format(:key, ~r/^[^\s]+$/, message: "cannot contain whitespace")
      |> validate_inclusion(:action, ["set", "del", "get"], message: "is invalid, valid values are: set, del, get")
    end

    def route(action, _record, _changes, metadata) do
      table_name = metadata.table_name
      pks = Enum.join(metadata.record_pks, "-")

      redis_action =
        case action do
          :insert -> "set"
          :update -> "set"
          :delete -> "del"
          :read -> "set"
        end

      struct(__MODULE__, %{key: "sequin:#{table_name}:#{pks}", action: redis_action})
    end

    def encode(message) when is_binary(message) or is_number(message), do: message
    def encode(message), do: Jason.encode!(message)
  end

  defmodule Nats do
    @moduledoc false
    use RoutedSink,
      schema: [
        subject: [
          type: :string,
          required: true
        ],
        headers: [
          type: :map,
          default: %{},
          required: false
        ]
      ]

    def apply_validations(changeset) do
      changeset
      |> validate_length(:subject, min: 1, max: 255)
      |> validate_format(:subject, ~r/^[a-zA-Z0-9\.\-_]+$/,
        message: "must contain only alphanumeric characters, dots, hyphens, and underscores"
      )
    end

    def route(action, _record, _changes, metadata) do
      struct(
        __MODULE__,
        %{
          subject: "sequin.changes.#{metadata.database_name}.#{metadata.table_schema}.#{metadata.table_name}.#{action}",
          headers: %{"Nats-Msg-Id" => metadata.idempotency_key}
        }
      )
    end

    def encode(message), do: Jason.encode_to_iodata!(message)
  end

  def route_message(%SinkConsumer{} = consumer, message) do
    consumer |> route_messages([message]) |> List.first()
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

  defp unwrap_message(%Broadway.Message{data: message}), do: unwrap_message(message)
  defp unwrap_message(%ConsumerEvent{data: message}), do: unwrap_message(message)
  defp unwrap_message(message), do: message

  defp merge_with_defaults(default_routing, user_routing) when is_struct(default_routing) and is_map(user_routing) do
    default_map = Map.from_struct(default_routing)

    # Only include user values that are not nil
    filtered_user_map = user_routing |> Enum.filter(fn {_key, value} -> not is_nil(value) end) |> Map.new()

    # Merge and convert back to struct
    merged_map = Map.merge(default_map, filtered_user_map)

    # Validate the merged attributes
    routing_module = default_routing.__struct__

    case routing_module.validate_attributes(merged_map) do
      {:ok, validated_struct} ->
        validated_struct

      {:error, %Ecto.Changeset{} = changeset} ->
        errors =
          Ecto.Changeset.traverse_errors(changeset, fn {msg, opts} ->
            Enum.reduce(opts, msg, fn {key, value}, acc ->
              String.replace(acc, "%{#{key}}", to_string(value))
            end)
          end)

        raise "Invalid routing attributes for #{inspect(routing_module)}: #{inspect(errors)}"

      {:error, error} ->
        raise "Invalid routing attributes for #{inspect(routing_module)}: #{inspect(error)}"
    end
  end

  defp routing_module(sink_type) do
    case sink_type do
      :http_push -> HttpPush
      :redis_string -> RedisString
      :nats -> Nats
      _ -> nil
    end
  end

  defp inner_route_message(routing_module, consumer, message) do
    message = unwrap_message(message)
    %{action: action, record: record, metadata: metadata, changes: changes} = message

    # TODO Consider memoizing this call
    default_routing = routing_module.route(action, record, changes, metadata)

    case consumer.routing do
      nil ->
        # Does not have a routing function, apply default routing with
        # consumer-configured settings (like http_endpoint_path)
        routing_module.route_with_consumer_config(default_routing, consumer)

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
  end

  defp inner_encode_message(routing_module, message) do
    message
    |> unwrap_message()
    |> routing_module.encode
  end
end

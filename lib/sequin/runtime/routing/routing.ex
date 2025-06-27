defmodule Sequin.Runtime.Routing do
  @moduledoc """
  Defines routing info structs for different sink types.
  These structs are used to validate and type-check routing function responses.
  """

  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.Function
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Functions.MiniElixir
  alias Sequin.Runtime.Routing.Helpers
  alias Sequin.Runtime.Routing.RoutedMessage
  alias Sequin.Runtime.Trace

  @doc """
  Route a single message, returning the routing info struct
  """
  @spec route_message(SinkConsumer.t(), any()) :: struct()
  def route_message(%SinkConsumer{} = consumer, message) do
    consumer |> route_messages([message]) |> List.first()
  end

  @doc """
  Route a list of messages, returning a list of routing info structs
  """
  @spec route_messages(SinkConsumer.t(), [any()]) :: [struct()]
  def route_messages(%SinkConsumer{} = consumer, messages) do
    case get_routing_module(consumer.type) do
      nil ->
        raise "The Sink type #{consumer.type} does not yet support the new Routing mechanism"

      routing_module ->
        Enum.map(messages, fn message ->
          inner_route_message(routing_module, consumer, message)
        end)
    end
  end

  @doc """
  Route a list of messages, returning a list of RoutedMessage structs
  """
  @spec route_and_transform_messages(SinkConsumer.t(), [any()]) :: [RoutedMessage.t()]
  def route_and_transform_messages(%SinkConsumer{} = consumer, messages) do
    case get_routing_module(consumer.type) do
      nil ->
        raise "The Sink type #{consumer.type} does not yet support the new Routing mechanism"

      routing_module ->
        Enum.map(messages, fn message ->
          routing_info = inner_route_message(routing_module, consumer, message)
          # Extract the ConsumerEvent/ConsumerRecord from the Broadway.Message wrapper
          unwrapped = extract_consumer_message(message)
          transformed_message = Sequin.Transforms.Message.to_external(consumer, unwrapped)
          %RoutedMessage{routing_info: routing_info, transformed_message: transformed_message}
        end)
    end
  end

  # Private functions

  defp extract_consumer_message(%Broadway.Message{data: message}), do: message
  defp extract_consumer_message(message), do: message

  defp unwrap_message(%Broadway.Message{data: message}), do: unwrap_message(message)
  defp unwrap_message(%ConsumerRecord{data: message}), do: unwrap_message(message)
  defp unwrap_message(%ConsumerEvent{data: message}), do: unwrap_message(message)
  defp unwrap_message(message), do: message

  defp merge_with_defaults(default_routing, overriding_routing)
       when is_struct(default_routing) and is_map(overriding_routing) do
    case Helpers.merge_and_validate(default_routing, overriding_routing) do
      {:ok, validated_struct} ->
        validated_struct

      {:error, %Ecto.Changeset{} = changeset} ->
        routing_module = default_routing.__struct__
        errors = Sequin.Error.errors_on(changeset)

        raise "Invalid routing attributes for #{inspect(routing_module)}: #{inspect(errors)}"
    end
  end

  defp get_routing_module(sink_type) do
    case sink_type do
      :http_push -> Sequin.Runtime.Routing.Consumers.HttpPush
      :redis_string -> Sequin.Runtime.Routing.Consumers.RedisString
      :nats -> Sequin.Runtime.Routing.Consumers.Nats
      :kafka -> Sequin.Runtime.Routing.Consumers.Kafka
      :gcp_pubsub -> Sequin.Runtime.Routing.Consumers.GcpPubsub
      _ -> nil
    end
  end

  defp inner_route_message(routing_module, consumer, message) do
    message = unwrap_message(message)
    # Handle the case where changes might be missing from the message
    %{action: action, record: record, metadata: metadata} = message
    changes = Map.get(message, :changes, nil)

    # TODO Consider memoizing this call
    default_routing_map = routing_module.route(to_string(action), record, changes, Map.from_struct(metadata))

    # Convert the map returned by route() to a validated struct
    default_routing =
      case Helpers.validate_attributes(routing_module, default_routing_map) do
        {:ok, validated_struct} -> validated_struct
        {:error, error} -> raise "Invalid routing from route/4: #{inspect(error)}"
      end

    case consumer.routing do
      nil ->
        # Consumer does not have a routing function, apply default routing with
        # consumer-configured settings (like http_endpoint_path)
        consumer_routing = routing_module.route_consumer(consumer)
        merge_with_defaults(default_routing, consumer_routing)

      routing ->
        # Consumer has a routing function, override with consumer settings, and override that with results of the evaluated routing function
        overriding_routing = run_routing_function(routing, message)

        Trace.info(consumer.id, %Trace.Event{
          message: "Executed routing function #{routing.name}",
          extra: %{input: message, output: overriding_routing}
        })

        # Merge user routing with defaults, user values override defaults
        merge_with_defaults(default_routing, overriding_routing)
    end
  end

  defp run_routing_function(%Function{id: nil} = function, message) do
    # If function is not persisted (via function edit) run interpreted
    MiniElixir.run_interpreted(function, message)
  end

  defp run_routing_function(%Function{} = function, message) do
    # If function is persisted (during runtime) run compiled
    MiniElixir.run_compiled(function, message)
  rescue
    ex ->
      raise Sequin.Error.service(
              service: :routing,
              message: "Routing function failed with the following error: #{Exception.message(ex)}"
            )
  end
end

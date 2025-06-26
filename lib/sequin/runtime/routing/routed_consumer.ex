defmodule Sequin.Runtime.Routing.RoutedConsumer do
  @moduledoc """
  Behavior for routing sink modules with required callbacks.

  This behavior defines the contract that all routing modules must implement.
  It provides minimal boilerplate while enforcing required functions and
  maintaining explicit schema definitions.

  ## Usage

      defmodule MyRoutingModule do
        use Sequin.Runtime.Routing.RoutedConsumer

        @primary_key false
        typed_embedded_schema do
          field :my_field, :string
        end

        def changeset(struct, params) do
          # Custom changeset implementation
        end

        def route(action, record, changes, metadata) do
          # Custom routing logic
        end

        def route_consumer(routing_info, %{sink: sink}) do
          # Custom routing overriding via sink attributes
        end
      end

  ## Required Callbacks

  - `changeset/2` - Must validate and cast parameters into a changeset
  - `route/4` - Must implement default routing logic and return a struct

  ## Overridable Callbacks

  - `route_with_consumer_config/2` - Override consumer-specific configuration
  """

  @doc """
  Route a message based on action, record, changes, and metadata.
  Must return a struct of the implementing module's type.

  ## Parameters

  - `action` - The database action (:insert, :update, :delete, :read)
  - `record` - The database record struct
  - `changes` - The changes struct (for updates)
  - `metadata` - Metadata struct containing routing context

  ## Returns

  A struct of the implementing module's type with routing information.
  """
  @callback route(action :: atom(), record :: struct(), changes :: struct(), metadata :: struct()) :: map()

  @doc """
  Apply consumer-specific configuration to routing info.
  Optional callback with default no-op implementation.

  This is called after the base routing logic to allow consumer-specific
  overrides like endpoint paths, connection settings, etc.

  ## Parameters

  - `consumer` - The consumer struct with sink configuration

  ## Returns

  A struct of the implementing module's type with routing information.
  """
  @callback route_consumer(Sequin.Consumers.SinkConsumer.t()) :: struct()

  @doc """
  Create and validate a changeset for the given struct and params.
  Must be implemented by each module for proper validation.

  ## Parameters

  - `struct` - The struct to validate (usually %Module{})
  - `params` - Map of parameters to cast and validate

  ## Returns

  An Ecto.Changeset with validation results.
  """
  @callback changeset(struct(), map()) :: Ecto.Changeset.t()

  @doc """
  Minimal macro that provides common functionality while keeping schemas explicit.
  Schema definition must be done outside of this macro.
  """
  defmacro __using__(_opts) do
    quote do
      @behaviour Sequin.Runtime.Routing.RoutedConsumer

      use Ecto.Schema
      use TypedEctoSchema

      import Ecto.Changeset

      alias Sequin.Runtime.Routing

      @after_compile Sequin.Runtime.Routing.Validator

      @doc """
      Default route_consumer implementation (no-op).
      Override this function to apply consumer-specific routing configuration.
      """
      def route_consumer(%Sequin.Consumers.SinkConsumer{}) do
        %{}
      end

      # Allow overriding of default implementations
      defoverridable route_consumer: 1
    end
  end
end

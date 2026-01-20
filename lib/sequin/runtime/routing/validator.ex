defmodule Sequin.Runtime.Routing.Validator do
  @moduledoc """
  Compile-time validation for routing modules.

  This module provides validation functions that can be used at compile time
  to ensure routing modules properly implement the RoutedConsumer behavior.

  Replaces current warning-level notices, for error-level notices that block compilation.

  Comes included as part of `use Sequin.Runtime.Routing.RoutedConsumer`

  """

  @doc """
  Validate that a module properly implements the RoutedConsumer behavior.
  This function is designed to be used in a `@before_compile` hook.

  ## Validations Performed

  - Checks that required functions are defined
  - Validates that the module defines a struct

  ## Parameters

  - `env` - The compilation environment containing module information

  ## Raises

  `CompileError` if the module doesn't meet the requirements.
  """
  defmacro __before_compile__(env) do
    Sequin.Runtime.Routing.Validator.validate_module_definitions(env.module)

    quote do
    end
  end

  def validate_module_definitions(module) do
    definitions = Module.definitions_in(module)

    validate_required_functions(definitions, module)
    validate_struct_definition(definitions, module)
    :ok
  end

  # Private validation functions

  defp validate_required_functions(definitions, module) do
    required_functions = Sequin.Runtime.Routing.RoutedConsumer.behaviour_info(:callbacks)

    missing_functions =
      Enum.filter(required_functions, fn {name, arity} ->
        not Enum.member?(definitions, {name, arity})
      end)

    if missing_functions != [] do
      raise CompileError,
        description: """
        Module #{module} is missing required functions for RoutedConsumer behavior:
        #{format_function_list(missing_functions)}

        Required functions:
        #{format_function_list(required_functions)}
        """
    end
  end

  defp validate_struct_definition(definitions, module) do
    if not (Enum.member?(definitions, {:__struct__, 0}) or Enum.member?(definitions, {:__struct__, 1})) do
      raise CompileError,
        description: """
        Module #{module} must define a struct with the routing parameters.

        Add a schema definition like:

        @primary_key false
        typed_embedded_schema do
          field :my_field, :string
        end
        """
    end
  end

  defp format_function_list(functions) do
    Enum.map_join(functions, "\n", fn {name, arity} -> "  - #{name}/#{arity}" end)
  end
end

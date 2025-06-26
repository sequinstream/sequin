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
  This function is designed to be used in an `@after_compile` hook.

  ## Validations Performed

  - Checks that required functions are exported
  - Validates that the module defines a struct

  ## Parameters

  - `env` - The compilation environment containing module information
  - `_bytecode` - The compiled bytecode (unused)

  ## Raises

  `CompileError` if the module doesn't meet the requirements.
  """
  def __after_compile__(env, _bytecode) do
    validate_module(env.module)
  end

  defp validate_module(module) do
    validate_required_functions(module)
    validate_struct_definition(module)
    :ok
  end

  # Private validation functions

  defp validate_required_functions(module) do
    required_functions = Sequin.Runtime.Routing.RoutedConsumer.behaviour_info(:callbacks)

    missing_functions =
      Enum.filter(required_functions, fn {name, arity} ->
        not function_exported?(module, name, arity)
      end)

    unless missing_functions == [] do
      raise CompileError,
        description: """
        Module #{module} is missing required functions for RoutedConsumer behavior:
        #{format_function_list(missing_functions)}

        Required functions:
        #{format_function_list(required_functions)}
        """
    end
  end

  defp validate_struct_definition(module) do
    unless function_exported?(module, :__struct__, 0) do
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

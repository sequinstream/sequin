defmodule Sequin.GenerateBehaviour do
  @moduledoc """
  Turns the current module into a behaviour based on the specs of publicly
  defined functions.

  This is useful in cases where we'd like to use mox, but only have one
  implementation of a given interface and would rather not maintain the
  additional behaviour module.
  """

  defmacro __using__(_opts) do
    quote do
      @before_compile unquote(__MODULE__)
    end
  end

  defmacro __before_compile__(env) do
    env.module
    |> Module.definitions_in(:def)
    |> Enum.map(&Module.spec_to_callback(env.module, &1))
  end
end

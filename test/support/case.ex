defmodule Sequin.Case do
  @moduledoc """
  This module defines shared infrastructure for all of our tests.

  It should only import things that we want available for every test case.
  """

  use ExUnit.CaseTemplate

  using do
    quote do
      import Sequin.Test.Assertions
      import unquote(__MODULE__)

      setup :start_supervisors
    end
  end

  def start_supervisors(context) do
    sup_tree =
      context
      |> Map.get(:start_supervisors, [])
      |> Map.new(fn supervisor ->
        name = Module.concat([context.module, supervisor])
        start_supervised!({supervisor, name: name, id: name})
        {supervisor, name}
      end)

    {:ok, supervisors: sup_tree}
  end
end

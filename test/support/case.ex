defmodule Sequin.Case do
  @moduledoc """
  This module defines shared infrastructure for all of our tests.

  It should only import things that we want available for every test case.
  """

  use ExUnit.CaseTemplate

  using do
    quote do
      import Hammox
      import Sequin.Test.Assertions
      import unquote(__MODULE__)

      setup :start_supervisors
      setup :verify_stubs
      # For Hammox
      # Note: You may be tempted to move this to Hammox.verify_on_exit!() in
      # verify_stubs/1, but that doesn't seem to have an effect.
      setup :verify_on_exit!
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

  def verify_stubs(_context) do
    Req.Test.verify_on_exit!()
    :ok
  end

  def errors_on(changeset) do
    Sequin.Error.errors_on(changeset)
  end

  def flash_text(conn, kind) do
    if toast = Phoenix.Flash.get(conn.assigns.flash, :toast) do
      if toast.kind == kind do
        Enum.join([Map.get(toast, :title), Map.get(toast, :description)], "\n")
      end
    else
      Phoenix.Flash.get(conn.assigns.flash, kind)
    end
  end
end

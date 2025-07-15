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
      import Sequin.TestSupport
      import unquote(__MODULE__)

      setup :start_supervisors
      setup :verify_stubs
      setup :default_stubs
      setup :setup_self_hosted_tag
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

  def default_stubs(_context) do
    Sequin.TestSupport.stub_utc_now(fn -> DateTime.utc_now() end)
    Sequin.TestSupport.stub_uuid4(fn -> UUID.uuid4() end)
    Sequin.TestSupport.stub_random(fn n -> Enum.random(n) end)
    Sequin.TestSupport.stub_process_alive?(fn pid -> Process.alive?(pid) end)
    Sequin.TestSupport.stub_application_get_env(fn atom, mod -> Application.get_env(atom, mod) end)

    :ok
  end

  def verify_stubs(_context) do
    # TODO Update Req to the latest version once this is released:
    # https://github.com/wojtekmach/req/commit/dcb7ddf6a449dfd2cc2a99c6354d050b65e5191a
    Req.Test.verify_on_exit!()
    Hammox.verify_on_exit!()
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

  def setup_self_hosted_tag(context) do
    self_hosted = Map.get(context, :self_hosted, true)
    Mox.stub(Sequin.ConfigMock, :self_hosted?, fn -> self_hosted end)
    :ok
  end
end

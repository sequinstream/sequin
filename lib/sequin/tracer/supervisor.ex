defmodule Sequin.Tracer.Supervisor do
  @moduledoc false
  use DynamicSupervisor

  alias Sequin.Tracer.Server

  def start_link(init_arg) do
    DynamicSupervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(_init_arg) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  @doc """
  Starts a Tracer.Server for the given account_id.
  """
  def start_for_account(account_id) do
    child_spec = Server.child_spec(account_id)
    DynamicSupervisor.start_child(__MODULE__, child_spec)
  end
end

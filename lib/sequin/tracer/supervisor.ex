defmodule Sequin.Tracer.DynamicSupervisor do
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

    case DynamicSupervisor.start_child(__MODULE__, child_spec) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      {:error, reason} -> {:error, reason}
    end
  end
end

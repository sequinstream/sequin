defmodule Sequin.DynamicSupervisor do
  @moduledoc false
  use DynamicSupervisor

  def start_link(init_args) do
    {name, init_args} = Keyword.pop!(init_args, :name)
    DynamicSupervisor.start_link(__MODULE__, init_args, name: via_tuple(name))
  end

  def child_spec(opts) do
    name = Keyword.fetch!(opts, :name)

    %{
      id: via_tuple(name),
      start: {__MODULE__, :start_link, [opts]}
    }
  end

  def via_tuple(name) do
    {:via, Registry, {Sequin.Registry, name}}
  end

  @impl DynamicSupervisor
  def init(init_arg) do
    strategy = Keyword.get(init_arg, :strategy, :one_for_one)
    DynamicSupervisor.init(strategy: strategy)
  end

  def start_child(supervisor \\ __MODULE__, child_spec) do
    DynamicSupervisor.start_child(supervisor, child_spec)
  end

  def stop_child(supervisor \\ __MODULE__, child_id) do
    case GenServer.whereis(child_id) do
      nil -> :ok
      pid -> DynamicSupervisor.terminate_child(supervisor, pid)
    end
  end
end

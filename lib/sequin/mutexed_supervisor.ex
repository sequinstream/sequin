defmodule Sequin.MutexedSupervisor do
  @moduledoc """
  Ensures only one instance of children are running at a time.

  In this pattern, the MutexOwner boots and tries to acquire a mutex. When it does, it calls the
  `on_acquired` function. This function will boot up children underneath the ChildrenSupervisor.

  If the MutexOwner loses its mutex, it will shutdown. Because we use one_for_all, this will also
  take down the ChildrenSupervisor, as desired.
  """

  use Supervisor

  alias Sequin.MutexOwner

  defmodule ChildrenSupervisor do
    @moduledoc """
    After the mutex is acquired, boots the Scheduler.Worker and SyncSupervisor.
    """
    use DynamicSupervisor

    def start_link(opts) do
      name = Keyword.fetch!(opts, :name)
      DynamicSupervisor.start_link(__MODULE__, nil, name: name)
    end

    @impl DynamicSupervisor
    def init(_args) do
      DynamicSupervisor.init(strategy: :one_for_one)
    end
  end

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @impl Supervisor
  def init(opts) do
    Supervisor.init(children(opts), strategy: :one_for_all)
  end

  defp children(opts) do
    name = Keyword.fetch!(opts, :name)
    child_specs = Keyword.fetch!(opts, :child_specs)
    child_supervisor = Module.concat(name, ChildrenSupervisor)
    mutex_owner = Module.concat(name, MutexOwner)

    [
      {ChildrenSupervisor, name: child_supervisor},
      {MutexOwner,
       on_acquired: fn -> start_supervisors(child_specs, child_supervisor) end,
       mutex_key: mutex_key(name),
       name: mutex_owner}
    ]
  end

  def child_spec(id, children) do
    Supervisor.child_spec({Ix.MutexedSupervisor, name: id, child_specs: children}, id: id)
  end

  defp start_supervisors(child_specs, child_supervisor) do
    Enum.each(child_specs, fn child_spec -> DynamicSupervisor.start_child(child_supervisor, child_spec) end)
  end

  defp mutex_key(name) do
    "sequin:mutexed_supervisor:#{env()}:#{name}"
  end

  defp env do
    Application.get_env(:sequin, :env)
  end
end

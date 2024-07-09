defmodule Sequin.Test.Support.CreateReplicationSlot do
  @moduledoc """
  GenServer with the sole purpose of creating a replication slot. Used in test.
  """
  use Postgrex.ReplicationConnection, restart: :temporary

  def start_link(opts) do
    {slot_name, opts} = Keyword.pop!(opts, :slot_name)
    {on_finish, opts} = Keyword.pop(opts, :on_finish, fn _ -> :ok end)
    args = [slot_name: slot_name, on_finish: on_finish]
    opts = Keyword.put(opts, :sync_connect, false)
    Postgrex.ReplicationConnection.start_link(__MODULE__, args, opts)
  end

  def create_replication_slot(slot_name, conn_opts) do
    pid = self()

    on_finish = fn -> send(pid, :done) end

    args = [slot_name: slot_name, on_finish: on_finish]
    opts = args ++ conn_opts
    # `CreateReplicationSlot` uses the Postgrex.ReplicationConnection to create the replication
    # slot. It does not seem we can create a replication slot with a "normal" Postgrex connection.
    {:ok, _pid} = start_link(opts)

    receive do
      :done -> :ok
    after
      5_000 -> raise "Replication slot creation timed out"
    end
  end

  @impl Postgrex.ReplicationConnection
  def init(args) do
    {:ok, %{step: :disconnected, slot_name: args[:slot_name], on_finish: args[:on_finish]}}
  end

  @impl Postgrex.ReplicationConnection
  def handle_connect(state) do
    query = "CREATE_REPLICATION_SLOT #{state.slot_name} LOGICAL pgoutput NOEXPORT_SNAPSHOT"

    {:query, query, %{state | step: :create_slot}}
  end

  @impl Postgrex.ReplicationConnection
  def handle_result(results, %{step: :create_slot} = state) when is_list(results) do
    state.on_finish.()
    pid = self()

    Task.Supervisor.async_nolink(Sequin.TaskSupervisor, fn -> GenServer.stop(pid) end)

    {:noreply, state}
  end

  @impl Postgrex.ReplicationConnection
  def handle_result(%Postgrex.Error{postgres: %{code: :duplicate_object}}, %{step: :create_slot} = state) do
    state.on_finish.()
    pid = self()

    Task.Supervisor.async_nolink(Sequin.TaskSupervisor, fn -> GenServer.stop(pid) end)

    {:noreply, state}
  end
end

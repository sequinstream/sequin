defmodule Sequin.ReplicationRuntime.Starter do
  @moduledoc """
  Starts processes under ReplicationRuntime.Supervisor when Sequin starts and then at regular intervals.
  """
  use GenServer

  alias Sequin.Replication
  alias Sequin.ReplicationRuntime.Supervisor

  require Logger

  @impl GenServer
  def init(_) do
    Logger.info("[ReplicationRuntimeStarter] Booting")

    schedule_start(:timer.seconds(5))

    {:ok, :ignore}
  end

  def start_link(_) do
    GenServer.start_link(__MODULE__, :ignore)
  end

  @impl GenServer
  def handle_info(:start, :ignore) do
    start()
    logger_info("[ReplicationRuntimeStarter] Booted source-related workers.")

    schedule_start()

    {:noreply, :ignore}
  end

  defp schedule_start(timeout \\ :timer.seconds(30)) do
    Process.send_after(self(), :start, timeout)
  end

  defp start do
    Enum.each(Replication.all_active_pg_replications(), fn pg_replication ->
      Supervisor.start_replication(pg_replication.id)
      Supervisor.start_wal_pipeline_servers(pg_replication)
    end)
  end

  defp logger_info(msg) do
    if Application.get_env(:sequin, :env) == :prod do
      Logger.info(msg)
    end
  end
end

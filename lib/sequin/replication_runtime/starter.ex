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
    logger_info("[ReplicationRuntimeStarter] Starting source-related workers...")

    start()

    schedule_start()

    {:noreply, :ignore}
  end

  defp schedule_start(timeout \\ :timer.seconds(30)) do
    Process.send_after(self(), :start, timeout)
  end

  defp start do
    Enum.each(Replication.all_active_pg_replications(), &Supervisor.start_replication(&1.id))
  end

  defp logger_info(msg) do
    if Application.get_env(:sequin, :env) == :prod do
      Logger.info(msg)
    end
  end
end

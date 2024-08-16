defmodule Sequin.StreamsRuntime.Starter do
  @moduledoc """
  Starts processes under StreamsRuntime.Supervisor when Sequin starts and then at regular intervals.
  """
  use GenServer

  alias Sequin.Streams
  alias Sequin.StreamsRuntime.Supervisor

  require Logger

  @impl GenServer
  def init(_) do
    Logger.info("[StreamsRuntimeStarter] Booting")

    schedule_start(:timer.seconds(5))

    {:ok, :ignore}
  end

  def start_link(_) do
    GenServer.start_link(__MODULE__, :ignore)
  end

  @impl GenServer
  def handle_info(:start, :ignore) do
    logger_info("[StreamsRuntimeStarter] Starting stream-related workers...")

    start()

    schedule_start()

    {:noreply, :ignore}
  end

  defp schedule_start(timeout \\ :timer.seconds(60)) do
    Process.send_after(self(), :start, timeout)
  end

  defp start do
    Enum.each(Streams.list_active_push_consumers(), &Supervisor.start_for_push_consumer(&1.id))
  end

  defp logger_info(msg) do
    if Application.get_env(:sequin, :env) == :prod do
      Logger.info(msg)
    end
  end
end

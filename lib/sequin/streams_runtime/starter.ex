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
    Logger.info("[StreamSupervisorStarter] Booting")

    schedule_start(:timer.seconds(5))

    {:ok, :ignore}
  end

  def start_link(_) do
    GenServer.start_link(__MODULE__, :ignore)
  end

  @impl GenServer
  def handle_info(:start, :ignore) do
    logger_info("[StreamSupervisorStarter] Starting stream-related workers...")

    start()

    schedule_start()

    {:noreply, :ignore}
  end

  defp schedule_start(timeout \\ :timer.seconds(60)) do
    Process.send_after(self(), :start, timeout)
  end

  defp start do
    Enum.each(Streams.all_streams(), &Supervisor.start_for_stream(&1.id))
    Enum.each(Streams.all_consumers(), &Supervisor.start_for_consumer(&1.id))
  end

  defp logger_info(msg) do
    if Application.get_env(:sequin, :env) == :prod do
      Logger.info(msg)
    end
  end
end

defmodule Sequin.Streams.Supervisors.ConsumerSupervisorStarter do
  @moduledoc """
  Starts processes under ConsumerSupervisor when Sequin starts and then at regular intervals.
  """
  use GenServer

  alias Sequin.Streams
  alias Sequin.Streams.Supervisors.ConsumerSupervisor

  require Logger

  @impl GenServer
  def init(_) do
    Logger.info("[ConsumerSupervisorStarter] Booting")

    schedule_start_consumers(:timer.seconds(5))

    {:ok, :ignore}
  end

  def start_link(_) do
    GenServer.start_link(__MODULE__, :ignore)
  end

  @impl GenServer
  def handle_info(:start_consumers, :ignore) do
    logger_info("[ConsumerSupervisorStarter] Starting consumers...")

    start_consumers()

    schedule_start_consumers()

    {:noreply, :ignore}
  end

  defp schedule_start_consumers(timeout \\ :timer.seconds(60)) do
    Process.send_after(self(), :start_consumers, timeout)
  end

  defp start_consumers do
    Enum.each(Streams.all_consumers(), &ConsumerSupervisor.start_for_consumer(&1.id))
  end

  defp logger_info(msg) do
    if Application.get_env(:sequin, :env) == :prod do
      Logger.info(msg)
    end
  end
end

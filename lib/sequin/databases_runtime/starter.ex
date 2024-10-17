defmodule Sequin.DatabasesRuntime.Starter do
  @moduledoc """
  Starts processes under DatabasesRuntime.Supervisor when Sequin starts and then at regular intervals.
  """
  use GenServer

  alias Sequin.Consumers
  alias Sequin.DatabasesRuntime.Supervisor

  require Logger

  @impl GenServer
  def init(_) do
    Logger.info("[DatabasesRuntimeStarter] Booting")

    schedule_start(:timer.seconds(1))

    {:ok, :ignore}
  end

  def start_link(_) do
    GenServer.start_link(__MODULE__, :ignore)
  end

  @impl GenServer
  def handle_info(:start, :ignore) do
    start()
    logger_info("[DatabasesRuntimeStarter] Booted table producer workers.")

    schedule_start()

    {:noreply, :ignore}
  end

  defp schedule_start(timeout \\ :timer.seconds(60)) do
    Process.send_after(self(), :start, timeout)
  end

  defp start do
    Enum.each(Consumers.list_consumers_where_table_producer(), fn consumer ->
      Supervisor.start_table_producer(consumer)
    end)
  end

  defp logger_info(msg) do
    if Application.get_env(:sequin, :env) == :prod do
      Logger.info(msg)
    end
  end
end

defmodule Sequin.ConsumersRuntime.Starter do
  @moduledoc """
  Starts processes under ConsumersRuntime.Supervisor when Sequin starts and then at regular intervals.
  """
  use GenServer

  alias Sequin.Consumers
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.ConsumersRuntime.Supervisor

  require Logger

  @impl GenServer
  def init(_) do
    Logger.info("[ConsumersRuntimeStarter] Booting")

    schedule_start(10)

    {:ok, :ignore}
  end

  def start_link(_) do
    GenServer.start_link(__MODULE__, :ignore, name: __MODULE__)
  end

  @impl GenServer
  def handle_info(:start, :ignore) do
    start()
    logger_info("[ConsumersRuntimeStarter] Booted consumer-related workers")

    schedule_start()

    {:noreply, :ignore}
  end

  defp schedule_start(timeout \\ :timer.seconds(20)) do
    Process.send_after(self(), :start, timeout)
  end

  def start do
    Enum.each(Consumers.list_active_sink_consumers([:sequence, :account]), &start(&1))
  end

  def start(%SinkConsumer{} = consumer) do
    Supervisor.start_for_sink_consumer(consumer)
    Logger.info("[ConsumersRuntimeStarter] Started consumer", consumer_id: consumer.id)
  catch
    :exit, e ->
      Logger.error("[ConsumersRuntimeStarter] Failed to start consumer", error: e, consumer_id: consumer.id)
      :ok
  end

  defp logger_info(msg) do
    if Application.get_env(:sequin, :env) == :prod do
      Logger.info(msg)
    end
  end
end

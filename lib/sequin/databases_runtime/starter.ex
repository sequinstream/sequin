defmodule Sequin.DatabasesRuntime.Starter do
  @moduledoc """
  Starts processes under DatabasesRuntime.Supervisor when Sequin starts and then at regular intervals.
  """
  use GenServer

  alias Sequin.Consumers
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.DatabasesRuntime.Supervisor
  alias Sequin.Replication
  alias Sequin.Replication.PostgresReplicationSlot

  require Logger

  @impl GenServer
  def init(_) do
    Logger.info("[DatabasesRuntimeStarter] Booting")

    schedule_start(:timer.seconds(1))

    {:ok, :ignore}
  end

  def start_link(_) do
    GenServer.start_link(__MODULE__, :ignore, name: __MODULE__)
  end

  @impl GenServer
  def handle_info(:start, :ignore) do
    start()
    logger_info("[DatabasesRuntimeStarter] Booted table producer workers.")

    schedule_start()

    {:noreply, :ignore}
  end

  defp schedule_start(timeout \\ :timer.seconds(20)) do
    Process.send_after(self(), :start, timeout)
  end

  def start do
    Logger.info("[DatabasesRuntimeStarter] Starting")
    Enum.each(Replication.all_active_pg_replications(), &start/1)
    Enum.each(Consumers.list_sink_consumers_with_active_backfill(), &start/1)
    Logger.info("[DatabasesRuntimeStarter] Finished starting")
  end

  defp start(%PostgresReplicationSlot{} = pg_replication) do
    Logger.info("[DatabasesRuntimeStarter] Starting replication", replication_id: pg_replication.id)
    Supervisor.start_replication(pg_replication)
    Logger.info("[DatabasesRuntimeStarter] Started replication", replication_id: pg_replication.id)
    Supervisor.start_wal_pipeline_servers(pg_replication)
  catch
    :exit, error ->
      Logger.error("[DatabasesRuntimeStarter] Failed to start replication",
        error: error,
        replication_id: pg_replication.id
      )
  end

  defp start(%SinkConsumer{} = consumer) do
    Logger.info("[DatabasesRuntimeStarter] Starting table reader", consumer_id: consumer.id)
    Supervisor.start_table_reader(consumer)
    Logger.info("[DatabasesRuntimeStarter] Started table reader", consumer_id: consumer.id)
  catch
    :exit, error ->
      Logger.error("[DatabasesRuntimeStarter] Failed to start table reader", error: error, consumer_id: consumer.id)
  end

  defp logger_info(msg) do
    if Application.get_env(:sequin, :env) == :prod do
      Logger.info(msg)
    end
  end
end

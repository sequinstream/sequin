defmodule Sequin.Runtime.Starter do
  @moduledoc """
  Starts processes under Runtime.Supervisor when Sequin starts and then at regular intervals.
  """
  use GenServer

  alias Sequin.Consumers
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Replication
  alias Sequin.Replication.PostgresReplicationSlot
  alias Sequin.Runtime.Supervisor

  require Logger

  @impl GenServer
  def init(_) do
    Logger.info("[RuntimeStarter] Booting")

    schedule_start(:timer.seconds(1))

    {:ok, :ignore}
  end

  def start_link(_) do
    GenServer.start_link(__MODULE__, :ignore, name: __MODULE__)
  end

  @impl GenServer
  def handle_info(:start, :ignore) do
    start()
    logger_info("[RuntimeStarter] Booted table producer workers.")

    schedule_start()

    {:noreply, :ignore}
  end

  defp schedule_start(timeout \\ :timer.seconds(20)) do
    Process.send_after(self(), :start, timeout)
  end

  def start do
    Logger.info("[RuntimeStarter] Starting")

    (Replication.all_active_pg_replications() ++ Consumers.list_sink_consumers_with_active_backfill())
    |> Task.async_stream(&start/1, timeout: :timer.seconds(30))
    |> Stream.run()

    Logger.info("[RuntimeStarter] Finished starting")
  end

  defp start(%PostgresReplicationSlot{} = pg_replication) do
    Supervisor.start_replication(pg_replication)
    Supervisor.start_wal_pipeline_servers(pg_replication)
  catch
    :exit, error ->
      Logger.error("[RuntimeStarter] Failed to start replication",
        error: error,
        replication_id: pg_replication.id
      )
  end

  defp start(%SinkConsumer{} = consumer) do
    Supervisor.maybe_start_table_readers(consumer)
  catch
    :exit, error ->
      Logger.error("[RuntimeStarter] Failed to start table reader", error: error, consumer_id: consumer.id)
  end

  defp logger_info(msg) do
    if Application.get_env(:sequin, :env) == :prod do
      Logger.info(msg)
    end
  end
end

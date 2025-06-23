defmodule Sequin.Runtime.WalSenderMonitor do
  @moduledoc """
  Monitors PostgreSQL WAL (Write-Ahead Log) sender activity for configured replication slots and exposes metrics via Prometheus.

  This GenServer continuously monitors the state of WAL sender processes in PostgreSQL by
  querying `pg_stat_activity` for processes with `backend_type = 'walsender'` that match
  specific replication slot names. WAL senders are responsible for streaming WAL data to
  replicas and logical replication subscribers.

  ## Metrics

  Exposes a Prometheus gauge `sequin_replication_wal_sender_state` with labels:
  - `database_id`: The ID of the database being monitored
  - `replication_slot_id`: The ID of the replication slot
  - `wait_event_type`: The type of wait event the WAL sender is experiencing
  - `wait_event`: The specific wait event name
  - `pid`: The process ID of the WAL sender

  The gauge value is:
  - `1` when the WAL sender is in that specific wait state
  - `0` when the WAL sender is not in that wait state

  ## State Management

  Uses `previous_states` (a map of MapSets keyed by replication slot ID) to track previously
  active states and reset them to 0 before setting the current state to 1. This ensures only
  one state combination is "high" at any given moment per replication slot, providing a clear
  picture of the current WAL sender activity.

  ## Configuration

  Configure with a list of replication slot IDs to monitor:
  ```elixir
  config :sequin, Sequin.Runtime.WalSenderMonitor,
    enabled?: true,
    replication_ids: ["slot-1", "slot-2"]
  ```

  ## Monitoring Interval

  Queries the WAL sender status every 1000ms (1 second) by default, but adjusts the next
  poll interval based on how long the current poll takes to maintain consistent timing.
  """
  use GenServer, restart: :temporary
  use Prometheus.Metric

  alias Sequin.Postgres
  alias Sequin.Replication

  require Logger

  @gauge_name :sequin_replication_wal_sender_state
  @target_interval 1000

  defstruct [:previous_states, :replications]

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def child_spec do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, []},
      type: :worker,
      restart: :temporary
    }
  end

  def enabled? do
    config = Application.get_env(:sequin, __MODULE__, [])
    Keyword.get(config, :enabled?, false)
  end

  @impl GenServer
  def init(_opts) do
    Process.send_after(self(), :start, 0)

    {:ok, %__MODULE__{previous_states: %{}, replications: []}}
  end

  @impl GenServer
  def handle_info(:start, state) do
    setup_prometheus()
    replications = load_configured_replications()
    schedule_monitor(0)

    {:noreply, %{state | replications: replications}}
  end

  @impl GenServer
  def handle_info(:monitor, state) do
    start_time = System.monotonic_time(:millisecond)

    new_previous_states = monitor_all_wal_senders(state.replications, state.previous_states)

    end_time = System.monotonic_time(:millisecond)
    duration = end_time - start_time

    next_interval = max(0, @target_interval - duration)
    schedule_monitor(next_interval)

    {:noreply, %{state | previous_states: new_previous_states}}
  end

  defp setup_prometheus do
    Gauge.new(
      name: @gauge_name,
      help: "WAL sender state (1 = active state, 0 = inactive state)",
      labels: [:database_id, :replication_slot_id, :wait_event_type, :wait_event, :pid]
    )
  end

  defp schedule_monitor(delay) do
    Process.send_after(self(), :monitor, delay)
  end

  defp load_configured_replications do
    :sequin
    |> Application.fetch_env!(__MODULE__)
    |> Keyword.fetch!(:replication_ids)
    |> Enum.map(&get_replication_with_db/1)
    |> Enum.reject(&is_nil/1)
  end

  defp get_replication_with_db(replication_id) do
    case Replication.get_pg_replication(replication_id) do
      {:ok, replication} ->
        Sequin.Repo.preload(replication, :postgres_database)

      {:error, _} ->
        Logger.warning("Replication slot #{replication_id} not found for WAL sender monitoring")
        nil
    end
  end

  defp monitor_all_wal_senders(replications, previous_states) do
    acc =
      Enum.reduce(replications, previous_states, fn replication, acc_states ->
        Logger.metadata(replication_id: replication.id, database_id: replication.postgres_database.id)
        monitor_wal_sender_for_replication(replication, acc_states)
      end)

    Logger.metadata(replication_id: nil, database_id: nil)

    acc
  rescue
    error ->
      Logger.error("Failed to monitor WAL senders: #{inspect(error)}")
      previous_states
  end

  defp monitor_wal_sender_for_replication(replication, previous_states) do
    db = replication.postgres_database
    slot_name = replication.slot_name
    replication_id = replication.id

    query = """
    SELECT * FROM pg_stat_activity
    WHERE backend_type = 'walsender'
    AND query ILIKE $1
    """

    case Postgres.query(db, query, ["%#{slot_name}%"]) do
      {:ok, %Postgrex.Result{rows: rows, columns: columns}} ->
        process_results_for_replication(rows, columns, db, replication_id, previous_states)

      {:error, error} ->
        Logger.error("Failed to query WAL sender status for replication #{replication_id}: #{inspect(error)}")
        previous_states
    end
  rescue
    error ->
      Logger.error("Failed to query WAL sender status for replication #{replication.id}: #{inspect(error)}")
      previous_states
  end

  defp process_results_for_replication([], _columns, _db, replication_id, previous_states) do
    # No WAL sender found - set all previous states for this replication to 0
    replication_previous_states = Map.get(previous_states, replication_id, MapSet.new())

    Enum.each(replication_previous_states, fn {database_id, prev_wait_event_type, prev_wait_event, prev_pid} ->
      Gauge.set(
        [name: @gauge_name, labels: [database_id, replication_id, prev_wait_event_type, prev_wait_event, prev_pid]],
        0
      )
    end)

    Map.put(previous_states, replication_id, MapSet.new())
  end

  defp process_results_for_replication([row | rest], columns, db, replication_id, previous_states) do
    unless rest == [] do
      Logger.warning("Found more than one walsender for replication_id, ignoring rest")
    end

    row_map = columns |> Enum.zip(row) |> Map.new()

    wait_event_type = row_map["wait_event_type"] || "unknown"
    wait_event = row_map["wait_event"] || "unknown"
    pid = to_string(row_map["pid"] || "unknown")

    # Create current state combination
    current_state = {db.id, wait_event_type, wait_event, pid}

    # Get previous states for this replication
    replication_previous_states = Map.get(previous_states, replication_id, MapSet.new())

    # Set all previous states for this replication to 0
    Enum.each(replication_previous_states, fn {database_id, prev_wait_event_type, prev_wait_event, prev_pid} ->
      Gauge.set(
        [name: @gauge_name, labels: [database_id, replication_id, prev_wait_event_type, prev_wait_event, prev_pid]],
        0
      )
    end)

    # Set current state to 1
    Gauge.set(
      [name: @gauge_name, labels: [db.id, replication_id, wait_event_type, wait_event, pid]],
      1
    )

    # Add current state to previous states for this replication
    updated_replication_previous_states = MapSet.put(replication_previous_states, current_state)
    Map.put(previous_states, replication_id, updated_replication_previous_states)
  end
end

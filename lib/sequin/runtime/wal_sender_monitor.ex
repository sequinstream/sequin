defmodule Sequin.Runtime.WalSenderMonitor do
  @moduledoc false
  use GenServer, restart: :temporary
  use Prometheus.Metric

  alias Sequin.Postgres

  require Logger

  @gauge_name :sequin_replication_wal_sender_state
  @monitor_interval 1000

  defstruct [:db, :previous_states]

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
    :sequin |> Application.get_env(__MODULE__) |> Keyword.get(:enabled?, false)
  end

  @impl GenServer
  def init(_opts) do
    setup_prometheus()
    schedule_monitor()

    {:ok, %__MODULE__{previous_states: MapSet.new()}}
  end

  @impl GenServer
  def handle_info(:monitor, state) do
    state = maybe_put_db(state)
    new_previous_states = monitor_wal_sender(state.db, state.previous_states)
    schedule_monitor()

    {:noreply, %{state | previous_states: new_previous_states}}
  end

  defp maybe_put_db(%{db: nil} = state) do
    %{state | db: Sequin.Databases.get_db!("05479dda-e45e-4c80-ae83-d8c3390ee97e")}
  end

  defp maybe_put_db(state) do
    state
  end

  defp setup_prometheus do
    Gauge.new(
      name: @gauge_name,
      help: "WAL sender state (1 = active state, 0 = inactive state)",
      labels: [:database_id, :wait_event_type, :wait_event]
    )
  end

  defp schedule_monitor do
    Process.send_after(self(), :monitor, @monitor_interval)
  end

  defp monitor_wal_sender(db, previous_states) do
    case Postgres.query(db, "select * from pg_stat_activity where backend_type = 'walsender'") do
      {:ok, %Postgrex.Result{rows: rows, columns: columns}} ->
        process_results(rows, columns, db, previous_states)

      {:error, error} ->
        Logger.error("Failed to query WAL sender status: #{inspect(error)}")
        previous_states
    end
  rescue
    error ->
      Logger.error("Failed to query WAL sender status: #{inspect(error)}")
      previous_states
  end

  defp process_results([], _columns, _db, previous_states) do
    # No WAL sender found - set all previous states to 0
    Enum.each(previous_states, fn {database_id, prev_wait_event_type, prev_wait_event} ->
      Gauge.set(
        [name: @gauge_name, labels: [database_id, prev_wait_event_type, prev_wait_event]],
        0
      )
    end)

    MapSet.new()
  end

  defp process_results([row | rest], columns, db, previous_states) do
    row_map = columns |> Enum.zip(row) |> Map.new()

    wait_event_type = row_map["wait_event_type"] || "unknown"
    wait_event = row_map["wait_event"] || "unknown"

    # Create current state combination
    current_state = {db.id, wait_event_type, wait_event}

    # Set all previous states to 0
    Enum.each(previous_states, fn {database_id, prev_wait_event_type, prev_wait_event} ->
      Gauge.set(
        [name: @gauge_name, labels: [database_id, prev_wait_event_type, prev_wait_event]],
        0
      )
    end)

    # Set current state to 1
    Gauge.set(
      [name: @gauge_name, labels: [db.id, wait_event_type, wait_event]],
      1
    )

    # Add current state to previous states for next iteration
    updated_previous_states = MapSet.put(previous_states, current_state)

    process_results(rest, columns, db, updated_previous_states)
  end
end

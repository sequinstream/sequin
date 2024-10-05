defmodule Sequin.ReplicationRuntime.WalEventServer do
  @moduledoc false
  use GenStateMachine, callback_mode: [:handle_event_function, :state_enter]

  alias Ecto.Adapters.SQL.Sandbox
  alias Sequin.Databases
  alias Sequin.Postgres
  alias Sequin.Replication

  require Logger

  @default_batch_size 1000
  @default_delay_ms 5_000

  # Client API

  def start_link(opts \\ []) do
    replication_slot_id = Keyword.fetch!(opts, :replication_slot_id)
    destination_oid = Keyword.fetch!(opts, :destination_oid)
    destination_database_id = Keyword.fetch!(opts, :destination_database_id)

    GenStateMachine.start_link(__MODULE__, opts,
      name: via_tuple({replication_slot_id, destination_oid, destination_database_id})
    )
  end

  def via_tuple({replication_slot_id, destination_oid, destination_database_id}) do
    Sequin.Registry.via_tuple({__MODULE__, {replication_slot_id, destination_oid, destination_database_id}})
  end

  def child_spec(opts) do
    replication_slot_id = Keyword.fetch!(opts, :replication_slot_id)
    destination_oid = Keyword.fetch!(opts, :destination_oid)
    destination_database_id = Keyword.fetch!(opts, :destination_database_id)

    %{
      id: {__MODULE__, {replication_slot_id, destination_oid, destination_database_id}},
      start: {__MODULE__, :start_link, [opts]},
      restart: :temporary,
      type: :worker
    }
  end

  defmodule State do
    @moduledoc false
    use TypedStruct

    alias Sequin.Databases.PostgresDatabase
    alias Sequin.Replication

    typedstruct do
      field :replication_slot, Replication.PostgresReplicationSlot.t()
      field :destination_table, PostgresDatabase.Table.t()
      field :destination_database, PostgresDatabase.t()
      field :wal_projection_ids, [Ecto.UUID.t()]
      field :task_ref, reference()
      field :test_pid, pid()
      field :successive_failure_count, integer(), default: 0
      field :batch_size, integer()
      field :delay_ms, integer()
    end
  end

  # Callbacks
  @impl GenStateMachine
  def init(opts) do
    test_pid = Keyword.get(opts, :test_pid)
    maybe_setup_allowances(test_pid)

    destination_oid = Keyword.fetch!(opts, :destination_oid)
    destination_database_id = Keyword.fetch!(opts, :destination_database_id)
    replication_slot_id = Keyword.fetch!(opts, :replication_slot_id)

    database = Databases.get_db!(destination_database_id)
    tables = Databases.tables(database)
    table = Sequin.Enum.find!(tables, &(&1.oid == destination_oid))
    replication_slot = Replication.get_pg_replication!(replication_slot_id)

    state = %State{
      replication_slot: replication_slot,
      destination_table: table,
      destination_database: database,
      wal_projection_ids: Keyword.fetch!(opts, :wal_projection_ids),
      test_pid: test_pid,
      batch_size: Keyword.get(opts, :batch_size, @default_batch_size),
      delay_ms: Keyword.get(opts, :delay_ms, @default_delay_ms)
    }

    actions = [
      {:next_event, :internal, :fetch_wal_events}
    ]

    {:ok, :fetching_wal_events, state, actions}
  end

  @impl GenStateMachine
  def handle_event(:enter, _old_state, :fetching_wal_events, state) do
    task =
      Task.Supervisor.async_nolink(
        Sequin.TaskSupervisor,
        fn ->
          maybe_setup_allowances(state.test_pid)
          Replication.list_wal_events(state.wal_projection_ids, limit: state.batch_size, order_by: [asc: :commit_lsn])
        end,
        timeout: 60_000
      )

    {:keep_state, %{state | task_ref: task.ref}}
  end

  def handle_event(:info, {ref, wal_events}, :fetching_wal_events, %State{task_ref: ref} = state) do
    Process.demonitor(ref, [:flush])
    state = %{state | task_ref: nil, successive_failure_count: 0}

    {:next_state, {:writing_to_destination, wal_events}, state}
  end

  def handle_event(:enter, _old_state, {:writing_to_destination, wal_events}, state) do
    task =
      Task.Supervisor.async_nolink(
        Sequin.TaskSupervisor,
        fn ->
          maybe_setup_allowances(state.test_pid)
          write_to_destination(state, wal_events)
        end,
        timeout: 60_000
      )

    {:keep_state, %{state | task_ref: task.ref}}
  end

  def handle_event(:info, {ref, :ok}, :writing_to_destination, %State{task_ref: ref} = state) do
    Process.demonitor(ref, [:flush])
    state = %{state | task_ref: nil}

    {:next_state, :deleting_wal_events, state}
  end

  def handle_event(:enter, _old_state, :deleting_wal_events, state) do
    task =
      Task.Supervisor.async_nolink(
        Sequin.TaskSupervisor,
        fn ->
          maybe_setup_allowances(state.test_pid)
          Replication.delete_wal_events(Enum.map(state.wal_events, & &1.id))
        end,
        timeout: 60_000
      )

    {:keep_state, %{state | task_ref: task.ref}}
  end

  def handle_event(:info, {ref, {:ok, _count}}, :deleting_wal_events, %State{task_ref: ref} = state) do
    Process.demonitor(ref, [:flush])
    state = %{state | task_ref: nil, wal_events: nil}

    if length(state.wal_events) == state.batch_size do
      {:next_state, :fetching_wal_events, state, [{:next_event, :internal, :fetch_wal_events}]}
    else
      # Random value between -0.2 and 0.2
      padding = :rand.uniform() * 0.4 - 0.2
      delay = trunc(state.delay_ms * (1 + padding))
      {:keep_state, state, [{:state_timeout, delay, :fetch_wal_events}]}
    end
  end

  def handle_event(:state_timeout, :fetch_wal_events, _state_name, state) do
    {:next_state, :fetching_wal_events, state, [{:next_event, :internal, :fetch_wal_events}]}
  end

  # Implement retry logic in case of task failure
  def handle_event(:info, {:DOWN, ref, _, _, reason}, state_name, %State{task_ref: ref} = state) do
    Logger.error("[WalEventServer] Task for #{state_name} failed with reason #{inspect(reason)}")

    state = %{state | task_ref: nil, successive_failure_count: state.successive_failure_count + 1}
    backoff = Sequin.Time.exponential_backoff(1000, state.successive_failure_count, :timer.minutes(5))

    actions = [
      {:state_timeout, backoff, :retry}
    ]

    {:next_state, {:awaiting_retry, state_name}, state, actions}
  end

  # Handle the retry after backoff
  def handle_event(:state_timeout, :retry, {:awaiting_retry, state_name}, state) do
    {:next_state, state_name, state}
  end

  defp write_to_destination(%State{} = state, wal_events) do
    Logger.info("[WalEventServer] Writing #{length(wal_events)} events to destination")

    # Prepare the data for insertion
    {columns, values} = prepare_insert_data(wal_events, state)

    # Craft the SQL query
    sql = """
    INSERT INTO test_event_logs (#{Enum.join(columns, ", ")})
    SELECT #{Enum.join(columns, ", ")}
    FROM unnest(#{Postgres.parameterized_tuple(length(columns))})
    AS t(#{Enum.join(columns, ", ")})
    ON CONFLICT (seq, source_database_id) DO NOTHING
    """

    # Execute the query on the destination database
    case Postgres.query(state.destination_database.id, sql, List.flatten(values)) do
      {:ok, result} ->
        Logger.info("[WalEventServer] Successfully wrote #{result.num_rows} rows to destination")
        {:ok, result.num_rows}

      {:error, error} ->
        Logger.error("[WalEventServer] Failed to write to destination: #{inspect(error)}")
        {:error, error}
    end
  end

  defp prepare_insert_data(wal_events, state) do
    columns = [
      "seq",
      "source_database_id",
      "source_oid",
      "source_pk",
      "record",
      "changes",
      "action",
      "committed_at",
      "inserted_at"
    ]

    values =
      Enum.map(wal_events, fn wal_event ->
        [
          wal_event.commit_lsn,
          state.replication_slot.postgres_database_id,
          state.destination_table.oid,
          Enum.join(wal_event.record_pks, ","),
          Jason.encode!(wal_event.data.record),
          Jason.encode!(wal_event.data.changes),
          Atom.to_string(wal_event.data.action),
          wal_event.data.metadata.commit_timestamp,
          NaiveDateTime.utc_now()
        ]
      end)

    {columns, values}
  end

  defp maybe_setup_allowances(nil), do: :ok

  defp maybe_setup_allowances(test_pid) do
    Sandbox.allow(Sequin.Repo, test_pid, self())
  end
end

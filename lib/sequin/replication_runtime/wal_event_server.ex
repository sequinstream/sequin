defmodule Sequin.ReplicationRuntime.WalEventServer do
  @moduledoc false
  use GenStateMachine, callback_mode: [:handle_event_function, :state_enter]

  alias Ecto.Adapters.SQL.Sandbox
  alias Sequin.Databases
  alias Sequin.Error
  alias Sequin.Health
  alias Sequin.Postgres
  alias Sequin.Replication

  require Logger

  @default_batch_size 1000
  @default_interval_ms 5_000
  @max_backoff_ms :timer.minutes(5)

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
      field :wal_projections, [Replication.WalProjection.t()]
      field :task_ref, reference()
      field :test_pid, pid()
      field :successive_failure_count, integer(), default: 0
      field :batch_size, integer()
      field :interval_ms, integer()
      field :wal_events, list()
      field :events_pending?, boolean(), default: false
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

    wal_projection_ids = Keyword.fetch!(opts, :wal_projection_ids)
    wal_projections = Enum.map(wal_projection_ids, &Replication.get_wal_projection!(&1))
    database = Databases.get_db!(destination_database_id)
    {:ok, tables} = Databases.tables(database)
    table = Sequin.Enum.find!(tables, &(&1.oid == destination_oid))
    replication_slot = Replication.get_pg_replication!(replication_slot_id)

    state = %State{
      replication_slot: replication_slot,
      destination_table: table,
      destination_database: database,
      wal_projections: wal_projections,
      test_pid: test_pid,
      batch_size: Keyword.get(opts, :batch_size, @default_batch_size),
      interval_ms: Keyword.get(opts, :interval_ms, @default_interval_ms),
      events_pending?: false
    }

    actions = [
      {:state_timeout, 0, :fetch_wal_events},
      {:next_event, :internal, :subscribe_to_pubsub}
    ]

    {:ok, :idle, state, actions}
  end

  @impl GenStateMachine
  def handle_event(:enter, _, :idle, _) do
    :keep_state_and_data
  end

  def handle_event(:state_timeout, :fetch_wal_events, :idle, state) do
    {:next_state, :fetching_wal_events, state}
  end

  def handle_event(:enter, _old_state, :fetching_wal_events, %State{} = state) do
    wal_projection_ids = Enum.map(state.wal_projections, & &1.id)

    task =
      Task.Supervisor.async_nolink(
        Sequin.TaskSupervisor,
        fn ->
          maybe_setup_allowances(state.test_pid)
          Replication.list_wal_events(wal_projection_ids, limit: state.batch_size, order_by: [asc: :commit_lsn])
        end,
        timeout: 60_000
      )

    {:keep_state, %{state | task_ref: task.ref, events_pending?: false}}
  end

  def handle_event(:info, {ref, wal_events}, :fetching_wal_events, %State{task_ref: ref} = state) do
    Process.demonitor(ref, [:flush])
    state = %{state | task_ref: nil, successive_failure_count: 0}

    if wal_events == [] do
      if state.test_pid do
        send(state.test_pid, {__MODULE__, :no_events})
      end

      {:next_state, :idle, state, [fetch_timeout(state)]}
    else
      state = %{state | wal_events: wal_events}
      {:next_state, :writing_to_destination, state}
    end
  end

  def handle_event(:enter, _old_state, :writing_to_destination, state) do
    task =
      Task.Supervisor.async_nolink(
        Sequin.TaskSupervisor,
        fn ->
          maybe_setup_allowances(state.test_pid)
          write_to_destination(state, state.wal_events)
        end,
        timeout: 60_000
      )

    {:keep_state, %{state | task_ref: task.ref}}
  end

  def handle_event(:info, {ref, :ok}, :writing_to_destination, %State{task_ref: ref} = state) do
    Process.demonitor(ref, [:flush])
    state = %{state | task_ref: nil, successive_failure_count: 0}

    Enum.each(state.wal_projections, fn wal_projection ->
      Health.update(wal_projection, :destination_insert, :healthy)
    end)

    {:next_state, :deleting_wal_events, state}
  end

  def handle_event(:info, {ref, {:error, reason}}, :writing_to_destination, %State{task_ref: ref} = state) do
    Process.demonitor(ref, [:flush])
    Logger.error("[WalEventServer] Failed to write to destination: #{inspect(reason)}")

    Enum.each(state.wal_projections, fn wal_projection ->
      Health.update(wal_projection, :destination_insert, :error, Error.ServiceError.from_postgrex(reason))
    end)

    if state.test_pid do
      send(state.test_pid, {__MODULE__, :write_failed, reason})
    end

    state = %{state | task_ref: nil, successive_failure_count: state.successive_failure_count + 1}
    backoff = Sequin.Time.exponential_backoff(1000, state.successive_failure_count, @max_backoff_ms)

    actions = [
      {:state_timeout, backoff, :retry_write}
    ]

    {:next_state, :awaiting_retry, state, actions}
  end

  def handle_event(:state_timeout, :retry_write, :awaiting_retry, state) do
    {:next_state, :writing_to_destination, state}
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
    has_more? = length(state.wal_events) == state.batch_size

    if state.test_pid do
      send(state.test_pid, {__MODULE__, :wrote_events, length(state.wal_events)})
    end

    state = %{state | task_ref: nil, wal_events: nil}

    if has_more? do
      {:next_state, :fetching_wal_events, state}
    else
      {:next_state, :idle, state, [fetch_timeout(state)]}
    end
  end

  # Handle the retry after backoff
  def handle_event(:state_timeout, :retry, {:awaiting_retry, state_name}, state) do
    {:next_state, state_name, state}
  end

  # Implement retry logic in case of task failure
  def handle_event(:info, {:DOWN, ref, _, _, reason}, state_name, %State{task_ref: ref} = state) do
    Logger.error("[WalEventServer] Task for #{state_name} failed with reason #{inspect(reason)}")

    Enum.each(state.wal_projections, fn wal_projection ->
      Health.update(
        wal_projection,
        :destination_insert,
        :error,
        Error.service(service: __MODULE__, message: "Unknown error")
      )
    end)

    state = %{state | task_ref: nil, successive_failure_count: state.successive_failure_count + 1}
    backoff = Sequin.Time.exponential_backoff(1000, state.successive_failure_count, @max_backoff_ms)

    actions = [
      {:state_timeout, backoff, :retry}
    ]

    {:next_state, {:awaiting_retry, state_name}, state, actions}
  end

  def handle_event(:internal, :subscribe_to_pubsub, _state, %State{wal_projections: wal_projections}) do
    Enum.each(wal_projections, fn wal_projection ->
      Phoenix.PubSub.subscribe(Sequin.PubSub, "wal_event_inserted:#{wal_projection.id}")
    end)

    :keep_state_and_data
  end

  def handle_event(:info, {:wal_event_inserted, _wal_projection_id}, :idle, data) do
    {:next_state, :fetching_wal_events, data}
  end

  def handle_event(:info, {:wal_event_inserted, _wal_projection_id}, _state, data) do
    {:keep_state, %{data | events_pending?: true}}
  end

  defp fetch_timeout(%State{events_pending?: true}), do: {:state_timeout, 0, :fetch_wal_events}

  defp fetch_timeout(%State{} = state) do
    padding = :rand.uniform() * 0.4 - 0.2
    delay = trunc(state.interval_ms * (1 + padding))
    {:state_timeout, delay, :fetch_wal_events}
  end

  defp write_to_destination(%State{} = state, wal_events) do
    Logger.info("[WalEventServer] Writing #{length(wal_events)} events to destination")
    wal_events = Enum.sort_by(wal_events, & &1.commit_lsn)

    table = Postgres.quote_name(state.destination_table.schema, state.destination_table.name)

    # Prepare the data for insertion
    {columns, values} = prepare_insert_data(wal_events, state)

    # Craft the SQL query with explicit type casts
    sql = """
    INSERT INTO #{table} (#{Enum.join(columns, ", ")})
    SELECT *
    FROM unnest(
      $1::bigint[],
      $2::uuid[],
      $3::bigint[],
      $4::text[],
      $5::jsonb[],
      $6::jsonb[],
      $7::text[],
      $8::timestamp with time zone[],
      $9::timestamp with time zone[]
    ) AS t(#{Enum.join(columns, ", ")})
    ON CONFLICT (seq, source_database_id, record_pk)
      DO UPDATE SET
        source_table_oid = EXCLUDED.source_table_oid,
        record = EXCLUDED.record,
        changes = EXCLUDED.changes,
        action = EXCLUDED.action,
        committed_at = EXCLUDED.committed_at,
        inserted_at = EXCLUDED.inserted_at
    """

    dest =
      if env() == :test do
        Sequin.Repo
      else
        state.destination_database
      end

    # Execute the query on the destination database
    case Postgres.query(dest, sql, values) do
      {:ok, result} ->
        Logger.info("[WalEventServer] Successfully wrote #{result.num_rows} rows to destination")
        :ok

      {:error, error} ->
        Logger.error("[WalEventServer] Failed to write to destination: #{inspect(error)}")
        {:error, error}
    end
  end

  defp prepare_insert_data(wal_events, state) do
    columns = [
      "seq",
      "source_database_id",
      "source_table_oid",
      "record_pk",
      "record",
      "changes",
      "action",
      "committed_at",
      "inserted_at"
    ]

    values =
      wal_events
      |> Enum.map(fn wal_event ->
        [
          wal_event.commit_lsn,
          UUID.string_to_binary!(state.replication_slot.postgres_database_id),
          wal_event.source_table_oid,
          Enum.join(wal_event.record_pks, ","),
          wal_event.record,
          wal_event.changes,
          Atom.to_string(wal_event.action),
          wal_event.committed_at,
          DateTime.utc_now()
        ]
      end)
      |> Enum.zip()
      |> Enum.map(&Tuple.to_list/1)

    {columns, values}
  end

  defp maybe_setup_allowances(nil), do: :ok

  defp maybe_setup_allowances(test_pid) do
    Sandbox.allow(Sequin.Repo, test_pid, self())
  end

  defp env do
    Application.get_env(:sequin, :env)
  end
end

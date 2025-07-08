defmodule Sequin.Runtime.WalPipelineServer do
  @moduledoc false
  use GenStateMachine, callback_mode: [:handle_event_function, :state_enter]

  alias Ecto.Adapters.SQL.Sandbox
  alias Sequin.Databases
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Databases.PostgresDatabaseTable
  alias Sequin.Error
  alias Sequin.Health
  alias Sequin.Health.Event
  alias Sequin.Postgres
  alias Sequin.Replication
  alias Sequin.Replication.WalEvent
  alias Sequin.Replication.WalPipeline
  alias Sequin.Repo

  require Logger

  @default_batch_size 1000
  @default_interval_ms 5_000
  @max_backoff_ms to_timeout(minute: 5)

  # Client API

  def start_link(opts \\ []) do
    replication_slot_id = Keyword.fetch!(opts, :replication_slot_id)
    destination_oid = Keyword.fetch!(opts, :destination_oid)
    destination_database_id = Keyword.fetch!(opts, :destination_database_id)

    GenStateMachine.start_link(__MODULE__, opts,
      name: via_tuple({replication_slot_id, destination_oid, destination_database_id})
    )
  end

  def via_tuple(%WalPipeline{} = pipeline) do
    via_tuple({pipeline.replication_slot_id, pipeline.destination_oid, pipeline.destination_database_id})
  end

  def via_tuple({replication_slot_id, destination_oid, destination_database_id}) do
    {:via, :syn, {:replication, {__MODULE__, {replication_slot_id, destination_oid, destination_database_id}}}}
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
      field :destination_table, PostgresDatabaseTable.t()
      field :destination_database, PostgresDatabase.t()
      field :wal_pipelines, [Replication.WalPipeline.t()]
      field :task_ref, reference()
      field :test_pid, pid()
      field :successive_failure_count, integer(), default: 0
      field :batch_size, integer()
      field :interval_ms, integer()
      field :wal_events, list()
      field :events_pending?, boolean(), default: false
      field :event_table_version, atom()
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

    wal_pipeline_ids = Keyword.fetch!(opts, :wal_pipeline_ids)

    wal_pipelines =
      wal_pipeline_ids
      |> Enum.map(&Replication.get_wal_pipeline!(&1))
      |> Repo.preload(:source_database)

    database = Databases.get_db!(destination_database_id)
    {:ok, tables} = Databases.tables(database)
    table = Sequin.Enum.find!(tables, &(&1.oid == destination_oid))
    replication_slot = Replication.get_pg_replication!(replication_slot_id)

    Logger.metadata(
      account_id: replication_slot.account_id,
      replication_slot_id: replication_slot_id,
      table_oid: destination_oid,
      database_id: destination_database_id,
      wal_pipeline_ids: wal_pipeline_ids
    )

    Logger.info("[WalPipelineServer] Starting")

    state = %State{
      replication_slot: replication_slot,
      destination_table: table,
      destination_database: database,
      wal_pipelines: wal_pipelines,
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
  def handle_event(:enter, _, :idle, %State{} = state) do
    state = maybe_put_event_table_version(state)
    {:keep_state, state}
  end

  def handle_event(:enter, _, :awaiting_retry, _) do
    :keep_state_and_data
  end

  def handle_event(:state_timeout, :fetch_wal_events, :idle, state) do
    {:next_state, :fetching_wal_events, state}
  end

  def handle_event(:enter, _old_state, :fetching_wal_events, %State{} = state) do
    wal_pipeline_ids = Enum.map(state.wal_pipelines, & &1.id)

    task =
      Task.Supervisor.async_nolink(
        Sequin.TaskSupervisor,
        fn ->
          maybe_setup_allowances(state.test_pid)

          wal_pipeline_ids
          |> Replication.list_wal_events(limit: state.batch_size, order_by: [asc: :commit_lsn])
          |> load_unchanged_toasts(state)
        end,
        timeout: 60_000
      )

    {:keep_state, %{state | task_ref: task.ref, events_pending?: false}}
  end

  def handle_event(:info, {ref, wal_events}, :fetching_wal_events, %State{task_ref: ref} = state) do
    Process.demonitor(ref, [:flush])
    state = %{state | task_ref: nil, successive_failure_count: 0}

    Enum.each(state.wal_pipelines, fn wal_pipeline ->
      Health.put_event(wal_pipeline, %Event{slug: :messages_fetch, status: :success})
    end)

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

    Enum.each(state.wal_pipelines, fn wal_pipeline ->
      Health.put_event(wal_pipeline, %Event{slug: :destination_insert, status: :success})
    end)

    {:next_state, :deleting_wal_events, state}
  end

  def handle_event(:info, {ref, {:error, reason}}, :writing_to_destination, %State{task_ref: ref} = state) do
    Process.demonitor(ref, [:flush])
    Logger.error("[WalPipelineServer] Failed to write to destination: #{inspect(reason)}")

    Enum.each(state.wal_pipelines, fn wal_pipeline ->
      Health.put_event(wal_pipeline, %Event{
        slug: :destination_insert,
        status: :fail,
        error: Error.ServiceError.from_postgrex(reason)
      })
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

    Enum.each(state.wal_pipelines, fn wal_pipeline ->
      Health.put_event(wal_pipeline, %Event{slug: :messages_delete, status: :success})
    end)

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
    Logger.error("[WalPipelineServer] Task for #{state_name} failed with reason #{inspect(reason)}")

    event_slug =
      case state_name do
        :fetching_wal_events -> :messages_fetch
        :deleting_wal_events -> :messages_delete
        :writing_to_destination -> :destination_insert
      end

    Enum.each(state.wal_pipelines, fn wal_pipeline ->
      Health.put_event(
        wal_pipeline,
        %Event{
          slug: event_slug,
          status: :fail,
          error: Error.service(service: __MODULE__, message: "Unknown error")
        }
      )
    end)

    state = %{state | task_ref: nil, successive_failure_count: state.successive_failure_count + 1}
    backoff = Sequin.Time.exponential_backoff(1000, state.successive_failure_count, @max_backoff_ms)

    actions = [
      {:state_timeout, backoff, :retry}
    ]

    {:next_state, {:awaiting_retry, state_name}, state, actions}
  end

  def handle_event(:internal, :subscribe_to_pubsub, _state, %State{wal_pipelines: wal_pipelines}) do
    Enum.each(wal_pipelines, fn wal_pipeline ->
      :syn.join(:replication, {:wal_event_inserted, wal_pipeline.id}, self())
    end)

    :keep_state_and_data
  end

  def handle_event(:info, :wal_event_inserted, :idle, data) do
    {:next_state, :fetching_wal_events, data}
  end

  def handle_event(:info, :wal_event_inserted, _state, data) do
    {:keep_state, %{data | events_pending?: true}}
  end

  defp maybe_put_event_table_version(%State{event_table_version: nil} = state) do
    case Postgres.event_table_version(state.destination_table) do
      {:ok, version} ->
        %{state | event_table_version: version}

      {:error, error} ->
        Logger.error("Unable to connect to Postgres to check event table version")
        raise error
    end
  end

  defp maybe_put_event_table_version(%State{} = state), do: state

  defp fetch_timeout(%State{events_pending?: true}), do: {:state_timeout, 0, :fetch_wal_events}

  defp fetch_timeout(%State{} = state) do
    padding = :rand.uniform() * 0.4 - 0.2
    delay = trunc(state.interval_ms * (1 + padding))
    {:state_timeout, delay, :fetch_wal_events}
  end

  defp write_to_destination(%State{} = state, wal_events) do
    Logger.info("[WalPipelineServer] Writing #{length(wal_events)} events to destination")
    wal_events = Enum.sort_by(wal_events, & &1.commit_lsn)

    # Prepare the data for insertion
    {columns, values} = prepare_insert_data(wal_events, state)

    # Craft the SQL query with explicit type casts
    sql = insert_sql(state, columns)

    dest =
      if env() == :test do
        Sequin.Repo
      else
        state.destination_database
      end

    # Execute the query on the destination database
    case Postgres.query(dest, sql, values) do
      {:ok, result} ->
        Logger.info("[WalPipelineServer] Successfully wrote #{result.num_rows} rows to destination")
        :ok

      # Switch this out when we switch `seq` to bigserial
      # We only need to do this because one batch can contain multiple events with the same `seq`
      {:error, %Postgrex.Error{message: nil, postgres: %{code: :cardinality_violation}}} ->
        Logger.error("[WalPipelineServer] Cardinality violation, splitting and retrying")

        Enum.reduce_while(wal_events, :ok, fn wal_event, :ok ->
          case write_to_destination(state, [wal_event]) do
            :ok -> {:cont, :ok}
            {:error, error} -> {:halt, {:error, error}}
          end
        end)

      {:error, error} ->
        Logger.error("[WalPipelineServer] Failed to write to destination: #{inspect(error)}")
        {:error, error}
    end
  end

  defp insert_sql(%{event_table_version: :v0} = state, columns) do
    table = Postgres.quote_name(state.destination_table.schema, state.destination_table.name)

    """
    INSERT INTO #{table} (#{Enum.join(columns, ", ")})
    SELECT *
    FROM unnest(
      $1::bigint[],
      $2::uuid[],
      $3::bigint[],
      $4::text[],
      $5::text[],
      $6::text[],
      $7::jsonb[],
      $8::jsonb[],
      $9::text[],
      $10::timestamp with time zone[],
      $11::timestamp with time zone[]
    ) AS t(#{Enum.join(columns, ", ")})
    ON CONFLICT (seq, committed_at, source_database_id, record_pk)
      DO UPDATE SET
        source_table_oid = EXCLUDED.source_table_oid,
        source_table_schema = EXCLUDED.source_table_schema,
        source_table_name = EXCLUDED.source_table_name,
        record = EXCLUDED.record,
        changes = EXCLUDED.changes,
        action = EXCLUDED.action,
        committed_at = EXCLUDED.committed_at,
        inserted_at = EXCLUDED.inserted_at
    """
  end

  defp insert_sql(%{event_table_version: :"v3.28.25"} = state, columns) do
    table = Postgres.quote_name(state.destination_table.schema, state.destination_table.name)

    """
    INSERT INTO #{table} (#{Enum.join(columns, ", ")})
    SELECT *
    FROM unnest(
      $1::bigint[],
      $2::uuid[],
      $3::bigint[],
      $4::text[],
      $5::text[],
      $6::text[],
      $7::jsonb[],
      $8::jsonb[],
      $9::text[],
      $10::timestamp with time zone[],
      $11::timestamp with time zone[],
      -- adds transaction_annotations column
      $12::jsonb[]
    ) AS t(#{Enum.join(columns, ", ")})
    ON CONFLICT (seq, committed_at, source_database_id, record_pk)
      DO UPDATE SET
        source_table_oid = EXCLUDED.source_table_oid,
        source_table_schema = EXCLUDED.source_table_schema,
        source_table_name = EXCLUDED.source_table_name,
        record = EXCLUDED.record,
        changes = EXCLUDED.changes,
        action = EXCLUDED.action,
        committed_at = EXCLUDED.committed_at,
        inserted_at = EXCLUDED.inserted_at,
        -- adds transaction_annotations column
        transaction_annotations = EXCLUDED.transaction_annotations
    """
  end

  defp prepare_insert_data(wal_events, state) do
    include_transaction_annotations = state.event_table_version == :"v3.28.25"

    columns = [
      "seq",
      "source_database_id",
      "source_table_oid",
      "source_table_schema",
      "source_table_name",
      "record_pk",
      "record",
      "changes",
      "action",
      "committed_at",
      "inserted_at"
    ]

    columns = maybe_push(columns, include_transaction_annotations, "transaction_annotations")

    values =
      wal_events
      |> Enum.map(fn wal_event ->
        values =
          [
            wal_event.commit_lsn + wal_event.commit_idx,
            Sequin.String.string_to_binary!(state.replication_slot.postgres_database_id),
            wal_event.source_table_oid,
            wal_event.source_table_schema,
            wal_event.source_table_name,
            Enum.join(wal_event.record_pks, ","),
            wal_event.record,
            wal_event.changes,
            Atom.to_string(wal_event.action),
            wal_event.committed_at,
            DateTime.utc_now()
          ]

        maybe_push(values, include_transaction_annotations, wal_event.transaction_annotations)
      end)
      |> Enum.zip()
      |> Enum.map(&Tuple.to_list/1)

    {columns, values}
  end

  defp maybe_push(list, false, _element), do: list
  defp maybe_push(list, true, element), do: list ++ [element]

  defp maybe_setup_allowances(nil), do: :ok

  defp maybe_setup_allowances(test_pid) do
    Sandbox.allow(Sequin.Repo, test_pid, self())
    Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
  end

  defp env do
    Application.get_env(:sequin, :env)
  end

  defp load_unchanged_toasts(wal_events, %State{} = state) do
    events_with_toast = Enum.filter(wal_events, &has_unchanged_toast?/1)

    case events_with_toast do
      [] ->
        wal_events

      events ->
        loaded_toasts = fetch_all_toast_values(events, state)
        replace_toast_values(wal_events, loaded_toasts)
    end
  end

  defp fetch_all_toast_values(events, state) do
    events
    |> Enum.group_by(&{&1.wal_pipeline_id, &1.source_table_oid})
    |> Enum.map(&fetch_toast_values_for_table(&1, state))
    |> Enum.reduce(%{}, &Map.merge/2)
  end

  defp fetch_toast_values_for_table({{pipeline_id, table_oid}, events}, state) do
    source_database = get_source_database(state.wal_pipelines, pipeline_id)
    table = get_table(source_database, table_oid)
    pk_names = get_primary_key_names(table)

    events
    |> fetch_toast_values(table, source_database)
    |> build_toast_values_map(table.oid, pk_names)
  end

  defp get_source_database(pipelines, pipeline_id) do
    pipelines
    |> Enum.find(&(&1.id == pipeline_id))
    |> Map.fetch!(:source_database)
  end

  defp get_table(source_database, table_oid) do
    source_database
    |> Map.fetch!(:tables)
    |> Enum.find(&(&1.oid == table_oid))
  end

  defp get_primary_key_names(table) do
    table.columns
    |> Enum.filter(& &1.is_pk?)
    |> Enum.map(& &1.name)
  end

  defp build_toast_values_map(toast_values, table_oid, pk_names) do
    Map.new(toast_values, fn toast_value ->
      pk_values =
        pk_names
        |> Enum.map(&Map.get(toast_value, &1))
        |> Enum.map(&to_string/1)

      {{table_oid, pk_values}, toast_value}
    end)
  end

  defp replace_toast_values(wal_events, loaded_toasts) do
    Enum.map(wal_events, fn event ->
      if has_unchanged_toast?(event) do
        update_toast_values(event, loaded_toasts)
      else
        event
      end
    end)
  end

  defp has_unchanged_toast?(%WalEvent{record: record}) when is_map(record) do
    Enum.any?(record, fn {_key, value} -> value == "unchanged_toast" end)
  end

  defp fetch_toast_values(events, %PostgresDatabaseTable{} = table, %PostgresDatabase{} = source_database) do
    table_name = Postgres.quote_name(table.schema, table.name)
    pk_columns = get_sorted_pk_columns(table)

    query = build_toast_query(table_name, events, table)
    casted_pks = cast_primary_keys(events, pk_columns)

    case execute_toast_query(source_database, query, casted_pks) do
      {:ok, result} ->
        process_toast_results(result, table.columns)

      {:error, error} ->
        Logger.error("[WalPipelineServer] Failed to fetch TOAST values: #{inspect(error)}")
        raise error
    end
  end

  defp get_sorted_pk_columns(table) do
    table.columns
    |> Enum.filter(& &1.is_pk?)
    |> Enum.sort_by(& &1.attnum)
  end

  defp build_toast_query(table_name, events, table) do
    pk_conditions = build_pk_conditions(events, table)

    """
    SELECT *
    FROM #{table_name}
    WHERE #{pk_conditions}
    """
  end

  defp cast_primary_keys(events, pk_columns) do
    Enum.flat_map(events, fn %WalEvent{record_pks: pks} ->
      pks
      |> Enum.zip(pk_columns)
      |> Enum.map(fn {pk, col} -> Postgres.cast_value(pk, col.type) end)
    end)
  end

  defp execute_toast_query(source_database, query, casted_pks) do
    Postgres.query(source_database, query, casted_pks)
  end

  defp process_toast_results(%Postgrex.Result{} = result, columns) do
    column_types = build_column_type_map(columns)

    result
    |> Postgres.result_to_maps()
    |> Enum.map(&process_row(&1, column_types))
  end

  defp build_column_type_map(columns) do
    Map.new(columns, fn col -> {col.name, col.type} end)
  end

  defp process_row(row, column_types) do
    Map.new(row, fn {col_name, value} ->
      {col_name, cast_column_value(value, Map.fetch!(column_types, col_name))}
    end)
  end

  defp cast_column_value(nil, _), do: nil
  defp cast_column_value(value, "uuid"), do: Sequin.String.binary_to_string!(value)
  defp cast_column_value(value, _), do: value

  defp build_pk_conditions(events, %PostgresDatabaseTable{} = table) do
    pk_columns = Enum.filter(table.columns, & &1.is_pk?)

    events
    |> Enum.map_join(" OR ", fn _event ->
      pk_columns
      |> Enum.map_join(" AND ", &"#{&1.name} = ?")
      |> then(&"(#{&1})")
    end)
    |> Postgres.parameterize_sql()
  end

  defp update_toast_values(event, toast_values) do
    case Map.get(toast_values, {event.source_table_oid, event.record_pks}) do
      nil ->
        event

      values ->
        updated_record =
          Map.new(event.record, fn
            {key, "unchanged_toast"} -> {key, Map.fetch!(values, key)}
            entry -> entry
          end)

        %{event | record: updated_record}
    end
  end
end

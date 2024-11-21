defmodule Sequin.DatabasesRuntime.TableProducerServer do
  @moduledoc false
  use GenStateMachine, callback_mode: [:handle_event_function, :state_enter]

  alias Ecto.Adapters.SQL.Sandbox
  alias Sequin.Consumers
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.RecordConsumerState
  alias Sequin.Consumers.SequenceFilter
  alias Sequin.Databases.PostgresDatabaseTable
  alias Sequin.Databases.Sequence
  alias Sequin.DatabasesRuntime.TableProducer
  alias Sequin.Health
  alias Sequin.Repo

  require Logger

  # Client API

  def start_link(opts \\ []) do
    consumer = Keyword.fetch!(opts, :consumer)
    table_oid = Keyword.fetch!(opts, :table_oid)
    GenStateMachine.start_link(__MODULE__, opts, name: via_tuple({consumer.id, table_oid}))
  end

  def via_tuple({consumer_id, table_oid}) do
    Sequin.Registry.via_tuple({__MODULE__, {consumer_id, table_oid}})
  end

  # Convenience function
  def via_tuple(%_{} = consumer) do
    consumer = Repo.preload(consumer, :sequence)
    via_tuple({consumer.id, consumer.sequence.table_oid})
  end

  # Convenience function
  def via_tuple(consumer_id) do
    consumer =
      consumer_id
      |> Consumers.get_consumer!()
      |> Repo.preload(:sequence)

    table_oid = table_oid(consumer)
    via_tuple({consumer.id, table_oid})
  end

  def child_spec(opts) do
    consumer = Keyword.fetch!(opts, :consumer)
    table_oid = Keyword.fetch!(opts, :table_oid)

    %{
      id: {__MODULE__, {consumer.id, table_oid}},
      start: {__MODULE__, :start_link, [opts]},
      # Will get restarted by Starter in event of crash
      restart: :temporary,
      type: :worker
    }
  end

  defmodule State do
    @moduledoc false
    use TypedStruct

    typedstruct do
      field :consumer, String.t()
      field :cursor_max, map()
      field :cursor_min, map()
      field :page_size, integer()
      field :task_ref, reference()
      field :test_pid, pid()
      field :table_oid, integer()
      field :successive_failure_count, integer(), default: 0
    end
  end

  # Callbacks

  @impl GenStateMachine
  def init(opts) do
    test_pid = Keyword.get(opts, :test_pid)
    maybe_setup_allowances(test_pid)
    consumer = opts |> Keyword.fetch!(:consumer) |> preload_consumer()

    Logger.metadata(consumer_id: consumer.id)

    state = %State{
      consumer: consumer,
      page_size: Keyword.get(opts, :page_size, 1000),
      test_pid: test_pid,
      table_oid: Keyword.fetch!(opts, :table_oid)
    }

    actions = [
      {:next_event, :internal, :init}
    ]

    {:ok, :initializing, state, actions}
  end

  @impl GenStateMachine
  def handle_event(:enter, _old_state, :initializing, _state) do
    :keep_state_and_data
  end

  def handle_event(:internal, :init, :initializing, %State{
        consumer: %{record_consumer_state: %RecordConsumerState{producer: :wal}}
      }) do
    Logger.info("[TableProducerServer] Producer should only be :wal, shutting down")
    {:stop, :normal}
  end

  def handle_event(:internal, :init, :initializing, state) do
    consumer = state.consumer

    case consumer.active_backfill do
      nil ->
        Logger.info("[TableProducerServer] No active backfill found, shutting down")
        {:stop, :normal}

      backfill ->
        cursor_min = TableProducer.cursor(backfill.id, :min)
        cursor_min = cursor_min || backfill.initial_min_cursor
        state = %{state | cursor_min: cursor_min}

        # Add initial count if not set
        if is_nil(backfill.rows_initial_count) do
          case TableProducer.fast_count_estimate(database(state), table(state), cursor_min) do
            {:ok, count} ->
              Consumers.update_backfill(backfill, %{rows_initial_count: count})

            {:error, error} ->
              Logger.error("[TableProducerServer] Failed to get initial count: #{inspect(error)}")
          end
        end

        actions = [reload_consumer_timeout()]

        {:next_state, :query_max_cursor, state, actions}
    end
  end

  def handle_event(:enter, _old_state, :query_max_cursor, state) do
    include_min = state.cursor_min == initial_min_cursor(state.consumer)

    task =
      Task.Supervisor.async_nolink(
        Sequin.TaskSupervisor,
        fn ->
          maybe_setup_allowances(state.test_pid)

          TableProducer.fetch_max_cursor(
            database(state),
            table(state),
            state.cursor_min,
            limit: state.page_size,
            include_min: include_min
          )
        end,
        timeout: 60_000
      )

    {:keep_state, %{state | task_ref: task.ref}}
  end

  def handle_event(:info, {ref, {:ok, nil}}, :query_max_cursor, %State{task_ref: ref} = state) do
    Process.demonitor(ref, [:flush])
    Logger.info("[TableProducerServer] Max cursor query returned nil. Table pagination complete.")
    Consumers.table_producer_finished(state.consumer.id)
    TableProducer.delete_cursor(state.consumer.active_backfill.id)

    {:stop, :normal}
  end

  def handle_event(:info, {ref, {:ok, result}}, :query_max_cursor, %State{task_ref: ref} = state) do
    Process.demonitor(ref, [:flush])
    :ok = TableProducer.update_cursor(state.consumer.active_backfill.id, :max, result)
    state = %{state | cursor_max: result, task_ref: nil, successive_failure_count: 0}

    {:next_state, :query_fetch_records, state}
  end

  @page_size_multiplier 3
  def handle_event(:enter, _old_state, :query_fetch_records, state) do
    include_min = state.cursor_min == initial_min_cursor(state.consumer)

    task =
      Task.Supervisor.async_nolink(
        Sequin.TaskSupervisor,
        fn ->
          maybe_setup_allowances(state.test_pid)

          TableProducer.fetch_records_in_range(
            database(state),
            table(state),
            state.cursor_min,
            state.cursor_max,
            limit: state.page_size * @page_size_multiplier,
            include_min: include_min
          )
        end,
        timeout: 60_000
      )

    {:keep_state, %{state | task_ref: task.ref}}
  end

  def handle_event(:info, {ref, {:ok, result}}, :query_fetch_records, %State{task_ref: ref} = state) do
    Process.demonitor(ref, [:flush])
    # Reset failure count on success
    state = %{state | task_ref: nil, successive_failure_count: 0}

    if length(result) == state.page_size * @page_size_multiplier do
      if state.test_pid do
        send(state.test_pid, {__MODULE__, :page_limit_reached})
      end

      # This happens if a lot of records were suddenly committed inside of our page. We got back way
      # more results than expected.
      Logger.info("[TableProducerServer] Fetch records result size equals the limit. Resetting process.")
      {:next_state, :query_max_cursor, state}
    else
      # Call handle_records inline
      {:ok, _count} = handle_records(state.consumer, table(state), result)
      :ok = TableProducer.update_cursor(state.consumer.active_backfill.id, :min, state.cursor_max)
      state = %State{state | cursor_min: state.cursor_max, cursor_max: nil, consumer: preload_consumer(state.consumer)}
      {:next_state, :query_max_cursor, state}
    end
  end

  # Implement retry logic in case of task failure
  def handle_event(:info, {:DOWN, ref, _, _, reason}, state_name, %State{task_ref: ref} = state) do
    Logger.error("[TableProducerServer] Task for #{state_name} failed with reason #{inspect(reason)}")

    state = %{state | task_ref: nil, successive_failure_count: state.successive_failure_count + 1}
    backoff = Sequin.Time.exponential_backoff(1000, state.successive_failure_count, :timer.minutes(5))

    actions = [
      {:state_timeout, backoff, :retry}
    ]

    {:next_state, {:awaiting_retry, state_name}, state, actions}
  end

  def handle_event(:info, {ref, {:error, error}}, state_name, %State{task_ref: ref} = state) do
    Process.demonitor(ref, [:flush])
    Logger.error("[TableProducerServer] Task for #{state_name} failed with reason #{inspect(error)}", error: error)

    state = %{state | task_ref: nil, successive_failure_count: state.successive_failure_count + 1}
    backoff = Sequin.Time.exponential_backoff(1000, state.successive_failure_count, :timer.minutes(5))

    actions = [
      {:state_timeout, backoff, :retry}
    ]

    {:next_state, {:awaiting_retry, state_name}, state, actions}
  end

  def handle_event(:enter, _old_state, {:awaiting_retry, _state_name}, _state) do
    :keep_state_and_data
  end

  # Handle the retry after backoff
  def handle_event(:state_timeout, :retry, {:awaiting_retry, state_name}, state) do
    {:next_state, state_name, state}
  end

  def handle_event({:timeout, :reload_consumer}, _evt, _state_name, state) do
    case preload_consumer(state.consumer) do
      nil ->
        Logger.info("[TableProducerServer] Consumer #{state.consumer.id} not found, shutting down")
        {:stop, :normal}

      consumer ->
        case consumer.active_backfill do
          nil ->
            Logger.info("[TableProducerServer] No active backfill found, shutting down")
            {:stop, :normal}

          _backfill ->
            actions = [reload_consumer_timeout()]
            {:keep_state, %{state | consumer: consumer}, actions}
        end
    end
  end

  defp reload_consumer_timeout do
    {{:timeout, :reload_consumer}, :timer.minutes(1), nil}
  end

  defp database(%State{consumer: consumer}) do
    consumer.replication_slot.postgres_database
  end

  defp table_oid(%{sequence: %Sequence{table_oid: table_oid}}), do: table_oid
  defp table_oid(%{source_tables: [source_table | _]}), do: source_table.table_oid

  defp sort_column_attnum(%{sequence: %Sequence{sort_column_attnum: sort_column_attnum}}), do: sort_column_attnum
  defp sort_column_attnum(%{source_tables: [source_table | _]}), do: source_table.sort_column_attnum

  defp group_column_attnums(%{sequence_filter: %SequenceFilter{group_column_attnums: group_column_attnums}}),
    do: group_column_attnums

  defp group_column_attnums(%{source_tables: [source_table | _]}), do: source_table.group_column_attnums

  defp table(%State{} = state) do
    database = database(state)
    db_table = Sequin.Enum.find!(database.tables, &(&1.oid == state.table_oid))
    %{db_table | sort_column_attnum: sort_column_attnum(state.consumer)}
  end

  # Message handling
  defp handle_records(consumer, table, records) do
    Logger.info("[TableProducerServer] Handling #{length(records)} record(s)")

    records_by_column_attnum = records_by_column_attnum(table, records)
    total_processed = length(records)

    matching_records = Enum.filter(records_by_column_attnum, &Consumers.matches_record?(consumer, table.oid, &1))

    # Handle different message kinds
    case_result =
      case consumer.message_kind do
        :record -> handle_record_messages(consumer, table, matching_records)
        :event -> handle_event_messages(consumer, table, matching_records)
      end

    case case_result do
      {:ok, count} ->
        if backfill = consumer.active_backfill do
          Consumers.update_backfill(backfill, %{
            rows_processed_count: backfill.rows_processed_count + total_processed,
            rows_ingested_count: backfill.rows_ingested_count + length(matching_records)
          })
        end

        Health.update(consumer, :ingestion, :healthy)
        {:ok, count}

      error ->
        error
    end

    # Update backfill counts
  end

  defp handle_record_messages(consumer, table, matching_records) do
    consumer_records =
      Enum.map(matching_records, fn record_attnums_to_values ->
        Sequin.Map.from_ecto(%ConsumerRecord{
          consumer_id: consumer.id,
          table_oid: table.oid,
          record_pks: record_pks(table, record_attnums_to_values),
          group_id: generate_group_id(consumer, table, record_attnums_to_values),
          replication_message_trace_id: UUID.uuid4()
        })
      end)

    Consumers.insert_consumer_records(consumer_records)
  end

  defp handle_event_messages(consumer, table, matching_records) do
    consumer_events =
      Enum.map(matching_records, fn record_attnums_to_values ->
        %{
          consumer_id: consumer.id,
          # You may need to get this from somewhere
          commit_lsn: 0,
          record_pks: record_pks(table, record_attnums_to_values),
          table_oid: table.oid,
          deliver_count: 0,
          replication_message_trace_id: UUID.uuid4(),
          data: build_event_data(table, consumer, record_attnums_to_values)
        }
      end)

    Consumers.insert_consumer_events(consumer_events)
  end

  defp build_event_data(table, consumer, record_attnums_to_values) do
    %{
      action: :read,
      record: build_event_payload(table, record_attnums_to_values),
      metadata: %{
        table_name: table.name,
        table_schema: table.schema,
        consumer: consumer,
        commit_timestamp: DateTime.utc_now()
      }
    }
  end

  defp build_event_payload(table, record_attnums_to_values) do
    Map.new(table.columns, fn column -> {column.name, Map.get(record_attnums_to_values, column.attnum)} end)
  end

  defp records_by_column_attnum(%PostgresDatabaseTable{} = table, records) do
    Enum.map(records, fn record ->
      Map.new(table.columns, fn %PostgresDatabaseTable.Column{} = column ->
        {column.attnum, Map.get(record, column.name)}
      end)
    end)
  end

  defp initial_min_cursor(consumer) do
    consumer.active_backfill.initial_min_cursor
  end

  defp record_pks(%PostgresDatabaseTable{} = table, record_attnums_to_values) do
    table.columns
    |> Enum.filter(& &1.is_pk?)
    |> Enum.sort_by(& &1.attnum)
    |> Enum.map(&Map.fetch!(record_attnums_to_values, &1.attnum))
  end

  defp generate_group_id(consumer, table, record_attnums_to_values) do
    group_column_attnums = group_column_attnums(consumer)

    if group_column_attnums do
      Enum.map_join(group_column_attnums, ",", fn attnum ->
        to_string(Map.get(record_attnums_to_values, attnum))
      end)
    else
      table |> record_pks(record_attnums_to_values) |> Enum.join(",")
    end
  end

  defp maybe_setup_allowances(nil), do: :ok

  defp maybe_setup_allowances(test_pid) do
    Sandbox.allow(Sequin.Repo, test_pid, self())
  end

  defp preload_consumer(consumer) do
    Repo.preload(consumer, [:sequence, :active_backfill, replication_slot: :postgres_database], force: true)
  end
end

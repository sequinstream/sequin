defmodule Sequin.DatabasesRuntime.TableReaderServer do
  @moduledoc false
  use GenStateMachine, callback_mode: [:handle_event_function, :state_enter]

  alias Ecto.Adapters.SQL.Sandbox
  alias Sequin.Consumers
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerEventData
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.ConsumerRecordData
  alias Sequin.Consumers.SequenceFilter
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Databases.PostgresDatabaseTable
  alias Sequin.Databases.Sequence
  alias Sequin.DatabasesRuntime.TableReader
  alias Sequin.Error
  alias Sequin.Health
  alias Sequin.Repo

  require Logger

  @callback flush_batch(String.t() | pid(), map()) :: :ok
  @callback discard_batch(String.t() | pid(), String.t()) :: :ok

  # Client API

  def start_link(opts \\ []) do
    backfill_id = Keyword.fetch!(opts, :backfill_id)
    GenStateMachine.start_link(__MODULE__, opts, name: via_tuple(backfill_id))
  end

  def flush_batch(backfill_id, batch_info) when is_binary(backfill_id) do
    GenStateMachine.call(via_tuple(backfill_id), {:flush_batch, batch_info})
  end

  def flush_batch(pid, batch_info) when is_pid(pid) do
    GenStateMachine.call(pid, {:flush_batch, batch_info})
  end

  def discard_batch(backfill_id, batch_id) when is_binary(backfill_id) do
    GenStateMachine.call(via_tuple(backfill_id), {:discard_batch, batch_id})
  end

  def discard_batch(pid, batch_id) when is_pid(pid) do
    GenStateMachine.call(pid, {:discard_batch, batch_id})
  end

  # Convenience function
  def via_tuple(%_{} = consumer) do
    consumer = Repo.preload(consumer, :active_backfill)
    via_tuple(consumer.active_backfill.id)
  end

  def via_tuple(backfill_id) when is_binary(backfill_id) do
    {:via, :syn, {:replication, {__MODULE__, backfill_id}}}
  end

  # Convenience function
  def via_tuple_for_consumer(consumer_id) do
    consumer_id
    |> Consumers.get_consumer!()
    |> via_tuple()
  end

  def child_spec(opts) do
    backfill_id = Keyword.fetch!(opts, :backfill_id)

    %{
      id: {__MODULE__, backfill_id},
      start: {__MODULE__, :start_link, [opts]},
      # Will get restarted by Starter in event of crash
      restart: :temporary,
      type: :worker
    }
  end

  defmodule State do
    @moduledoc false
    use TypedStruct

    alias Sequin.Consumers.Backfill
    alias Sequin.Consumers.SinkConsumer
    alias Sequin.Databases.PostgresDatabase

    typedstruct do
      field :id, String.t()
      field :backfill, Backfill.t()
      field :consumer, SinkConsumer.t()
      field :current_cursor, map() | nil
      field :next_cursor, map() | nil
      field :page_size, integer()
      field :test_pid, pid()
      field :table_oid, integer()
      field :successive_failure_count, integer(), default: 0
      field :max_pending_messages, integer()
      field :count_pending_messages, integer(), default: 0
      field :check_state_timeout, integer()
      field :batch, list() | nil
      field :batch_id, String.t() | nil
      field :batch_lsn, integer()
      field :fetch_slot_lsn, (PostgresDatabase.t(), String.t() -> {:ok, term()} | {:error, term()})
    end
  end

  # Callbacks

  @impl GenStateMachine
  def init(opts) do
    test_pid = Keyword.get(opts, :test_pid)
    maybe_setup_allowances(test_pid)
    backfill = opts |> Keyword.fetch!(:backfill_id) |> Consumers.get_backfill!() |> Repo.preload(:sink_consumer)
    consumer = preload_consumer(backfill.sink_consumer)

    Logger.metadata(consumer_id: consumer.id, backfill_id: backfill.id)

    max_pending_messages =
      Keyword.get(opts, :max_pending_messages, Application.get_env(:sequin, :backfill_max_pending_messages, 1_000_000))

    page_size = Keyword.get(opts, :page_size, 1000)

    state = %State{
      id: backfill.id,
      backfill: backfill,
      consumer: consumer,
      page_size: page_size,
      test_pid: test_pid,
      table_oid: Keyword.fetch!(opts, :table_oid),
      max_pending_messages: max_pending_messages,
      count_pending_messages: 0,
      check_state_timeout: Keyword.get(opts, :check_state_timeout, :timer.seconds(30)),
      fetch_slot_lsn: Keyword.get(opts, :fetch_slot_lsn, &TableReader.fetch_slot_lsn/2)
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

  def handle_event(:internal, :init, :initializing, state) do
    backfill = state.backfill

    cursor = TableReader.cursor(backfill.id)
    cursor = cursor || backfill.initial_min_cursor
    state = %{state | current_cursor: cursor, next_cursor: nil}

    # Add initial count if not set
    if is_nil(backfill.rows_initial_count) do
      case TableReader.fast_count_estimate(database(state), table(state), cursor) do
        {:ok, count} ->
          Consumers.update_backfill(backfill, %{rows_initial_count: count})

        {:error, error} ->
          Logger.error("[TableReaderServer] Failed to get initial count: #{inspect(error)}")
      end
    end

    actions = [check_state_timeout(state.check_state_timeout)]

    {:next_state, :fetch_batch, state, actions}
  end

  def handle_event(:enter, _old_state, :fetch_batch, _state) do
    actions = [{:state_timeout, 0, :fetch_batch}]
    {:keep_state_and_data, actions}
  end

  def handle_event(:state_timeout, :fetch_batch, :fetch_batch, state) do
    include_min = state.current_cursor == initial_min_cursor(state.consumer)
    batch_id = UUID.uuid4()
    Logger.metadata(current_batch_id: batch_id)

    res =
      TableReader.with_watermark(
        database(state),
        state.id,
        batch_id,
        table_oid(state.consumer),
        fn t_conn ->
          TableReader.fetch_batch(
            t_conn,
            table(state),
            state.current_cursor,
            limit: state.page_size,
            include_min: include_min
          )
        end
      )

    case res do
      {:ok, %{rows: [], next_cursor: nil}, _lsn} ->
        Logger.info("[TableReaderServer] Batch returned no records. Table pagination complete.")
        Consumers.table_reader_finished(state.consumer.id)
        TableReader.delete_cursor(state.consumer.active_backfill.id)

        {:stop, :normal}

      {:ok, %{rows: rows, next_cursor: next_cursor}, lsn} ->
        Logger.debug("[TableReaderServer] Batch returned #{length(rows)} records")

        if state.test_pid do
          send(state.test_pid, {__MODULE__, {:batch_fetched, batch_id}})
        end

        state = %{state | batch: rows, batch_id: batch_id, next_cursor: next_cursor, batch_lsn: lsn}
        {:next_state, :await_flush, state}

      {:error, error} ->
        Logger.error("[TableReaderServer] Failed to fetch batch: #{inspect(error)}", error: error)

        state = %{state | successive_failure_count: state.successive_failure_count + 1}
        backoff = Sequin.Time.exponential_backoff(1000, state.successive_failure_count, :timer.minutes(5))

        actions = [
          {:state_timeout, backoff, :retry}
        ]

        {:next_state, :awaiting_retry, state, actions}
    end
  end

  def handle_event(:enter, _old_state, :await_flush, _state) do
    :keep_state_and_data
  end

  def handle_event(:enter, _old_state, :awaiting_retry, _state) do
    :keep_state_and_data
  end

  # Handle the retry after backoff
  def handle_event(:state_timeout, :retry, :awaiting_retry, state) do
    if state.count_pending_messages < state.max_pending_messages do
      {:next_state, :fetch_batch, state}
    else
      {:next_state, {:paused, :max_pending_messages}, state}
    end
  end

  def handle_event({:timeout, :check_state}, _evt, state_name, state) do
    case preload_consumer(state.consumer) do
      nil ->
        Logger.info("[TableReaderServer] Consumer #{state.consumer.id} not found, shutting down")
        {:stop, :normal}

      %SinkConsumer{} = consumer ->
        message_count = Consumers.fast_count_messages_for_consumer(consumer)
        actions = [check_state_timeout(state.check_state_timeout)]
        state = %{state | count_pending_messages: message_count, consumer: consumer}
        current_slot_lsn = fetch_slot_lsn(state)

        cond do
          is_nil(consumer.active_backfill) ->
            Logger.info("[TableReaderServer] No active backfill found, shutting down")
            {:stop, :normal}

          state_name == {:paused, :max_pending_messages} and state.count_pending_messages < state.max_pending_messages ->
            {:next_state, :fetch_batch, state, actions}

          state_name == :await_flush and current_slot_lsn > state.batch_lsn ->
            Logger.warning(
              "[TableReaderServer] Detected stale batch #{state.batch_id}. " <>
                "Batch LSN #{state.batch_lsn} is behind slot LSN #{current_slot_lsn}. Retrying."
            )

            state = %{state | batch: nil, batch_id: nil, next_cursor: nil}
            {:next_state, :fetch_batch, state, actions}

          true ->
            {:keep_state, state, actions}
        end
    end
  end

  def handle_event(:enter, _old_state, {:paused, :max_pending_messages}, state) do
    if state.test_pid do
      send(state.test_pid, {__MODULE__, :paused})
    end

    :keep_state_and_data
  end

  def handle_event(
        {:call, from},
        {:flush_batch, %{batch_id: batch_id} = batch_info},
        _state_name,
        %{batch_id: batch_id} = state
      ) do
    next_state = process_batch(state, batch_info)
    Logger.metadata(current_batch_id: nil)
    next_state = %{next_state | batch_id: nil, batch: nil, next_cursor: nil, current_cursor: state.next_cursor}

    if state.count_pending_messages < state.max_pending_messages do
      {:next_state, :fetch_batch, next_state, [{:reply, from, :ok}]}
    else
      {:next_state, {:paused, :max_pending_messages}, next_state, [{:reply, from, :ok}]}
    end
  end

  def handle_event({:call, from}, {:flush_batch, batch_info}, _state_name, _state) do
    Logger.warning("[TableReaderServer] Received flush_batch call for unknown batch #{batch_info.batch_id}")
    {:keep_state_and_data, [{:reply, from, :ok}]}
  end

  # Add call handler
  @impl GenStateMachine
  def handle_event({:call, from}, {:discard_batch, batch_id}, :await_flush, %State{batch_id: batch_id} = state) do
    Logger.warning("[TableReaderServer] Discarding batch #{state.batch_id} by request")

    state = %{state | batch: nil, batch_id: nil, next_cursor: nil}
    {:next_state, :fetch_batch, state, [{:reply, from, :ok}]}
  end

  def handle_event({:call, from}, {:discard_batch, batch_id}, _state_name, %State{} = state) do
    Logger.info(
      "[TableReaderServer] Ignoring call to discard batch, batch not found: #{batch_id} (current batch: #{state.batch_id})"
    )

    {:keep_state_and_data, [{:reply, from, :ok}]}
  end

  defp check_state_timeout(timeout) do
    {{:timeout, :check_state}, timeout, nil}
  end

  defp replication_slot(%State{consumer: consumer}) do
    consumer.replication_slot
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

  # New private helper function
  defp process_batch(%State{} = state, %{seq: seq, drop_pks: drop_pks}) do
    pk_columns =
      table(state).columns
      |> Enum.filter(& &1.is_pk?)
      # Important - the system expects the PKs to be sorted by attnum
      |> Enum.sort_by(& &1.attnum)
      |> Enum.map(& &1.name)

    batch_by_pk =
      Map.new(state.batch, fn row ->
        pks = Enum.map(pk_columns, &Map.fetch!(row, &1))
        # Important - PKs are passed around as a list of strings
        pks = Enum.map(pks, &to_string/1)
        {pks, row}
      end)

    filtered_batch =
      batch_by_pk
      |> Enum.reject(fn {pks, _row} ->
        MapSet.member?(drop_pks, pks)
      end)
      # Unwrap the map into a list of rows
      |> Enum.map(fn {_pk, row} -> row end)

    map_size_diff = length(state.batch) - length(filtered_batch)

    if map_size_diff > 0 do
      Logger.info("[TableReaderServer] Dropped #{map_size_diff} rows")
    end

    {:ok, _count, consumer} = handle_records(state.consumer, table(state), seq, filtered_batch)

    # Update current_cursor with next_cursor and persist to Redis
    :ok = TableReader.update_cursor(state.consumer.active_backfill.id, state.next_cursor)
    %State{state | consumer: consumer}
  end

  # Message handling
  defp handle_records(consumer, table, seq, records) do
    Logger.info("[TableReaderServer] Handling #{length(records)} record(s)")

    records_by_column_attnum = records_by_column_attnum(table, records)
    total_processed = length(records)

    matching_records = Enum.filter(records_by_column_attnum, &Consumers.matches_record?(consumer, table.oid, &1))

    {:ok, count} =
      case consumer.message_kind do
        :record -> handle_record_messages(consumer, table, seq, matching_records)
        :event -> handle_event_messages(consumer, table, seq, matching_records)
      end

    {:ok, backfill} =
      Consumers.update_backfill(consumer.active_backfill, %{
        rows_processed_count: consumer.active_backfill.rows_processed_count + total_processed,
        rows_ingested_count: consumer.active_backfill.rows_ingested_count + length(matching_records)
      })

    Health.update(consumer, :ingestion, :healthy)
    {:ok, count, %{consumer | active_backfill: backfill}}
  end

  defp handle_record_messages(consumer, table, seq, matching_records) do
    consumer_records =
      Enum.map(matching_records, fn record_attnums_to_values ->
        Sequin.Map.from_ecto(%ConsumerRecord{
          consumer_id: consumer.id,
          seq: seq,
          table_oid: table.oid,
          record_pks: record_pks(table, record_attnums_to_values),
          group_id: generate_group_id(consumer, table, record_attnums_to_values),
          replication_message_trace_id: UUID.uuid4(),
          data: build_record_data(table, consumer, record_attnums_to_values)
        })
      end)

    Consumers.insert_consumer_records(consumer_records)
  end

  defp build_record_data(table, consumer, record_attnums_to_values) do
    Sequin.Map.from_ecto(%ConsumerRecordData{
      record: build_record_payload(table, record_attnums_to_values),
      metadata: %{
        database_name: consumer.replication_slot.postgres_database.name,
        table_name: table.name,
        table_schema: table.schema,
        consumer: %{
          id: consumer.id,
          name: consumer.name,
          inserted_at: consumer.inserted_at,
          updated_at: consumer.updated_at
        },
        commit_timestamp: DateTime.utc_now()
      }
    })
  end

  defp handle_event_messages(consumer, table, seq, matching_records) do
    consumer_events =
      Enum.map(matching_records, fn record_attnums_to_values ->
        Sequin.Map.from_ecto(%ConsumerEvent{
          consumer_id: consumer.id,
          seq: seq,
          # You may need to get this from somewhere
          commit_lsn: 0,
          record_pks: record_pks(table, record_attnums_to_values),
          group_id: generate_group_id(consumer, table, record_attnums_to_values),
          table_oid: table.oid,
          deliver_count: 0,
          replication_message_trace_id: UUID.uuid4(),
          data: build_event_data(table, consumer, record_attnums_to_values)
        })
      end)

    Consumers.insert_consumer_events(consumer_events)
  end

  defp build_event_data(table, consumer, record_attnums_to_values) do
    Sequin.Map.from_ecto(%ConsumerEventData{
      action: :read,
      record: build_record_payload(table, record_attnums_to_values),
      metadata: %{
        database_name: consumer.replication_slot.postgres_database.name,
        table_name: table.name,
        table_schema: table.schema,
        consumer: %{
          id: consumer.id,
          name: consumer.name,
          inserted_at: consumer.inserted_at,
          updated_at: consumer.updated_at
        },
        commit_timestamp: DateTime.utc_now()
      }
    })
  end

  defp build_record_payload(table, record_attnums_to_values) do
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

  defp fetch_slot_lsn(%State{} = state) do
    # Check if we have a stale batch
    case state.fetch_slot_lsn.(database(state), replication_slot(state).slot_name) do
      {:ok, current_lsn} ->
        current_lsn

      {:error, %Error.NotFoundError{}} ->
        raise "[TableReaderServer] Replication slot #{replication_slot(state).slot_name} not found"

      {:error, error} ->
        Logger.error("[TableReaderServer] Failed to fetch slot LSN: #{inspect(error)}")
        # We'll try fetching on the next go-around
        0
    end
  end
end

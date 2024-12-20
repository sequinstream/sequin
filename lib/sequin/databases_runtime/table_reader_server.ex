defmodule Sequin.DatabasesRuntime.TableReaderServer do
  @moduledoc false
  use GenStateMachine, callback_mode: [:handle_event_function, :state_enter]

  alias Ecto.Adapters.SQL.Sandbox
  alias Sequin.Consumers
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerEventData
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.SequenceFilter
  alias Sequin.Databases.PostgresDatabaseTable
  alias Sequin.Databases.Sequence
  alias Sequin.DatabasesRuntime.TableReader
  alias Sequin.Health
  alias Sequin.Repo

  require Logger

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

  # def verify_batch_id(server, batch_id) do
  #   GenStateMachine.call(server, {:verify_batch_id, batch_id})
  # end

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
    consumer =
      consumer_id
      |> Consumers.get_consumer!()
      |> Repo.preload(:active_backfill)

    via_tuple(consumer.active_backfill.id)
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

    typedstruct do
      field :id, String.t()
      field :backfill, Backfill.t()
      field :consumer, SinkConsumer.t()
      field :cursor, map()
      field :page_size, integer()
      field :task_ref, reference()
      field :test_pid, pid()
      field :table_oid, integer()
      field :successive_failure_count, integer(), default: 0
      field :max_pending_messages, integer()
      field :max_pending_batches, integer()
      field :consumer_reload_timeout, integer()
      field :batches, list(), default: []
      field :current_batch_id, String.t()
      field :pending_flushes, list(), default: []
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
      # Default to 100_000 rows
      max_pending_batches: Keyword.get(opts, :max_pending_batches, div(100_000, page_size)),
      consumer_reload_timeout: Keyword.get(opts, :consumer_reload_timeout, :timer.seconds(30))
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
    state = %{state | cursor: cursor}

    # Add initial count if not set
    if is_nil(backfill.rows_initial_count) do
      case TableReader.fast_count_estimate(database(state), table(state), cursor) do
        {:ok, count} ->
          Consumers.update_backfill(backfill, %{rows_initial_count: count})

        {:error, error} ->
          Logger.error("[TableReaderServer] Failed to get initial count: #{inspect(error)}")
      end
    end

    actions = [reload_consumer_timeout(state.consumer_reload_timeout)]

    {:next_state, :fetch_batch, state, actions}
  end

  def handle_event(:enter, _old_state, :fetch_batch, state) do
    include_min = state.cursor == initial_min_cursor(state.consumer)
    current_batch_id = UUID.uuid4()

    task =
      Task.Supervisor.async_nolink(
        Sequin.TaskSupervisor,
        fn ->
          maybe_setup_allowances(state.test_pid)

          TableReader.with_watermark(
            database(state),
            state.id,
            current_batch_id,
            table_oid(state.consumer),
            fn t_conn ->
              TableReader.fetch_batch(
                t_conn,
                table(state),
                state.cursor,
                limit: state.page_size,
                include_min: include_min
              )
            end
          )
        end,
        timeout: 60_000
      )

    {:keep_state, %{state | task_ref: task.ref, current_batch_id: current_batch_id}}
  end

  def handle_event(:info, {ref, {:ok, %{rows: [], next_cursor: nil}}}, :fetch_batch, %State{task_ref: ref} = state) do
    Process.demonitor(ref, [:flush])
    Logger.info("[TableReaderServer] Batch #{state.current_batch_id} returned no records. Table pagination complete.")

    if state.batches == [] do
      Consumers.table_reader_finished(state.consumer.id)
      TableReader.delete_cursor(state.consumer.active_backfill.id)
      {:stop, :normal}
    else
      {:next_state, :done_fetching, state}
    end
  end

  def handle_event(
        :info,
        {ref, {:ok, %{rows: rows, next_cursor: next_cursor}}},
        state_name,
        %State{task_ref: ref} = state
      )
      when state_name in [:fetch_batch, {:paused, :max_pending_messages}] do
    Logger.debug("[TableReaderServer] Batch #{state.current_batch_id} returned #{length(rows)} records")
    Process.demonitor(ref, [:flush])

    if state.test_pid do
      send(state.test_pid, {__MODULE__, {:batch_fetched, state.current_batch_id}})
    end

    batch_data = %{batch: rows, cursor: next_cursor}

    # Check if there's a pending flush for this batch
    case get_next_pending_flush(state, state.current_batch_id) do
      {:ok, pending_flush, state_without_flush} ->
        # Process pending flush immediately
        state = %{
          state_without_flush
          | cursor: next_cursor,
            task_ref: nil,
            successive_failure_count: 0,
            current_batch_id: nil
        }

        {state, _from} = flush_batch(state.current_batch_id, pending_flush, batch_data, state, nil)

        if length(state.batches) < state.max_pending_batches do
          {:repeat_state, state}
        else
          {:next_state, {:paused, :max_pending_batches}, state}
        end

      {:error, :no_pending_flushes} ->
        # Store the batch for later processing
        state = %{
          add_batch(state, state.current_batch_id, batch_data)
          | cursor: next_cursor,
            task_ref: nil,
            successive_failure_count: 0,
            current_batch_id: nil
        }

        if length(state.batches) < state.max_pending_batches do
          {:repeat_state, state}
        else
          {:next_state, {:paused, :max_pending_batches}, state}
        end

      {:error, :wrong_flush_order} ->
        {:stop, :restart, state}
    end
  end

  # Implement retry logic in case of task failure
  def handle_event(:info, {:DOWN, ref, _, _, reason}, state_name, %State{task_ref: ref} = state) do
    Logger.error("[TableReaderServer] Task for #{state_name} failed with reason #{inspect(reason)}")

    state = %{state | task_ref: nil, successive_failure_count: state.successive_failure_count + 1}
    backoff = Sequin.Time.exponential_backoff(1000, state.successive_failure_count, :timer.minutes(5))

    actions = [
      {:state_timeout, backoff, :retry}
    ]

    {:next_state, {:awaiting_retry, state_name}, state, actions}
  end

  def handle_event(:info, {ref, {:error, error}}, state_name, %State{task_ref: ref} = state) do
    Process.demonitor(ref, [:flush])
    Logger.error("[TableReaderServer] Task for #{state_name} failed with reason #{inspect(error)}", error: error)

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

  def handle_event({:timeout, :reload_consumer}, _evt, state_name, state) do
    case preload_consumer(state.consumer) do
      nil ->
        Logger.info("[TableReaderServer] Consumer #{state.consumer.id} not found, shutting down")
        {:stop, :normal}

      consumer ->
        message_count = Consumers.fast_count_messages_for_consumer(consumer)
        actions = [reload_consumer_timeout(state.consumer_reload_timeout)]

        cond do
          is_nil(consumer.active_backfill) ->
            Logger.info("[TableReaderServer] No active backfill found, shutting down")
            {:stop, :normal}

          state_name == :done_fetching ->
            {:keep_state_and_data, actions}

          # If there are too many pending messages, pause the backfill
          message_count > state.max_pending_messages and state_name != {:paused, :max_pending_messages} ->
            if state.test_pid do
              send(state.test_pid, {__MODULE__, :paused})
            end

            Logger.info("[TableReaderServer] Pausing backfill for consumer #{consumer.id} due to many pending messages")
            {:next_state, {:paused, :max_pending_messages}, state, actions}

          message_count > state.max_pending_messages and state_name == {:paused, :max_pending_messages} ->
            {:keep_state_and_data, actions}

          state_name == {:paused, :max_pending_messages} ->
            Logger.info("[TableReaderServer] Resuming backfill for consumer #{consumer.id}")
            {:next_state, :fetch_batch, %{state | consumer: consumer}, actions}

          true ->
            {:keep_state, %{state | consumer: consumer}, actions}
        end
    end
  end

  # Ignore results that come back after we've paused
  def handle_event(:info, {ref, _}, {:paused, :max_pending_messages}, %State{task_ref: ref} = state) do
    Process.demonitor(ref, [:flush])
    {:keep_state, %{state | task_ref: nil}}
  end

  def handle_event(:enter, _old_state, {:paused, :max_pending_messages}, _state) do
    :keep_state_and_data
  end

  def handle_event(:enter, _old_state, {:paused, :max_pending_batches}, _state) do
    :keep_state_and_data
  end

  def handle_event(:enter, _old_state, :done_fetching, _state) do
    :keep_state_and_data
  end

  # def handle_event({:call, from}, {:verify_batch_id, batch_id}, state_name, state) do
  #   if Map.has_key?(state.batches, batch_id) or state.current_batch_id == batch_id do
  #     GenServer.reply(from, :ok)
  #     {:keep_state, state, [{:reply, from, :ok}]}
  #   else
  #     GenServer.reply(from, :error)
  #     {:keep_state, state, [{:reply, from, :error}]}
  #   end
  # end

  def handle_event(
        {:call, from},
        {:flush_batch, %{batch_id: batch_id} = batch_info},
        _state_name,
        %{current_batch_id: batch_id} = state
      ) do
    new_state = add_pending_flush(state, batch_id, batch_info)
    {:keep_state, new_state, [{:reply, from, :ok}]}
  end

  def handle_event({:call, from}, {:flush_batch, %{batch_id: batch_id} = batch_info}, state_name, state) do
    case get_next_batch(state, batch_id) do
      {:ok, batch_data, new_state} ->
        {final_state, _from} = flush_batch(batch_id, batch_info, batch_data, new_state, from)

        case state_name do
          {:paused, :max_pending_batches} ->
            {:next_state, :fetch_batch, final_state, [{:reply, from, :ok}]}

          :done_fetching when final_state.batches == [] ->
            Consumers.table_reader_finished(final_state.consumer.id)
            TableReader.delete_cursor(final_state.consumer.active_backfill.id)
            GenServer.reply(from, :ok)
            {:stop, :normal}

          _ ->
            {:keep_state, final_state, [{:reply, from, :ok}]}
        end

      {:error, :stale_batch} ->
        {:keep_state, state, [{:reply, from, :ok}]}

      {:error, :wrong_batch_order} ->
        {:stop, :restart, state}
    end
  end

  defp reload_consumer_timeout(timeout) do
    {{:timeout, :reload_consumer}, timeout, nil}
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
          replication_message_trace_id: UUID.uuid4()
        })
      end)

    Consumers.insert_consumer_records(consumer_records)
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
      record: build_event_payload(table, record_attnums_to_values),
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

  # New private helper function
  defp flush_batch(batch_id, %{seq: seq, drop_pks: drop_pks}, %{batch: batch, cursor: cursor}, state, from) do
    pk_columns =
      table(state).columns
      |> Enum.filter(& &1.is_pk?)
      |> Enum.map(& &1.name)

    # Make a map of the rows by their primary keys
    filtered_batch =
      batch
      |> Map.new(fn row ->
        {Map.take(row, pk_columns), row}
      end)
      # Then reject any rows that match the drop_pks
      |> Enum.reject(fn {pk_map, _row} ->
        Enum.any?(drop_pks, fn drop_pk -> Map.equal?(pk_map, drop_pk) end)
      end)
      # Then "unwrap" the map into a list of rows
      |> Enum.map(fn {_pk_map, row} -> row end)

    map_size_diff = length(batch) - length(filtered_batch)

    if map_size_diff > 0 do
      Logger.info("[TableReaderServer] Dropped #{map_size_diff} rows from batch #{batch_id}")
    end

    {:ok, _count, consumer} = handle_records(state.consumer, table(state), seq, filtered_batch)
    state = %State{state | consumer: consumer}

    :ok = TableReader.update_cursor(state.consumer.active_backfill.id, cursor)

    {state, from}
  end

  # Add private helper functions for state management
  defp add_batch(state, batch_id, batch_data) do
    Logger.debug("[TableReaderServer] Adding batch #{batch_id}")
    %{state | batches: state.batches ++ [Map.put(batch_data, :batch_id, batch_id)]}
  end

  defp add_pending_flush(state, batch_id, flush_info) do
    Logger.debug("[TableReaderServer] Adding pending flush for batch #{batch_id}")
    %{state | pending_flushes: state.pending_flushes ++ [Map.put(flush_info, :batch_id, batch_id)]}
  end

  defp get_next_batch(state, batch_id) do
    case state.batches do
      [%{batch_id: ^batch_id} = batch_data | rest] ->
        Logger.debug("[TableReaderServer] Processing batch #{batch_id}")
        {:ok, batch_data, %{state | batches: rest}}

      [%{batch_id: other_id} | _] ->
        if Enum.any?(state.batches, &(&1.batch_id == batch_id)) do
          Logger.error("[TableReaderServer] Attempted to process batch #{batch_id} but #{other_id} is next in line")
          {:error, :wrong_batch_order}
        else
          {:error, :stale_batch}
        end

      [] ->
        {:error, :no_batches}
    end
  end

  defp get_next_pending_flush(state, batch_id) do
    case state.pending_flushes do
      [%{batch_id: ^batch_id} = flush_info | rest] ->
        Logger.debug("[TableReaderServer] Processing pending flush for batch #{batch_id}")
        {:ok, flush_info, %{state | pending_flushes: rest}}

      [%{batch_id: other_id} | _] ->
        if Enum.any?(state.pending_flushes, &(&1.batch_id == batch_id)) do
          index = Enum.find_index(state.pending_flushes, &(&1.batch_id == batch_id))

          Logger.error(
            "[TableReaderServer] Attempted to process pending flush #{batch_id} (index #{index}/#{length(state.pending_flushes)}) but #{other_id} is next in line"
          )

          {:error, :wrong_flush_order}
        else
          {:error, :stale_flush}
        end

      [] ->
        {:error, :no_pending_flushes}
    end
  end
end

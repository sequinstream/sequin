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
  alias Sequin.DatabasesRuntime.PageSizeOptimizer
  alias Sequin.DatabasesRuntime.SlotMessageStore
  alias Sequin.DatabasesRuntime.TableReader
  alias Sequin.Error
  alias Sequin.Error.InvariantError
  alias Sequin.Error.ServiceError
  alias Sequin.Health
  alias Sequin.Health.Event
  alias Sequin.Repo

  require Logger

  @callback flush_batch(String.t() | pid(), map()) :: :ok
  @callback discard_batch(String.t() | pid(), String.t()) :: :ok

  @initial_batch_progress_timeout :timer.seconds(10)
  @max_batch_progress_timeout :timer.minutes(1)

  @max_backoff_ms :timer.seconds(3)
  @max_backoff_time :timer.minutes(1)

  defguardp record_messages?(state) when state.consumer.message_kind == :record
  defguardp event_messages?(state) when state.consumer.message_kind == :event

  # Client API

  def start_link(opts \\ []) do
    backfill_id = Keyword.fetch!(opts, :backfill_id)
    GenStateMachine.start_link(__MODULE__, opts, name: via_tuple(backfill_id))
  end

  def flush_batch(backfill_id, batch_info) when is_binary(backfill_id) do
    GenStateMachine.call(via_tuple(backfill_id), {:flush_batch, batch_info})
  catch
    :exit, _ ->
      Logger.warning("[TableReaderServer] Table reader for backfill #{backfill_id} not running, skipping flush")
      :ok
  end

  def flush_batch(pid, batch_info) when is_pid(pid) do
    GenStateMachine.call(pid, {:flush_batch, batch_info})
  catch
    :exit, _ ->
      Logger.warning("[TableReaderServer] Table reader not running, skipping flush")
      :ok
  end

  def discard_batch(backfill_id, batch_id) when is_binary(backfill_id) do
    GenStateMachine.call(via_tuple(backfill_id), {:discard_batch, batch_id})
  catch
    :exit, _ ->
      :ok
  end

  def discard_batch(pid, batch_id) when is_pid(pid) do
    GenStateMachine.call(pid, {:discard_batch, batch_id})
  catch
    :exit, _ ->
      :ok
  end

  # Convenience function
  def via_tuple(%SinkConsumer{} = consumer) do
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

  defmodule Batch do
    @moduledoc false
    use TypedStruct

    alias Sequin.DatabasesRuntime.KeysetCursor

    @type status :: :pending_flush | :flushed

    typedstruct do
      field :messages, list(map())
      field :id, TableReader.batch_id()
      field :size, non_neg_integer()
      # ~LSN around the time the batch was fetched. Approximate, as it doesn't correspond to
      # the watermark messages emitted by this process (see TableReader.with_watermark/5 for explanation.)
      field :appx_lsn, non_neg_integer()
      field :cursor, KeysetCursor.cursor()
      field :status, status(), default: :pending_flush
    end
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
      field :page_size_optimizer, PageSizeOptimizer.t()
      field :test_pid, pid()
      field :table_oid, integer()
      field :successive_failure_count, integer(), default: 0
      field :max_pending_messages, integer()
      field :count_pending_messages, integer(), default: 0
      field :check_state_timeout, integer()
      field :batches, [Batch.t()], default: []
      field :fetch_slot_lsn, (PostgresDatabase.t(), String.t() -> {:ok, term()} | {:error, term()})
      field :fetch_batch, (pid(), PostgresDatabaseTable.t(), map(), Keyword.t() -> {:ok, term()} | {:error, term()})
      field :batch_check_count, integer(), default: 0
      field :page_size_optimizer_mod, module()
    end
  end

  # Callbacks

  @impl GenStateMachine
  def init(opts) do
    test_pid = Keyword.get(opts, :test_pid)
    maybe_setup_allowances(test_pid)

    max_pending_messages =
      Keyword.get(opts, :max_pending_messages, Application.get_env(:sequin, :backfill_max_pending_messages, 1_000_000))

    initial_page_size = Keyword.get(opts, :initial_page_size, 1_000)
    max_timeout_ms = Keyword.get(opts, :max_timeout_ms, :timer.seconds(5))

    page_size_optimizer_mod = Keyword.get(opts, :page_size_optimizer_mod, PageSizeOptimizer)

    state = %State{
      id: Keyword.fetch!(opts, :backfill_id),
      page_size_optimizer: page_size_optimizer_mod.new(initial_page_size, max_timeout_ms),
      page_size_optimizer_mod: page_size_optimizer_mod,
      test_pid: test_pid,
      table_oid: Keyword.fetch!(opts, :table_oid),
      max_pending_messages: max_pending_messages,
      count_pending_messages: 0,
      check_state_timeout: Keyword.get(opts, :check_state_timeout, :timer.seconds(30)),
      fetch_slot_lsn: Keyword.get(opts, :fetch_slot_lsn, &TableReader.fetch_slot_lsn/2),
      fetch_batch: Keyword.get(opts, :fetch_batch, &TableReader.fetch_batch/4)
    }

    actions = [
      {:next_event, :internal, :init}
    ]

    schedule_process_logging(0)
    {:ok, :initializing, state, actions}
  end

  @impl GenStateMachine
  def handle_event(:enter, _old_state, :initializing, _state) do
    :keep_state_and_data
  end

  def handle_event(:internal, :init, :initializing, %State{} = state) do
    backfill = state.id |> Consumers.get_backfill!() |> Repo.preload(:sink_consumer)
    consumer = preload_consumer(backfill.sink_consumer)

    Logger.metadata(consumer_id: consumer.id, backfill_id: backfill.id, account_id: consumer.account_id)

    Logger.info("[TableReaderServer] Started")

    consumer.id
    |> SlotMessageStore.via_tuple()
    |> GenServer.whereis()
    |> Process.monitor()

    :syn.join(:consumers, {:table_reader_batches_changed, consumer.id}, self())

    cursor = TableReader.cursor(backfill.id)
    cursor = cursor || backfill.initial_min_cursor
    state = %{state | backfill: backfill, consumer: consumer, current_cursor: cursor}

    actions = [check_state_timeout(state.check_state_timeout)]

    {:next_state, :fetch_batch, state, actions}
  end

  def handle_event(:enter, _old_state, :fetch_batch, _state) do
    actions = [{:state_timeout, 0, :fetch_batch}]
    {:keep_state_and_data, actions}
  end

  def handle_event(:state_timeout, :fetch_batch, :fetch_batch, %State{} = state) do
    execute_timed(:fetch_batch, fn ->
      include_min = state.current_cursor == initial_min_cursor(state.consumer)
      batch_id = UUID.uuid4()
      Logger.metadata(current_batch_id: batch_id)

      page_size = state.page_size_optimizer_mod.size(state.page_size_optimizer)

      start_time = System.monotonic_time(:millisecond)

      res =
        TableReader.with_watermark(
          database(state),
          state.consumer.replication_slot.id,
          state.id,
          batch_id,
          table_oid(state.consumer),
          fn t_conn ->
            state.fetch_batch.(
              t_conn,
              table(state),
              state.current_cursor,
              limit: page_size,
              include_min: include_min
            )
          end
        )

      # Needs to be >0 for PageSizeOptimizer
      time_ms = max(System.monotonic_time(:millisecond) - start_time, 1)

      case res do
        {:ok, %{rows: [], next_cursor: nil}, _appx_lsn} ->
          Logger.info("[TableReaderServer] Batch returned no records. Table pagination complete.")
          Consumers.table_reader_finished(state.consumer.id)
          TableReader.delete_cursor(state.consumer.active_backfill.id)

          {:stop, :normal}

        {:ok, %{rows: rows, next_cursor: next_cursor}, appx_lsn} ->
          Logger.debug("[TableReaderServer] Batch returned #{length(rows)} records")

          # Record successful timing
          optimizer = state.page_size_optimizer_mod.put_timing(state.page_size_optimizer, page_size, time_ms)
          state = %{state | page_size_optimizer: optimizer}

          if state.test_pid do
            send(state.test_pid, {__MODULE__, {:batch_fetched, batch_id}})
          end

          messages =
            rows
            |> messages_from_rows(state)
            |> filter_messages(state)

          if messages == [] do
            # All rows filtered out
            # Update Redis to the next cursor right away
            :ok = TableReader.update_cursor(state.consumer.active_backfill.id, next_cursor)
            {:repeat_state, %{state | current_cursor: next_cursor}}
          else
            batch = %Batch{
              messages: messages,
              id: batch_id,
              appx_lsn: appx_lsn,
              cursor: state.current_cursor
            }

            state = %{state | batches: [batch], next_cursor: next_cursor}

            {:next_state, :await_flush, state}
          end

        {:error, error} ->
          Logger.error("[TableReaderServer] Failed to fetch batch: #{inspect(error)}", error: error)

          if match?(%ServiceError{service: :postgres, code: :query_timeout}, error) do
            # Record timeout
            optimizer = state.page_size_optimizer_mod.put_timeout(state.page_size_optimizer, page_size)
            state = %{state | page_size_optimizer: optimizer}
            {:repeat_state, state}
          else
            state = %{state | successive_failure_count: state.successive_failure_count + 1}
            backoff = Sequin.Time.exponential_backoff(1000, state.successive_failure_count, :timer.minutes(5))

            actions = [
              {:state_timeout, backoff, :retry}
            ]

            {:next_state, :await_fetch_retry, state, actions}
          end
      end
    end)
  end

  def handle_event(:enter, _old_state, :await_flush, _state) do
    :keep_state_and_data
  end

  def handle_event(:enter, _old_state, :await_fetch_retry, _state) do
    :keep_state_and_data
  end

  # Handle the retry after backoff
  def handle_event(:state_timeout, :retry, :await_fetch_retry, state) do
    if state.count_pending_messages < state.max_pending_messages do
      {:next_state, :fetch_batch, state}
    else
      {:next_state, {:paused, :max_pending_messages}, state}
    end
  end

  def handle_event({:timeout, :check_state}, _evt, state_name, %State{} = state) do
    case preload_consumer(state.consumer) do
      nil ->
        Logger.info("[TableReaderServer] Consumer #{state.consumer.id} not found, shutting down")
        {:stop, :normal}

      %SinkConsumer{} = consumer ->
        {:ok, message_count} = SlotMessageStore.count_messages(consumer.id)
        actions = [check_state_timeout(state.check_state_timeout)]
        state = %{state | count_pending_messages: message_count, consumer: consumer}
        current_slot_lsn = fetch_slot_lsn(state)

        stale_batch = Enum.find(state.batches, fn batch -> current_slot_lsn > batch.appx_lsn end)

        cond do
          is_nil(consumer.active_backfill) ->
            Logger.info("[TableReaderServer] No active backfill found, shutting down")
            {:stop, :normal}

          state_name == {:paused, :max_pending_messages} and state.count_pending_messages < state.max_pending_messages ->
            {:next_state, :fetch_batch, state, actions}

          stale_batch ->
            Logger.warning(
              "[TableReaderServer] Detected stale batch #{stale_batch.id}. " <>
                "Batch LSN #{stale_batch.appx_lsn} is behind slot LSN #{current_slot_lsn}. Stopping."
            )

            {:stop, :stale_batch, state}

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
        {:flush_batch, %{batch_id: batch_id, commit_lsn: commit_lsn} = batch_info},
        _state_name,
        %State{batches: batches} = state
      ) do
    execute_timed(:flush_batch, fn ->
      batch = Enum.find(batches, fn batch -> batch.id == batch_id end)

      cond do
        is_nil(batch) ->
          Logger.warning("[TableReaderServer] Received flush_batch call for unknown batch #{batch_info.batch_id}")
          {:keep_state_and_data, [{:reply, from, :ok}]}

        not is_nil(batch) and batch.status == :pending_flush ->
          messages = drop_messages_by_pk(batch.messages, batch_info.drop_pks)
          batch_size = length(messages)
          batch = %{batch | size: batch_size}

          if batch_size == 0 do
            # Complete the batch immediately
            state = complete_batch(%{state | batches: [batch]})

            if state.count_pending_messages < state.max_pending_messages do
              {:next_state, :fetch_batch, state}
            else
              {:next_state, {:paused, :max_pending_messages}, state}
            end
          else
            messages =
              messages
              |> Stream.with_index()
              |> Stream.map(fn
                {%ConsumerRecord{} = record, idx} ->
                  %{record | commit_lsn: commit_lsn, commit_idx: idx}

                {%ConsumerEvent{} = event, idx} ->
                  %{event | commit_lsn: commit_lsn, commit_idx: idx}
              end)
              |> Enum.to_list()

            case push_messages_with_retry(state, batch.id, messages) do
              :ok ->
                # Clear the messages from memory
                batch = %{batch | messages: nil}
                {:next_state, :await_persist, %{state | batches: [batch]}, [{:reply, from, :ok}]}

              {:error, error} ->
                # TODO: Just discard the batch and try again
                Logger.error("[TableReaderServer] Failed to push batch: #{inspect(error)}", error: error)
                {:stop, :failed_to_push_batch, [{:reply, from, :ok}]}
            end
          end

        batch and batch.status == :flushed ->
          Logger.warning(
            "[TableReaderServer] Received flush_batch call after already flushing batch #{batch_id}. Restarting."
          )

          {:stop, :normal, [{:reply, from, :ok}]}
      end
    end)
  end

  # Add call handler
  @impl GenStateMachine
  def handle_event({:call, from}, {:discard_batch, batch_id}, :await_flush, %State{} = state) do
    {wrapped_batch, remaining_batches} = Enum.split_with(state.batches, fn batch -> batch.id == batch_id end)
    batch = List.first(wrapped_batch)
    batch_ids = Enum.map(state.batches, & &1.id)

    state =
      if is_nil(batch) do
        Logger.warning(
          "[TableReaderServer] Ignoring call to discard batch, batch not found: #{batch_id} (batch_ids: #{inspect(batch_ids)})"
        )

        state
      else
        Logger.warning("[TableReaderServer] Discarding batch #{batch_id} by request")

        %{state | batches: remaining_batches, next_cursor: nil}
      end

    {:next_state, :fetch_batch, state, [{:reply, from, :ok}]}
  end

  def handle_event(:enter, _old_state, :await_persist, state) do
    timeout =
      Sequin.Time.exponential_backoff(
        @initial_batch_progress_timeout,
        state.batch_check_count,
        @max_batch_progress_timeout
      )

    actions = [{:state_timeout, timeout, :check_batch_progress}]
    {:keep_state_and_data, actions}
  end

  def handle_event(:state_timeout, :check_batch_progress, _state_name, %State{} = state) do
    execute_timed(:check_batch_progress, fn ->
      Logger.info("[TableReaderServer] Checking batch progress")
      [batch] = state.batches

      case SlotMessageStore.unpersisted_table_reader_batch_ids(state.consumer.id) do
        {:ok, batch_ids} ->
          if batch.id in batch_ids do
            Logger.info("[TableReaderServer] Batch #{batch.id} is in progress")
            # Increment check count and calculate next timeout
            state = %{state | batch_check_count: state.batch_check_count + 1}

            timeout =
              Sequin.Time.exponential_backoff(
                @initial_batch_progress_timeout,
                state.batch_check_count,
                @max_batch_progress_timeout
              )

            actions = [{:state_timeout, timeout, :check_batch_progress}]
            {:keep_state, state, actions}
          else
            state = complete_batch(state)

            if state.count_pending_messages < state.max_pending_messages do
              {:next_state, :fetch_batch, state}
            else
              {:next_state, {:paused, :max_pending_messages}, state}
            end
          end

        {:error, error} ->
          Logger.error("[TableReaderServer] Batch progress check failed: #{Exception.message(error)}")
          raise error
      end
    end)
  end

  def handle_event(:info, :table_reader_batches_changed, :await_persist, _) do
    actions = [{:state_timeout, 0, :check_batch_progress}]
    {:keep_state_and_data, actions}
  end

  def handle_event(:info, :table_reader_batches_changed, _state_name, _state) do
    # Ignore if not in await_persist state
    :keep_state_and_data
  end

  def handle_event(:info, {:DOWN, _ref, :process, _pid, reason}, state_name, state) do
    Logger.info("[TableReaderServer] Consumer #{state.consumer.id} message store process died, shutting down",
      reason: reason,
      state_name: state_name
    )

    {:stop, :normal}
  end

  def handle_event(:info, :process_logging, _state_name, %State{} = state) do
    info = Process.info(self(), [:memory, :message_queue_len])

    metadata = [
      memory_mb: Float.round(info[:memory] / 1_024 / 1_024, 2),
      message_queue_len: info[:message_queue_len],
      batch_len: length(state.batches),
      batch_statuses: Enum.map(state.batches, & &1.status),
      current_page_size: state.page_size_optimizer_mod.size(state.page_size_optimizer),
      fetch_history: state.page_size_optimizer_mod.history(state.page_size_optimizer)
    ]

    # Get all timing metrics from process dictionary
    timing_metrics =
      Process.get()
      |> Enum.filter(fn {key, _} ->
        key |> to_string() |> String.ends_with?("_total_ms")
      end)
      |> Keyword.new()

    metadata = Keyword.merge(metadata, timing_metrics)

    Logger.info("[TableReaderServer] Process metrics", metadata)

    # Clear timing metrics after logging
    timing_metrics
    |> Keyword.keys()
    |> Enum.each(&clear_counter/1)

    schedule_process_logging()
    {:keep_state, state}
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

  defp messages_from_rows(rows, %State{} = state) when record_messages?(state) do
    table = table(state)
    consumer = state.consumer

    Enum.map(rows, fn row ->
      data = build_record_data(table, consumer, row)
      payload_size_bytes = :erlang.external_size(data)

      # commit_lsn and commit_idx are added later
      %ConsumerRecord{
        consumer_id: consumer.id,
        table_oid: table.oid,
        record_pks: record_pks(table, row),
        group_id: generate_group_id(consumer, table, row),
        replication_message_trace_id: UUID.uuid4(),
        data: data,
        payload_size_bytes: payload_size_bytes
      }
    end)
  end

  defp messages_from_rows(rows, %State{} = state) when event_messages?(state) do
    table = table(state)
    consumer = state.consumer

    Enum.map(rows, fn row ->
      data = build_event_data(table, consumer, row)
      payload_size_bytes = :erlang.external_size(data)

      # commit_lsn and commit_idx are added later
      %ConsumerEvent{
        consumer_id: consumer.id,
        record_pks: record_pks(table, row),
        group_id: generate_group_id(consumer, table, row),
        table_oid: table.oid,
        deliver_count: 0,
        replication_message_trace_id: UUID.uuid4(),
        data: data,
        payload_size_bytes: payload_size_bytes
      }
    end)
  end

  defp filter_messages(messages, %State{} = state) do
    table = table(state)

    Enum.filter(messages, fn message ->
      record = record_by_column_attnum(table, message.data.record)
      Consumers.matches_record?(state.consumer, table.oid, record)
    end)
  end

  defp drop_messages_by_pk(messages, drop_pks) when is_list(messages) and is_struct(drop_pks, MapSet) do
    Enum.reject(messages, fn message ->
      MapSet.member?(drop_pks, message.record_pks)
    end)
  end

  defp push_messages_with_retry(
         %State{} = state,
         batch_id,
         messages,
         first_attempt_at \\ System.monotonic_time(:millisecond),
         attempt \\ 1
       ) do
    elapsed = System.monotonic_time(:millisecond) - first_attempt_at

    case SlotMessageStore.put_table_reader_batch(state.consumer.id, messages, batch_id) do
      :ok ->
        :ok

      {:error, %InvariantError{code: :payload_size_limit_exceeded}} when elapsed < @max_backoff_time ->
        backoff = Sequin.Time.exponential_backoff(50, attempt, @max_backoff_ms)

        Logger.info(
          "[TableReaderServer] Slot message store for consumer #{state.consumer.id} is full. " <>
            "Backing off for #{backoff}ms before retry #{attempt + 1}..."
        )

        Process.sleep(backoff)
        push_messages_with_retry(state, batch_id, messages, first_attempt_at, attempt + 1)

      {:error, error} ->
        # TODO: Just discard the batch and try again
        raise error
    end
  end

  defp build_record_data(table, consumer, row) do
    %ConsumerRecordData{
      action: :read,
      record: build_record_payload(table, row),
      metadata: %ConsumerRecordData.Metadata{
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
    }
  end

  defp build_event_data(table, consumer, row) do
    %ConsumerEventData{
      action: :read,
      record: build_record_payload(table, row),
      metadata: %ConsumerEventData.Metadata{
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
    }
  end

  defp build_record_payload(table, row) do
    Map.new(table.columns, fn column -> {column.name, Map.get(row, column.name)} end)
  end

  defp record_by_column_attnum(%PostgresDatabaseTable{} = table, record) do
    Map.new(table.columns, fn %PostgresDatabaseTable.Column{} = column ->
      {column.attnum, Map.get(record, column.name)}
    end)
  end

  defp initial_min_cursor(consumer) do
    consumer.active_backfill.initial_min_cursor
  end

  defp record_pks(%PostgresDatabaseTable{} = table, row) do
    table.columns
    |> Enum.filter(& &1.is_pk?)
    |> Enum.sort_by(& &1.attnum)
    |> Enum.map(&Map.fetch!(row, &1.name))
    |> Enum.map(&to_string/1)
  end

  defp generate_group_id(consumer, table, row) do
    group_column_attnums = group_column_attnums(consumer)

    if group_column_attnums do
      group_columns =
        Enum.map(group_column_attnums, fn attnum -> Sequin.Enum.find!(table.columns, &(&1.attnum == attnum)) end)

      Enum.map_join(group_columns, ",", fn column ->
        to_string(Map.get(row, column.name))
      end)
    else
      table |> record_pks(row) |> Enum.join(",")
    end
  end

  defp maybe_setup_allowances(nil), do: :ok

  defp maybe_setup_allowances(test_pid) do
    Sandbox.allow(Sequin.Repo, test_pid, self())
    Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
    Mox.allow(Sequin.DatabasesRuntime.PageSizeOptimizerMock, test_pid, self())
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

  defp complete_batch(%State{} = state) do
    [batch] = state.batches
    Logger.info("[TableReaderServer] Batch #{batch.id} is committed")
    # Batch is committed, update cursor and reset state
    :ok = TableReader.update_cursor(state.consumer.active_backfill.id, state.next_cursor)

    state = update_stats(state, batch.size)

    %{
      state
      | batches: [],
        next_cursor: nil,
        current_cursor: state.next_cursor,
        batch_check_count: 0
    }
  end

  defp update_stats(%State{} = state, message_count) do
    {:ok, backfill} =
      Consumers.update_backfill(
        state.consumer.active_backfill,
        %{
          rows_processed_count: state.consumer.active_backfill.rows_processed_count + message_count,
          rows_ingested_count: state.consumer.active_backfill.rows_ingested_count + message_count
        },
        skip_lifecycle: true
      )

    Health.put_event(state.consumer, %Event{slug: :messages_ingested, status: :success})
    %{state | consumer: %{state.consumer | active_backfill: backfill}}
  end

  # Private helper functions for timing metrics

  defp execute_timed(name, fun) do
    {time, result} = :timer.tc(fun)
    # Convert microseconds to milliseconds
    incr_counter(:"#{name}_total_ms", div(time, 1000))
    result
  end

  defp incr_counter(name, amount) do
    current = Process.get(name, 0)
    Process.put(name, current + amount)
  end

  defp clear_counter(name) do
    Process.delete(name)
  end

  defp schedule_process_logging(interval \\ :timer.seconds(30)) do
    Process.send_after(self(), :process_logging, interval)
  end
end

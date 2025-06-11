defmodule Sequin.Runtime.TableReaderServer do
  @moduledoc false
  use GenStateMachine, callback_mode: [:handle_event_function]

  alias Ecto.Adapters.SQL.Sandbox
  alias Sequin.Consumers
  alias Sequin.Consumers.Backfill
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Databases.ConnectionCache
  alias Sequin.Error
  alias Sequin.Error.InvariantError
  alias Sequin.Error.ServiceError
  alias Sequin.EtsMultiset
  alias Sequin.Health
  alias Sequin.Health.Event
  alias Sequin.Repo
  alias Sequin.Runtime.PageSizeOptimizer
  alias Sequin.Runtime.SlotMessageStore
  alias Sequin.Runtime.TableReader

  require Logger

  @type fetch_task :: %{
          ref: reference(),
          batch_id: String.t(),
          page_size: integer(),
          started_at: DateTime.t()
        }

  @callback flush_batch(String.t() | pid(), map()) :: :ok

  # these could be scoped to a replication slot as well
  @callback active_table_oids() :: [integer()]
  @callback pks_seen(table_oid :: integer(), pks :: [any()]) :: :ok

  @max_backoff_ms :timer.seconds(1)
  @max_backoff_time :timer.minutes(1)

  @max_batches 3

  @table_oid_to_backfill_ids_ets_table :table_oid_backfill_ids_multiset

  # Client API

  def start_link(opts \\ []) do
    backfill_id = Keyword.fetch!(opts, :backfill_id)
    GenStateMachine.start_link(__MODULE__, opts, name: via_tuple(backfill_id))
  end

  def setup_table_oid_to_backfill_id_ets_table do
    EtsMultiset.new_named(@table_oid_to_backfill_ids_ets_table, access: :public)
    :ok
  end

  defp add_backfill_id_to_table_oid_to_backfill_id_ets_table(table_oid, backfill_id) do
    EtsMultiset.put(@table_oid_to_backfill_ids_ets_table, table_oid, backfill_id)
    :ok
  end

  def remove_backfill_id_from_table_oid_to_backfill_id_ets_table(table_oid, backfill_id) do
    EtsMultiset.delete(@table_oid_to_backfill_ids_ets_table, table_oid, backfill_id)
    :ok
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

  @doc """
  Drops specific primary keys from all batches in the table reader.
  This is useful when you want to exclude certain records from being processed
  across all current and future batches.

  ## Parameters
    * `backfill_id` - The ID of the backfill
    * `pks` - A list of primary key lists to drop
  """
  def drop_pks(backfill_id, pks) when is_binary(backfill_id) and is_list(pks) do
    GenStateMachine.call(via_tuple(backfill_id), {:drop_pks, pks})
  catch
    :exit, _ ->
      Logger.warning("[TableReaderServer] Table reader for backfill #{backfill_id} not running, skipping drop_pks")
      :ok
  end

  @doc """
  Returns a list of table oids for which there are active backfills.
  """
  def active_table_oids do
    EtsMultiset.keys(@table_oid_to_backfill_ids_ets_table)
  end

  @doc """
  Marks primary keys as seen, removing them from all batches in the ETS multiset.

  This function directly accesses the ETS table without going through the GenServer,
  making it more efficient for high-frequency operations.

  ## Parameters
    * `table_oid` - The table oid
    * `pks` - A list of primary key lists to remove from all batches

  ## Returns
    * `:ok` - Always returns :ok, even if the table doesn't exist (to avoid race conditions)
  """
  @spec pks_seen(integer(), [any()]) :: :ok
  def pks_seen(table_oid, pks) do
    # Get backfills running for this table
    case EtsMultiset.get(@table_oid_to_backfill_ids_ets_table, table_oid) do
      [] ->
        :ok

      backfill_ids ->
        # Create a set of primary keys for efficient multiset diffs
        pks_set = MapSet.new(pks)

        Enum.each(backfill_ids, fn backfill_id ->
          case :ets.whereis(multiset_name(backfill_id)) do
            # TableReaderServer not running, so we don't need to worry about dropping PKs
            :undefined ->
              :ok

            _ ->
              # Get all batch_ids (keys) from the multiset
              batch_ids = EtsMultiset.keys(multiset_name(backfill_id))

              # Remove the primary keys from each batch
              Enum.each(batch_ids, fn batch_id ->
                EtsMultiset.difference(multiset_name(backfill_id), batch_id, pks_set)
              end)
          end
        end)
    end
  end

  def via_tuple(backfill_id) when is_binary(backfill_id) do
    {:via, :syn, {:replication, {__MODULE__, backfill_id}}}
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

    alias Sequin.Runtime.KeysetCursor

    @derive {Inspect, except: [:messages]}
    typedstruct do
      field :messages, list(map())
      field :id, TableReader.batch_id()
      field :size, non_neg_integer()
      # ~LSN around the time the batch was fetched. Approximate, as it doesn't correspond to
      # the watermark messages emitted by this process (see TableReader.with_watermark/5 for explanation.)
      field :appx_lsn, non_neg_integer()
      field :cursor, KeysetCursor.cursor()
    end
  end

  defmodule State do
    @moduledoc false
    use TypedStruct

    alias Sequin.Consumers.Backfill
    alias Sequin.Consumers.SinkConsumer
    alias Sequin.Databases.PostgresDatabase
    alias Sequin.Runtime.TableReaderServer

    typedstruct do
      # Core data
      field :id, String.t()
      field :backfill, Backfill.t()
      field :consumer, SinkConsumer.t()
      field :table_oid, integer()
      field :test_pid, pid()

      # Fetching
      field :cursor, map() | nil
      field :next_cursor, map() | nil
      field :page_size_optimizer, PageSizeOptimizer.t()
      field :successive_failure_count, integer(), default: 0
      field :count_pending_messages, integer(), default: 0
      field :unflushed_batches, [Batch.t()], default: []
      field :flushed_batches, [Batch.t()], default: []
      field :last_fetch_request_at, DateTime.t() | nil
      field :done_fetching, boolean(), default: false
      # batch_ids that were discarded after fetching
      field :ignoreable_batch_ids, MapSet.t(), default: MapSet.new()
      # Multiset to track primary keys by batch_id
      field :pk_multiset, :ets.tid()

      # Mockable fns
      field :fetch_slot_lsn, (PostgresDatabase.t(), String.t() -> {:ok, term()} | {:error, term()})
      field :fetch_batch_pks, function()
      field :fetch_batch, function()

      field :page_size_optimizer_mod, module()

      # Settings
      field :setting_check_state_timeout, integer()
      field :setting_check_sms_timeout, integer()
      field :setting_max_pending_messages, integer()

      # Two-stage fetching tasks
      field :current_id_fetch_task, TableReaderServer.fetch_task() | nil
      field :current_batch_fetch_task, TableReaderServer.fetch_task() | nil
      field :slot_message_store_ref, reference() | nil
      # Tracks the time taken for the ID fetch task to use for page size optimization
      field :last_id_fetch_time_ms, integer() | nil
      # Allow for overwriting the task_supervisor in tests
      # this will allow us to exit the task when the test ends, avoiding
      # flooding our logs with DbConnection errors related to the sandbox ending
      field :task_supervisor, pid() | module()
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
      page_size_optimizer:
        page_size_optimizer_mod.new(
          initial_page_size: initial_page_size,
          max_timeout_ms: max_timeout_ms,
          max_page_size: 40_000
        ),
      page_size_optimizer_mod: page_size_optimizer_mod,
      test_pid: test_pid,
      table_oid: Keyword.fetch!(opts, :table_oid),
      setting_max_pending_messages: max_pending_messages,
      count_pending_messages: 0,
      setting_check_state_timeout: Keyword.get(opts, :check_state_timeout, :timer.seconds(30)),
      setting_check_sms_timeout: Keyword.get(opts, :check_sms_timeout, :timer.seconds(5)),
      fetch_slot_lsn: Keyword.get(opts, :fetch_slot_lsn, &TableReader.fetch_slot_lsn/2),
      fetch_batch_pks: Keyword.get(opts, :fetch_batch_pks, &TableReader.fetch_batch_pks/4),
      fetch_batch: Keyword.get(opts, :fetch_batch, &TableReader.fetch_batch/6),
      task_supervisor: Keyword.get(opts, :task_supervisor, Sequin.TaskSupervisor)
    }

    actions = [
      {:next_event, :internal, :init},
      process_logging_timeout()
    ]

    {:ok, :initializing, state, actions}
  end

  @impl GenStateMachine

  def handle_event(:internal, :init, :initializing, %State{} = state) do
    backfill = state.id |> Consumers.get_backfill!() |> Repo.preload(:sink_consumer)
    consumer = preload_consumer(backfill.sink_consumer)

    Logger.metadata(consumer_id: consumer.id, backfill_id: backfill.id, account_id: consumer.account_id)

    Logger.info("[TableReaderServer] Started")

    # Create ETS tables
    pk_multiset = EtsMultiset.new_named(multiset_name(backfill.id), access: :public)
    add_backfill_id_to_table_oid_to_backfill_id_ets_table(backfill.table_oid, backfill.id)

    slot_message_store_ref =
      consumer.id
      # Monitor just the first partition (there's always at least one) and if any crash they will all restart
      # due to supervisor setting of :one_for_all
      |> SlotMessageStore.via_tuple(0)
      |> GenServer.whereis()
      |> Process.monitor()

    :syn.join(:consumers, {:table_reader_batches_changed, consumer.id}, self())

    cursor = TableReader.cursor(backfill.id)

    cursor =
      if cursor do
        Logger.info("[TableReaderServer] Found cursor for backfill #{backfill.id}", cursor: cursor)
        cursor
      else
        Logger.info("[TableReaderServer] No cursor found for backfill #{backfill.id}. Using initial min cursor.",
          cursor: backfill.initial_min_cursor
        )

        backfill.initial_min_cursor
      end

    state = %{
      state
      | backfill: backfill,
        consumer: consumer,
        cursor: cursor,
        slot_message_store_ref: slot_message_store_ref,
        pk_multiset: pk_multiset
    }

    actions = [
      check_state_timeout(state.setting_check_state_timeout),
      check_sms_timeout(state.setting_check_sms_timeout),
      maybe_fetch_timeout()
    ]

    {:next_state, :running, state, actions}
  end

  def handle_event({:timeout, :maybe_fetch_batch}, _, :running, %State{} = state) do
    if should_fetch?(state) do
      {:keep_state, state, [fetch_batch_timeout()]}
    else
      {:keep_state_and_data, [maybe_fetch_timeout(1)]}
    end
  end

  def handle_event(
        {:timeout, :fetch_batch},
        _,
        :running,
        %State{current_id_fetch_task: nil, current_batch_fetch_task: nil} = state
      ) do
    execute_timed(:fetch_batch, fn ->
      include_min = state.cursor == initial_min_cursor(state.backfill)
      batch_id = UUID.uuid4()
      Logger.metadata(current_batch_id: batch_id)

      # Avoid copying state into the Task by using local variables
      page_size = state.page_size_optimizer_mod.size(state.page_size_optimizer)
      test_pid = state.test_pid
      database = database(state)
      table = table(state)
      cursor = state.cursor
      fetch_batch_pks = state.fetch_batch_pks

      task =
        Task.Supervisor.async_nolink(state.task_supervisor, fn ->
          maybe_setup_allowances(test_pid)

          res =
            with {:ok, conn} <- ConnectionCache.connection(database) do
              fetch_batch_pks.(
                conn,
                table,
                cursor,
                limit: page_size,
                include_min: include_min
              )
            end

          {:task_result, res}
        end)

      state = %{
        state
        | current_id_fetch_task: %{
            ref: task.ref,
            batch_id: batch_id,
            page_size: page_size,
            started_at: Sequin.utc_now()
          }
      }

      {:keep_state, state}
    end)
  end

  def handle_event({:timeout, :fetch_batch}, _, :running, %State{}) do
    # Either ID fetch or batch fetch is in progress
    {:keep_state_and_data, [maybe_fetch_timeout(1)]}
  end

  def handle_event(
        :info,
        {ref, {:task_result, result}},
        :running,
        %State{current_id_fetch_task: %{ref: ref, batch_id: batch_id, page_size: page_size}} = state
      ) do
    include_min = state.cursor == initial_min_cursor(state.backfill)
    Process.demonitor(ref, [:flush])
    now = Sequin.utc_now()
    time_ms = DateTime.diff(now, state.current_id_fetch_task.started_at, :millisecond)

    case result do
      {:ok, %{pks: []}} ->
        # No more rows to fetch
        state = %{state | current_id_fetch_task: nil}

        if state.unflushed_batches == [] and state.flushed_batches == [] do
          Consumers.table_reader_finished(state.backfill)
          TableReader.delete_cursor(state.backfill.id)

          Logger.info("[TableReaderServer] ID fetch returned no records. No batches to flush. Table pagination complete.",
            cursor: state.cursor
          )

          {:stop, :normal}
        else
          Logger.info("[TableReaderServer] ID fetch returned no records. Table pagination complete. Awaiting flush.")
          state = %{state | done_fetching: true, ignoreable_batch_ids: MapSet.put(state.ignoreable_batch_ids, batch_id)}
          {:keep_state, state}
        end

      {:ok, %{pks: primary_keys, next_cursor: next_cursor}} ->
        Logger.debug("[TableReaderServer] ID fetch returned #{length(primary_keys)} primary keys")

        # Store primary keys in the ETS multiset under the batch_id key
        EtsMultiset.union(state.pk_multiset, batch_id, MapSet.new(primary_keys))

        state = %{
          state
          | successive_failure_count: 0,
            current_id_fetch_task: nil,
            last_id_fetch_time_ms: time_ms,
            next_cursor: next_cursor
        }

        # Start the batch fetch task
        test_pid = state.test_pid
        database = database(state)
        table = table(state)
        table_oid = state.backfill.table_oid
        consumer = state.consumer
        backfill = state.backfill
        id = state.id
        slot_id = state.consumer.replication_slot.id
        cursor = state.cursor

        task =
          Task.Supervisor.async_nolink(state.task_supervisor, fn ->
            maybe_setup_allowances(test_pid)

            res =
              TableReader.with_watermark(
                database,
                slot_id,
                id,
                batch_id,
                table_oid,
                fn t_conn ->
                  state.fetch_batch.(
                    t_conn,
                    consumer,
                    backfill,
                    table,
                    cursor,
                    include_min: include_min,
                    limit: page_size
                  )
                end
              )

            {:task_result, res}
          end)

        state = %{
          state
          | current_batch_fetch_task: %{
              ref: task.ref,
              batch_id: batch_id,
              page_size: page_size,
              started_at: Sequin.utc_now()
            }
        }

        {:keep_state, state}

      {:error, error} ->
        timeout_error? = match?(%ServiceError{service: :postgres, code: :query_timeout}, error)
        error_details = if timeout_error?, do: error.details, else: %{}

        Logger.error("[TableReaderServer] Failed to fetch primary keys with fetch_batch_pks: #{inspect(error)}",
          error: error,
          error_details: error_details,
          page_size: page_size,
          query_history: state.page_size_optimizer_mod.history(state.page_size_optimizer)
        )

        state = %{state | current_id_fetch_task: nil}

        cond do
          timeout_error? ->
            # Record timeout
            optimizer = state.page_size_optimizer_mod.put_timeout(state.page_size_optimizer, page_size)
            state = %{state | page_size_optimizer: optimizer, last_id_fetch_time_ms: nil}
            {:keep_state, state, [maybe_fetch_timeout()]}

          is_struct(error, Postgrex.Error) ->
            error = Error.ServiceError.from_postgrex(error)
            Health.put_event(state.consumer, %Event{slug: :backfill_fetch_batch, status: :warning, error: error})
            state = %{state | successive_failure_count: state.successive_failure_count + 1, last_id_fetch_time_ms: nil}
            {:keep_state, state, [maybe_fetch_timeout(1)]}

          true ->
            state = %{state | successive_failure_count: state.successive_failure_count + 1, last_id_fetch_time_ms: nil}
            {:keep_state, state, [maybe_fetch_timeout(1)]}
        end
    end
  end

  def handle_event(
        :info,
        {ref, {:task_result, result}},
        :running,
        %State{current_batch_fetch_task: %{ref: ref, batch_id: batch_id, page_size: page_size}} = state
      ) do
    Process.demonitor(ref, [:flush])
    now = Sequin.utc_now()
    batch_time_ms = DateTime.diff(now, state.current_batch_fetch_task.started_at, :millisecond)

    # Use the slower of the two times (ID fetch or batch fetch) for page size optimization
    # This ensures we optimize for the slowest part of the process, whether it's fetching IDs or fetching the actual data
    # PageSizeOptimizer expects a number >0
    time_ms = batch_time_ms |> max(state.last_id_fetch_time_ms || 0) |> max(1)

    state = %{state | current_batch_fetch_task: nil, last_id_fetch_time_ms: nil}

    case result do
      {:ok, %{messages: []}, _appx_lsn} ->
        # No messages after filtering
        # Record successful timing using the slower of the two operations
        optimizer = state.page_size_optimizer_mod.put_timing(state.page_size_optimizer, page_size, time_ms)

        # Clean up the multiset for this batch since we won't be using it
        EtsMultiset.delete_key(state.pk_multiset, batch_id)

        state = %{
          state
          | page_size_optimizer: optimizer,
            ignoreable_batch_ids: MapSet.put(state.ignoreable_batch_ids, batch_id),
            cursor: state.next_cursor,
            successive_failure_count: 0
        }

        {:keep_state, state, [maybe_fetch_timeout()]}

      {:ok, %{messages: messages}, appx_lsn} ->
        Logger.debug("[TableReaderServer] Batch fetch returned #{length(messages)} messages")

        # Record successful timing using the slower of the two operations
        optimizer = state.page_size_optimizer_mod.put_timing(state.page_size_optimizer, page_size, time_ms)
        state = %{state | page_size_optimizer: optimizer}

        if state.test_pid do
          send(state.test_pid, {__MODULE__, {:batch_fetched, batch_id}})
        end

        batch = %Batch{
          messages: messages,
          id: batch_id,
          appx_lsn: appx_lsn,
          size: length(messages),
          cursor: state.cursor
        }

        state = %{
          state
          | unflushed_batches: state.unflushed_batches ++ [batch],
            # Important that we update to the cursor of the ID fetch, not the follow-up fetch
            cursor: state.next_cursor,
            next_cursor: nil,
            successive_failure_count: 0
        }

        {:keep_state, state, [maybe_fetch_timeout()]}

      {:error, error} ->
        timeout_error = match?(%ServiceError{service: :postgres, code: :query_timeout}, error)
        error_details = if timeout_error, do: error.details, else: %{}

        Logger.error("[TableReaderServer] Failed to fetch batch: #{inspect(error)}",
          error: error,
          error_details: error_details,
          page_size: page_size,
          query_history: state.page_size_optimizer_mod.history(state.page_size_optimizer)
        )

        if timeout_error do
          # Record timeout
          optimizer = state.page_size_optimizer_mod.put_timeout(state.page_size_optimizer, page_size)
          state = %{state | page_size_optimizer: optimizer, last_id_fetch_time_ms: nil}
          {:keep_state, state, [maybe_fetch_timeout()]}
        else
          state = %{state | successive_failure_count: state.successive_failure_count + 1, last_id_fetch_time_ms: nil}
          {:keep_state, state, [maybe_fetch_timeout(1)]}
        end
    end
  end

  def handle_event(
        :info,
        {:DOWN, ref, :process, _pid, reason},
        _state_name,
        %State{current_id_fetch_task: %{ref: ref}} = state
      ) do
    Logger.error("[TableReaderServer] ID fetch task failed", reason: reason)

    state = %{
      state
      | current_id_fetch_task: nil,
        successive_failure_count: state.successive_failure_count + 1
    }

    {:keep_state, state, [maybe_fetch_timeout(1)]}
  end

  def handle_event(
        :info,
        {:DOWN, ref, :process, _pid, reason},
        _state_name,
        %State{current_batch_fetch_task: %{ref: ref}} = state
      ) do
    Logger.error("[TableReaderServer] Batch fetch task failed", reason: reason)

    state = %{
      state
      | current_batch_fetch_task: nil,
        successive_failure_count: state.successive_failure_count + 1
    }

    {:keep_state, state, [maybe_fetch_timeout(1)]}
  end

  def handle_event(:info, {:DOWN, ref, :process, _pid, reason}, state_name, %State{slot_message_store_ref: ref} = state) do
    Logger.info("[TableReaderServer] Consumer #{state.consumer.id} message store process died, shutting down",
      reason: reason,
      state_name: state_name
    )

    {:stop, :normal}
  end

  def handle_event(:info, {ref, {:task_result, _result}}, _state_name, %State{
        current_batch_fetch_task: task1,
        current_id_fetch_task: task2
      }) do
    # This will happen if we e.g. discarded batches while a fetch was in progress
    stale_ref = not match?(%{ref: ^ref}, task1) and not match?(%{ref: ^ref}, task2)

    if stale_ref do
      Process.demonitor(ref, [:flush])
      :keep_state_and_data
    else
      raise ArgumentError,
            "Expected to fall through with a stale ref, instead not handling ref #{ref} with task1 #{inspect(task1)} and task2 #{inspect(task2)}"
    end
  end

  def handle_event({:timeout, :check_state}, _, _state_name, %State{} = state) do
    state = reload_backfill(state)

    case preload_consumer(state.consumer) do
      nil ->
        Logger.info("[TableReaderServer] Consumer #{state.consumer.id} not found, shutting down")
        {:stop, :normal}

      %SinkConsumer{} = consumer ->
        {:ok, message_count} = SlotMessageStore.count_messages(consumer)
        state = %{state | count_pending_messages: message_count, consumer: consumer}
        current_slot_lsn = fetch_slot_lsn(state)

        stale_batch = Enum.find(state.unflushed_batches, fn batch -> current_slot_lsn > batch.appx_lsn end)

        cond do
          state.backfill.state != :active ->
            Logger.info("[TableReaderServer] Backfill #{state.backfill.id} is not active, shutting down",
              backfill_id: state.backfill.id,
              state: state.backfill.state
            )

            {:stop, :normal}

          stale_batch ->
            Logger.warning(
              "[TableReaderServer] Detected stale batch #{stale_batch.id}. " <>
                "Batch LSN #{stale_batch.appx_lsn} is behind slot LSN #{current_slot_lsn}. Stopping."
            )

            {:stop, :stale_batch, state}

          true ->
            actions = [
              check_state_timeout(state.setting_check_state_timeout),
              # Now that count_pending_messages is updated, see if we can fetch right away
              maybe_fetch_timeout()
            ]

            {:keep_state, state, actions}
        end
    end
  end

  def handle_event({:call, from}, {:flush_batch, %{batch_id: batch_id} = batch_info}, _state_name, %State{
        current_batch_fetch_task: %{batch_id: batch_id}
      }) do
    # Race condition: We received a flush_batch call for a batch that's just about to return to us
    # Re-queue this message to the end of our mailbox with a small delay
    Logger.debug(
      "[TableReaderServer] Received flush_batch for batch #{batch_id} that's just about to return, re-queueing"
    )

    Process.send_after(self(), {:requeued_flush_batch, {from, batch_info}}, 1)
    :keep_state_and_data
  end

  # Handle re-queued flush_batch messages
  def handle_event(:info, {:requeued_flush_batch, {from, batch_info}}, state_name, state) do
    handle_event({:call, from}, {:flush_batch, batch_info}, state_name, state)
  end

  def handle_event(
        {:call, from},
        {:flush_batch, %{batch_id: batch_id, commit_lsn: commit_lsn} = batch_info},
        _state_name,
        %State{unflushed_batches: [batch | batches]} = state
      ) do
    unflushed_batch_ids = Enum.map(state.unflushed_batches, & &1.id)
    flushed_batch_ids = Enum.map(state.flushed_batches, & &1.id)
    ignoreable_batch_id? = MapSet.member?(state.ignoreable_batch_ids, batch_id)

    execute_timed(:flush_batch, fn ->
      cond do
        ignoreable_batch_id? ->
          # This is a batch that we discarded right after fetching, for example because it had no
          # messages after filtering.
          ignoreable_batch_ids = MapSet.delete(state.ignoreable_batch_ids, batch_id)
          # Clean up the multiset for this batch
          EtsMultiset.delete_key(state.pk_multiset, batch_id)

          {:keep_state, %{state | ignoreable_batch_ids: ignoreable_batch_ids}, [{:reply, from, :ok}]}

        batch.id != batch_id and batch_id in flushed_batch_ids ->
          Logger.warning("[TableReaderServer] Received flush_batch call for already flushed batch #{batch_id}")
          {:stop, :normal, [{:reply, from, :ok}]}

        batch.id != batch_id ->
          Logger.warning(
            "[TableReaderServer] Received flush_batch call for batch that is unknown, out-of-order #{batch_info.batch_id}",
            unflushed_batch_ids: Enum.join(unflushed_batch_ids, ","),
            flushed_batch_ids: Enum.join(flushed_batch_ids, ",")
          )

          {:keep_state_and_data, [{:reply, from, :ok}]}

        true ->
          Health.put_event(state.consumer, %Event{slug: :messages_ingested, status: :success})

          # Then filter out messages whose PKs are not in the multiset
          messages =
            Enum.filter(batch.messages, fn message ->
              EtsMultiset.value_member?(state.pk_multiset, batch_id, message.record_pks)
            end)

          batch_size = length(messages)

          # Clean up the multiset for this batch
          EtsMultiset.delete_key(state.pk_multiset, batch_id)

          if batch_size == 0 do
            # Complete the batch immediately
            Logger.info("[TableReaderServer] Batch #{batch.id} is committed")
            :ok = TableReader.update_cursor(state.backfill.id, batch.cursor)

            {:keep_state, %{state | unflushed_batches: batches}, [maybe_fetch_timeout(), {:reply, from, :ok}]}
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
                batch = %Batch{batch | messages: nil, size: batch_size}

                {:keep_state,
                 %{
                   state
                   | unflushed_batches: batches,
                     flushed_batches: state.flushed_batches ++ [batch]
                 }, [{:reply, from, :ok}]}

              {:error, error} ->
                # TODO: Just discard the batch and try again
                Logger.error("[TableReaderServer] Failed to push batch: #{inspect(error)}", error: error)
                {:stop, :failed_to_push_batch, [{:reply, from, :ok}]}
            end
          end
      end
    end)
  end

  def handle_event({:call, from}, {:flush_batch, %{batch_id: batch_id, commit_lsn: commit_lsn}}, _state_name, state) do
    if batch_id in state.ignoreable_batch_ids do
      # We'll get this when we discard a batch right after fetching, or if it corresponds to our
      # last fetch request.
      ignoreable_batch_ids = MapSet.delete(state.ignoreable_batch_ids, batch_id)
      # Clean up the multiset for this batch
      EtsMultiset.delete_key(state.pk_multiset, batch_id)

      {:keep_state, %{state | ignoreable_batch_ids: ignoreable_batch_ids}, [{:reply, from, :ok}]}
    else
      Logger.warning(
        "[TableReaderServer] Received a flush, but no unflushed batches in state",
        batch_id: batch_id,
        commit_lsn: commit_lsn
      )

      {:keep_state_and_data, [{:reply, from, :ok}]}
    end
  end

  @doc """
  Handles the drop_pks call to remove specific primary keys from all multisets.
  """
  def handle_event({:call, from}, {:drop_pks, pks}, _state_name, %State{} = state) do
    # Remove the PKs from all batch multisets
    batch_ids = EtsMultiset.keys(state.pk_multiset)

    Enum.each(batch_ids, fn batch_id ->
      EtsMultiset.difference(state.pk_multiset, batch_id, MapSet.new(pks))
    end)

    {:keep_state_and_data, [{:reply, from, :ok}]}
  end

  def handle_event({:timeout, :check_sms}, _, _state_name, %State{} = state) do
    execute_timed(:check_sms, fn ->
      case SlotMessageStore.unpersisted_table_reader_batch_ids(state.consumer) do
        {:ok, unpersisted_batch_ids} ->
          # Find completed batches (those no longer in unpersisted_batch_ids)
          {completed_batches, remaining_batches} =
            Enum.split_with(state.flushed_batches, fn batch ->
              batch.id not in unpersisted_batch_ids
            end)

          completed_batches_size =
            Enum.reduce(completed_batches, 0, fn batch, acc -> acc + batch.size end)

          # Complete each batch in order
          Enum.each(completed_batches, fn batch ->
            Logger.info("[TableReaderServer] Batch #{batch.id} is committed")
            :ok = TableReader.update_cursor(state.backfill.id, batch.cursor)
          end)

          state = maybe_update_backfill(state, completed_batches_size)

          actions = [
            check_sms_timeout(state.setting_check_sms_timeout),
            maybe_fetch_timeout()
          ]

          if state.unflushed_batches == [] and remaining_batches == [] and state.done_fetching do
            Consumers.table_reader_finished(state.backfill)
            TableReader.delete_cursor(state.backfill.id)
            {:stop, :normal}
          else
            {:keep_state, %{state | flushed_batches: remaining_batches}, actions}
          end

        {:error, error} ->
          Logger.error("[TableReaderServer] Batch progress check failed: #{Exception.message(error)}")
          {:keep_state, state, [check_sms_timeout(state.setting_check_sms_timeout)]}
      end
    end)
  end

  def handle_event(:info, :table_reader_batches_changed, _state_name, _state) do
    {:keep_state_and_data, [check_sms_timeout(0)]}
  end

  def handle_event({:timeout, :process_logging}, _, _state_name, %State{} = state) do
    info = Process.info(self(), [:memory, :message_queue_len])

    metadata = [
      memory_mb: Float.round(info[:memory] / 1_024 / 1_024, 2),
      message_queue_len: info[:message_queue_len],
      unflushed_batch_len: length(state.unflushed_batches),
      flushed_batch_len: length(state.flushed_batches),
      current_page_size: state.page_size_optimizer_mod.size(state.page_size_optimizer),
      fetch_history: state.page_size_optimizer_mod.history(state.page_size_optimizer),
      id_fetch_in_progress: not is_nil(state.current_id_fetch_task),
      batch_fetch_in_progress: not is_nil(state.current_batch_fetch_task)
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

    {:keep_state, state, [process_logging_timeout()]}
  end

  defp should_fetch?(%State{} = state) do
    # If we have a last fetch time, ensure we've waited the backoff period
    can_fetch_after_backoff? =
      if state.last_fetch_request_at && state.successive_failure_count > 0 do
        backoff = Sequin.Time.exponential_backoff(1000, state.successive_failure_count, :timer.minutes(5))

        DateTime.diff(Sequin.utc_now(), state.last_fetch_request_at, :millisecond) >= backoff
      else
        true
      end

    not state.done_fetching and
      can_fetch_after_backoff? and
      state.count_pending_messages < state.setting_max_pending_messages and
      length(state.unflushed_batches) + length(state.flushed_batches) < @max_batches and
      is_nil(state.current_id_fetch_task) and
      is_nil(state.current_batch_fetch_task)
  end

  defp check_state_timeout(timeout) do
    {{:timeout, :check_state}, timeout, nil}
  end

  defp check_sms_timeout(timeout) do
    {{:timeout, :check_sms}, timeout, nil}
  end

  defp fetch_batch_timeout do
    {{:timeout, :fetch_batch}, 0, nil}
  end

  defp maybe_fetch_timeout(timeout \\ 0) do
    {{:timeout, :maybe_fetch_batch}, :timer.seconds(timeout), nil}
  end

  defp process_logging_timeout do
    {{:timeout, :process_logging}, :timer.seconds(30), nil}
  end

  defp replication_slot(%State{consumer: consumer}) do
    consumer.replication_slot
  end

  defp database(%State{consumer: consumer}) do
    consumer.replication_slot.postgres_database
  end

  defp table(%State{} = state) do
    database = database(state)
    %{backfill: %Backfill{} = backfill} = state

    db_table = Sequin.Enum.find!(database.tables, &(&1.oid == state.table_oid))
    %{db_table | sort_column_attnum: backfill.sort_column_attnum}
  end

  defp push_messages_with_retry(
         %State{} = state,
         batch_id,
         messages,
         first_attempt_at \\ System.monotonic_time(:millisecond),
         attempt \\ 1
       ) do
    elapsed = System.monotonic_time(:millisecond) - first_attempt_at

    case SlotMessageStore.put_table_reader_batch(state.consumer, messages, batch_id) do
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

  defp initial_min_cursor(%Backfill{} = backfill) do
    backfill.initial_min_cursor
  end

  defp maybe_setup_allowances(nil), do: :ok

  defp maybe_setup_allowances(test_pid) do
    Sandbox.allow(Sequin.Repo, test_pid, self())
    Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
    Mox.allow(Sequin.Runtime.PageSizeOptimizerMock, test_pid, self())
  end

  defp preload_consumer(consumer) do
    Repo.preload(consumer, [:filter, replication_slot: :postgres_database], force: true)
  end

  defp reload_backfill(%State{} = state) do
    %{state | backfill: Repo.reload(state.backfill)}
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

  defp maybe_update_backfill(%State{} = state, 0) do
    state
  end

  defp maybe_update_backfill(%State{} = state, message_count) do
    {:ok, backfill} =
      Consumers.update_backfill(
        state.backfill,
        %{
          rows_processed_count: state.backfill.rows_processed_count + message_count,
          rows_ingested_count: state.backfill.rows_ingested_count + message_count
        },
        skip_lifecycle: true
      )

    %{state | backfill: backfill}
  end

  # Private helper functions for timing metrics

  defp execute_timed(name, fun) do
    {time, result} = :timer.tc(fun, :millisecond)
    incr_counter(:"#{name}_total_ms", time)
    result
  end

  defp incr_counter(name, amount) do
    current = Process.get(name, 0)
    Process.put(name, current + amount)
  end

  defp clear_counter(name) do
    Process.delete(name)
  end

  # Returns the name of the ETS multiset for a given backfill ID
  @doc false
  defp multiset_name(backfill_id) do
    Module.concat(__MODULE__, backfill_id)
  end
end

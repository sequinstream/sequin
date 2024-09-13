defmodule Sequin.ConsumersRuntime.TableProducerServer do
  @moduledoc false
  use GenStateMachine, callback_mode: [:handle_event_function, :state_enter], restart: :transient

  alias Ecto.Adapters.SQL.Sandbox
  alias Sequin.ConsumersRuntime.TableProducer
  alias Sequin.Databases.ConnectionCache
  alias Sequin.Repo

  require Logger

  # Client API

  def start_link(opts \\ []) do
    consumer = Keyword.fetch!(opts, :consumer)
    GenStateMachine.start_link(__MODULE__, opts, name: via_tuple(consumer.id))
  end

  def via_tuple(consumer_id) do
    Sequin.Registry.via_tuple({__MODULE__, consumer_id})
  end

  defmodule State do
    @moduledoc false
    use TypedStruct

    typedstruct do
      field :consumer, String.t()
      field :cursor_max, map()
      field :cursor_min, map()
      field :record_handler_ctx, any()
      field :record_handler_module, module()
      field :page_size, integer()
      field :task_ref, reference()
      field :test_pid, pid()
      field :table_oid, integer()
    end
  end

  # Callbacks

  @impl GenStateMachine
  def init(opts) do
    test_pid = Keyword.get(opts, :test_pid)
    maybe_setup_allowances(test_pid)
    consumer = opts |> Keyword.fetch!(:consumer) |> Repo.preload(replication_slot: :postgres_database)

    state = %State{
      consumer: consumer,
      record_handler_ctx: Keyword.fetch!(opts, :record_handler_ctx),
      record_handler_module: Keyword.fetch!(opts, :record_handler_module),
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

  def handle_event(:internal, :init, :initializing, state) do
    cursor_min = TableProducer.cursor(state.consumer.id, :min)
    state = %{state | cursor_min: cursor_min}
    {:next_state, :query_max_cursor, state}
  end

  def handle_event(:enter, _old_state, :query_max_cursor, state) do
    task =
      Task.Supervisor.async_nolink(
        Sequin.TaskSupervisor,
        fn ->
          maybe_setup_allowances(state.test_pid)

          with {:ok, conn} <- conn(database(state)) do
            TableProducer.fetch_max_cursor(
              conn,
              table(state),
              state.cursor_min,
              state.page_size
            )
          end
        end,
        timeout: 60_000
      )

    {:keep_state, %{state | task_ref: task.ref}}
  end

  def handle_event(:info, {ref, {:ok, nil}}, :query_max_cursor, %State{task_ref: ref}) do
    Process.demonitor(ref, [:flush])
    Logger.info("[TableProducerServer] Max cursor query returned nil. Table pagination complete.")
    # TODO: Clean-up and shut down

    {:stop, :normal}
  end

  def handle_event(:info, {ref, {:ok, result}}, :query_max_cursor, %State{task_ref: ref} = state) do
    Process.demonitor(ref, [:flush])
    :ok = TableProducer.update_cursor(state.consumer.id, :max, result)
    state = %{state | cursor_max: result, task_ref: nil}

    {:next_state, :query_fetch_records, state}
  end

  @page_size_multiplier 3
  def handle_event(:enter, _old_state, :query_fetch_records, state) do
    task =
      Task.Supervisor.async_nolink(
        Sequin.TaskSupervisor,
        fn ->
          maybe_setup_allowances(state.test_pid)

          with {:ok, conn} <- conn(database(state)) do
            TableProducer.fetch_records_in_range(
              conn,
              table(state),
              state.cursor_min,
              state.cursor_max,
              state.page_size * @page_size_multiplier
            )
          end
        end,
        timeout: 60_000
      )

    {:keep_state, %{state | task_ref: task.ref}}
  end

  def handle_event(:info, {ref, {:ok, result}}, :query_fetch_records, %State{task_ref: ref} = state) do
    Process.demonitor(ref, [:flush])
    state = %{state | task_ref: nil}

    if length(result) == state.page_size * @page_size_multiplier do
      if state.test_pid do
        send(state.test_pid, {__MODULE__, :page_limit_reached})
      end

      # This happens if a lot of records were suddenly committed inside of our page. We got back way
      # more results than expected.
      Logger.info("[TableProducerServer] Fetch records result size equals the limit. Resetting process.")
      {:next_state, :query_max_cursor, state}
    else
      apply(state.record_handler_module, :handle_records, [state.record_handler_ctx, result])
      :ok = TableProducer.update_cursor(state.consumer.id, :min, state.cursor_max)
      state = %State{state | cursor_min: state.cursor_max, cursor_max: nil}
      {:next_state, :query_max_cursor, state}
    end
  end

  def handle_event(:info, {:DOWN, ref, _, _, reason}, state_name, %State{task_ref: ref} = state) do
    Logger.error("[TableProducerServer] Task for #{state_name} failed with reason #{inspect(reason)}")
    {:next_state, state_name, %{state | task_ref: nil}}
  end

  defp conn(database) do
    ConnectionCache.connection(database)
  end

  defp database(%State{consumer: consumer}) do
    consumer.replication_slot.postgres_database
  end

  defp table(%State{} = state) do
    database = database(state)
    table = Sequin.Enum.find!(database.tables, &(&1.oid == state.table_oid))
    %{table | sort_column_attnum: Map.fetch!(database.tables_sort_column_attnums, state.table_oid)}
  end

  defp maybe_setup_allowances(nil), do: :ok

  defp maybe_setup_allowances(test_pid) do
    Mox.allow(Sequin.Mocks.ConsumersRuntime.RecordHandlerMock, test_pid, self())
    Sandbox.allow(Sequin.Repo, test_pid, self())
  end
end

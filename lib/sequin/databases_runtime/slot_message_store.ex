defmodule Sequin.DatabasesRuntime.SlotMessageStore do
  @moduledoc """
  A GenServer that manages an in-memory message store for a sink consumer.

  The SlotMessageStore serves as a buffer between the replication slot (SlotProcessor) and SinkConsumers. It:

  - Stores messages in memory to improve performance
  - Periodically flushes messages to Postgres for durability
  - Tracks message delivery status and flush state
  - Helps maintain the WAL low watermark by storing undeliverable messages
  - Provides message lookup and filtering capabilities

  Messages are tagged with update and flush events to track their state. The store
  performs both upsert and delete flush operations to Postgres on a regular basis.

  TODOS:
  - [ ] Trap exits, elegant drain
  """
  use GenServer

  alias Sequin.Consumers
  alias Sequin.Consumers.AcknowledgedMessages
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.DatabasesRuntime.SlotMessageStore.State
  alias Sequin.DatabasesRuntime.TableReader
  alias Sequin.Error
  alias Sequin.Health
  alias Sequin.Health.Event
  alias Sequin.Metrics
  alias Sequin.Tracer.Server, as: TracerServer

  require Logger

  @min_log_time_ms 200

  @trim_allow_consumer_id_allow_list ~w(
    c4192100-f55e-4203-9836-3a00398d2876
    b4558a5c-ef60-4d4e-8950-237ac53efbee
  )

  defguardp event_messages?(state) when state.consumer.message_kind == :event
  defguardp record_messages?(state) when state.consumer.message_kind == :record

  @type consumer_id :: String.t()
  @type ack_id :: String.t()
  @type not_visible_until :: DateTime.t()

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    consumer = opts[:consumer]
    consumer_id = if consumer, do: consumer.id, else: Keyword.fetch!(opts, :consumer_id)
    GenServer.start_link(__MODULE__, opts, id: via_tuple(consumer_id), name: via_tuple(consumer_id))
  end

  @spec via_tuple(consumer_id()) :: {:via, :syn, {:replication, {module(), consumer_id()}}}
  def via_tuple(consumer_id) do
    {:via, :syn, {:replication, {__MODULE__, consumer_id}}}
  end

  @doc """
  Stores new messages in the message store.

  Should raise so SlotProcessor cannot continue if this fails.
  """
  @spec put_messages(consumer_id(), list(ConsumerRecord.t() | ConsumerEvent.t())) :: :ok
  def put_messages(consumer_id, messages) do
    GenServer.call(via_tuple(consumer_id), {:put_messages, messages})
  catch
    :exit, e ->
      error = exit_to_sequin_error(e)
      Logger.error("[SlotMessageStore] Failed to put messages", error: error)
      raise error
  end

  @doc """
  Similar to `put_messages/2` but for a batch of messages that state will track
  as a single unit. Used for TableReaderServer.

  Should raise so TableReaderServer cannot continue if this fails.
  """
  @spec put_table_reader_batch(consumer_id(), list(ConsumerRecord.t() | ConsumerEvent.t()), TableReader.batch_id()) :: :ok
  def put_table_reader_batch(consumer_id, messages, batch_id) do
    GenServer.call(via_tuple(consumer_id), {:put_table_reader_batch, messages, batch_id})
  catch
    :exit, e ->
      error = exit_to_sequin_error(e)
      Logger.error("[SlotMessageStore] Failed to put table reader batch", error: error)
      raise error
  end

  @doc """
  Should raise so TableReaderServer cannot continue if this fails.
  """
  @spec batch_progress(consumer_id(), TableReader.batch_id()) :: {:ok, :completed | :in_progress}
  def batch_progress(consumer_id, batch_id) do
    GenServer.call(via_tuple(consumer_id), {:batch_progress, batch_id})
  catch
    :exit, e ->
      error = exit_to_sequin_error(e)
      Logger.error("[SlotMessageStore] Failed to get batch progress", error: error)
      raise error
  end

  @doc """
  Produces the next batch of deliverable messages, up to the specified count.
  Returns `{:ok, messages}` where messages is a list of deliverable messages.
  """
  @spec produce(consumer_id(), pos_integer()) ::
          {:ok, list(ConsumerRecord.t() | ConsumerEvent.t())} | {:error, Exception.t()}
  def produce(consumer_id, count) do
    GenServer.call(via_tuple(consumer_id), {:produce, count})
  catch
    :exit, e ->
      {:error, exit_to_sequin_error(e)}
  end

  @doc """
  Acknowledges messages as successfully processed using their ack_ids.
  """
  @spec ack(SinkConsumer.t(), list(ack_id())) :: :ok | {:error, Exception.t()}
  def ack(consumer, ack_ids) do
    # Delete from database right away
    Consumers.ack_messages(consumer, ack_ids)

    GenServer.call(via_tuple(consumer.id), {:ack, ack_ids})
  catch
    :exit, e ->
      {:error, exit_to_sequin_error(e)}
  end

  @doc """
  Negative acknowledges messages, making them available for redelivery after
  the specified not_visible_until timestamps.
  """
  @spec nack(consumer_id(), %{ack_id() => not_visible_until()}) :: :ok | {:error, Exception.t()}
  def nack(consumer_id, ack_ids_with_not_visible_until) do
    GenServer.call(via_tuple(consumer_id), {:nack, ack_ids_with_not_visible_until})
  catch
    :exit, e ->
      {:error, exit_to_sequin_error(e)}
  end

  @doc """
  Resets the visibility of a message by its ack_id.
  """
  @spec reset_message_visibility(consumer_id(), String.t()) ::
          {:ok, ConsumerRecord.t() | ConsumerEvent.t()} | {:error, Exception.t()}
  def reset_message_visibility(consumer_id, message_id) do
    GenServer.call(via_tuple(consumer_id), {:reset_message_visibility, message_id})
  catch
    :exit, e ->
      {:error, exit_to_sequin_error(e)}
  end

  @spec reset_all_visibility(consumer_id()) :: :ok | {:error, Exception.t()}
  def reset_all_visibility(consumer_id) do
    GenServer.call(via_tuple(consumer_id), :reset_all_visibility)
  catch
    :exit, e ->
      {:error, exit_to_sequin_error(e)}
  end

  @doc """
  Should raise so SlotProcessor cannot continue if this fails.
  """
  @spec min_unflushed_commit_lsn(consumer_id(), reference()) :: non_neg_integer()
  def min_unflushed_commit_lsn(consumer_id, monitor_ref) do
    GenServer.call(via_tuple(consumer_id), {:min_unflushed_commit_lsn, monitor_ref})
  catch
    :exit, e ->
      error = exit_to_sequin_error(e)
      Logger.error("[SlotMessageStore] Failed to get min unflushed commit lsn", error: error)
      raise error
  end

  @doc """
  Counts the number of messages in the message store.
  """
  @spec count_messages(consumer_id()) :: non_neg_integer() | {:error, Exception.t()}
  def count_messages(consumer_id) do
    GenServer.call(via_tuple(consumer_id), :count_messages)
  catch
    :exit, e ->
      {:error, exit_to_sequin_error(e)}
  end

  @doc """
  Peek at SlotMessageStore state. For use in dev/test only.
  """
  @spec peek(consumer_id()) :: State.t() | {:error, Exception.t()}
  def peek(consumer_id) do
    GenServer.call(via_tuple(consumer_id), :peek)
  catch
    :exit, e ->
      {:error, exit_to_sequin_error(e)}
  end

  @spec peek_messages(consumer_id(), pos_integer()) ::
          {:ok, list(ConsumerRecord.t() | ConsumerEvent.t())} | {:error, Exception.t()}
  def peek_messages(consumer_id, count) do
    GenServer.call(via_tuple(consumer_id), {:peek_messages, count})
  catch
    :exit, e ->
      {:error, exit_to_sequin_error(e)}
  end

  @doc """
  Set the monitor reference for the SlotMessageStore into State

  We use `pid` to call here because we want to confirm the `ref` is generated
  from Process.monitor/1 and we want to double ack that the `pid` is the correct one.
  """
  @spec set_monitor_ref(pid(), reference()) :: :ok
  def set_monitor_ref(pid, ref) do
    GenServer.call(pid, {:set_monitor_ref, ref}, :timer.seconds(120))
  end

  def child_spec(opts) do
    consumer = opts[:consumer]
    consumer_id = if consumer, do: consumer.id, else: Keyword.fetch!(opts, :consumer_id)

    %{
      id: via_tuple(consumer_id),
      start: {__MODULE__, :start_link, [opts]}
    }
  end

  @impl GenServer
  def init(opts) do
    consumer = opts[:consumer]
    consumer_id = if consumer, do: consumer.id, else: Keyword.fetch!(opts, :consumer_id)

    Logger.metadata(consumer_id: consumer_id)

    state = %State{
      consumer: consumer,
      consumer_id: consumer_id,
      flush_batch_size: Keyword.get(opts, :flush_batch_size, 1_000),
      flush_interval: Keyword.get(opts, :flush_interval, :timer.seconds(10)),
      flush_wait_ms: Keyword.get(opts, :flush_wait_ms, :timer.seconds(60)),
      persisted_mode?: Keyword.get(opts, :persisted_mode?, true),
      test_pid: Keyword.get(opts, :test_pid)
    }

    {:ok, state, {:continue, :init}}
  end

  @impl GenServer
  def handle_continue(:init, %State{} = state) do
    # Allow test process to access the database connection
    if state.test_pid do
      Ecto.Adapters.SQL.Sandbox.allow(Sequin.Repo, state.test_pid, self())
      Mox.allow(Sequin.TestSupport.DateTimeMock, state.test_pid, self())
      Mox.allow(Sequin.TestSupport.UUIDMock, state.test_pid, self())
    end

    schedule_process_logging()
    schedule_flush(state)

    case load_from_postgres(state) do
      {:ok, state} -> {:noreply, state}
      {:error, _} -> {:stop, :normal, state}
    end
  end

  @impl GenServer
  def handle_call({:put_messages, messages}, _from, %State{} = state) do
    initial_count = map_size(state.messages)
    {time, state} = :timer.tc(fn -> State.put_messages(state, messages) end)
    new_count = map_size(state.messages)

    if div(time, 1000) > @min_log_time_ms do
      Logger.warning(
        "[SlotMessageStore] Put messages took longer than expected",
        count: new_count - initial_count,
        message_count: new_count,
        duration_ms: div(time, 1000)
      )
    end

    Health.put_event(state.consumer, %Event{slug: :messages_ingested, status: :success})
    :syn.publish(:consumers, {:messages_ingested, state.consumer.id}, :messages_ingested)

    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_call({:put_table_reader_batch, messages, batch_id}, _from, %State{} = state) do
    {time, state} = :timer.tc(fn -> State.put_table_reader_batch(state, messages, batch_id) end)

    if div(time, 1000) > @min_log_time_ms do
      Logger.info(
        "[SlotMessageStore] Put table reader batch",
        count: length(messages),
        duration_ms: div(time, 1000),
        message_count: map_size(state.messages)
      )
    end

    Health.put_event(state.consumer, %Event{slug: :messages_ingested, status: :success})
    :syn.publish(:consumers, {:messages_ingested, state.consumer.id}, :messages_ingested)

    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_call({:batch_progress, batch_id}, _from, state) do
    {:reply, State.batch_progress(state, batch_id), state}
  end

  def handle_call({:produce, count}, _from, %State{} = state) do
    {time, {state, messages}} =
      :timer.tc(fn ->
        deliverable_messages = State.deliverable_messages(state, count)
        {_state, _messages} = State.deliver_messages(state, deliverable_messages)
      end)

    if div(time, 1000) > @min_log_time_ms do
      Logger.warning(
        "[SlotMessageStore] Produce messages took longer than expected",
        count: length(messages),
        duration_ms: div(time, 1000),
        message_count: map_size(state.messages)
      )
    end

    if length(messages) > 0 do
      Health.put_event(state.consumer, %Event{slug: :messages_pending_delivery, status: :success})
    end

    {:reply, {:ok, messages}, state}
  end

  def handle_call({:ack, []}, _from, state) do
    {:reply, {:ok, 0}, state}
  end

  def handle_call({:ack, ack_ids}, _from, state) do
    consumer = state.consumer

    {time, {state, dropped_messages, acked_count}} =
      :timer.tc(fn ->
        State.ack(state, ack_ids)
      end)

    Logger.debug(
      "[SlotMessageStore] Acked messages",
      count: acked_count,
      duration_ms: div(time, 1000),
      message_count: map_size(state.messages)
    )

    Health.put_event(state.consumer, %Event{slug: :messages_delivered, status: :success})
    Metrics.incr_consumer_messages_processed_count(state.consumer, acked_count)
    Metrics.incr_consumer_messages_processed_throughput(state.consumer, acked_count)

    :telemetry.execute(
      [:sequin, :posthog, :event],
      %{event: "consumer_ack"},
      %{
        distinct_id: "00000000-0000-0000-0000-000000000000",
        properties: %{
          consumer_id: consumer.id,
          consumer_name: consumer.name,
          message_count: acked_count,
          message_kind: consumer.message_kind,
          "$groups": %{account: consumer.account_id}
        }
      }
    )

    TracerServer.messages_acked(consumer, ack_ids)

    Enum.each(
      dropped_messages,
      &Sequin.Logs.log_for_consumer_message(
        :info,
        consumer.account_id,
        consumer.id,
        &1.replication_message_trace_id,
        "Message acknowledged"
      )
    )

    AcknowledgedMessages.store_messages(consumer.id, dropped_messages)

    {:reply, {:ok, acked_count}, state}
  end

  def handle_call({:nack, ack_ids_with_not_visible_until}, _from, state) do
    {state, nacked_count} = State.nack(state, ack_ids_with_not_visible_until)
    {:reply, {:ok, nacked_count}, state}
  end

  def handle_call({:reset_message_visibility, ack_id}, _from, state) do
    {state, updated_message} = State.reset_message_visibility(state, ack_id)
    {:reply, {:ok, updated_message}, state}
  end

  def handle_call(:reset_all_visibility, _from, state) do
    {state, _} = State.reset_all_visibility(state)
    {:reply, :ok, state}
  end

  def handle_call({:min_unflushed_commit_lsn, monitor_ref}, _from, state) do
    {:reply, State.min_unflushed_commit_lsn(state, monitor_ref), state}
  end

  def handle_call(:count_messages, _from, state) do
    {:reply, map_size(state.messages), state}
  end

  def handle_call(:peek, _from, state) do
    {:reply, state, state}
  end

  def handle_call({:peek_messages, count}, _from, state) do
    {:reply, State.peek_messages(state, count), state}
  end

  def handle_call({:set_monitor_ref, ref}, _from, state) do
    {:reply, :ok, %{state | slot_processor_monitor_ref: ref}}
  end

  @impl GenServer
  def handle_info(:flush, state) do
    to_flush = State.messages_to_flush(state)
    more? = length(to_flush) == state.flush_batch_size

    {time, state} = :timer.tc(fn -> flush_messages(state, to_flush) end)

    if div(time, 1000) > @min_log_time_ms do
      Logger.warning("[SlotMessageStore] Flushed messages took longer than expected",
        count: length(to_flush),
        message_count: map_size(state.messages),
        duration_ms: div(time, 1000)
      )
    end

    if more? do
      Process.send_after(self(), :flush, 0)
      {:noreply, state}
    else
      Logger.info("[SlotMessageStore] Finished flushing messages")
      schedule_flush(state)
      {:noreply, trim_or_load_messages(state)}
    end
  end

  def handle_info(:process_logging, state) do
    info =
      Process.info(self(), [
        # Total memory used by process in bytes
        :memory,
        # Number of messages in queue
        :message_queue_len
      ])

    Logger.info("[SlotMessageStore] Process metrics",
      memory_mb: Float.round(info[:memory] / 1_024 / 1_024, 2),
      message_queue_len: info[:message_queue_len]
    )

    schedule_process_logging()
    {:noreply, state}
  end

  defp load_from_postgres(%State{persisted_mode?: false} = state), do: {:ok, state}

  defp load_from_postgres(%State{persisted_mode?: true} = state) do
    case Consumers.get_sink_consumer(state.consumer_id) do
      {:ok, consumer} ->
        Logger.metadata(account_id: consumer.account_id)

        Logger.info("[SlotMessageStore] Loading messages...")
        {time, messages} = :timer.tc(fn -> load_messages(consumer) end)
        Logger.info("[SlotMessageStore] Loaded messages", count: map_size(messages), duration_ms: div(time, 1000))

        {:ok, %{state | messages: messages, consumer: consumer}}

      {:error, %Error.NotFoundError{entity: :consumer} = error} ->
        Logger.error("[SlotMessageStore] Consumer not found", consumer_id: state.consumer_id)
        {:error, error}
    end
  end

  defp flush_messages(%State{persisted_mode?: true} = state, messages) when event_messages?(state) do
    {:ok, _count} = Consumers.upsert_consumer_events(messages)
    State.flush_messages(state, messages)
  end

  defp flush_messages(%State{persisted_mode?: true} = state, messages) when record_messages?(state) do
    {:ok, _count} = Consumers.upsert_consumer_records(messages)
    State.flush_messages(state, messages)
  end

  defp trim_or_load_messages(%State{consumer_id: consumer_id} = state)
       when consumer_id not in @trim_allow_consumer_id_allow_list,
       do: state

  defp trim_or_load_messages(%State{persisted_mode?: false} = state), do: state

  defp trim_or_load_messages(%State{persisted_mode?: true} = state) do
    cond do
      map_size(state.messages) > State.max_messages_in_memory() ->
        Logger.info("[SlotMessageStore] Trimming messages")

        messages =
          state.messages
          |> Map.values()
          |> Enum.sort_by(&{&1.commit_lsn, &1.commit_idx})
          |> Enum.take(State.max_messages_in_memory())
          |> Map.new(&{&1.ack_id, &1})

        %{state | messages: messages}

      map_size(state.messages) == State.max_messages_in_memory() ->
        Logger.info("[SlotMessageStore] Already at max messages in memory")
        state

      true ->
        limit = State.max_messages_in_memory() - map_size(state.messages)
        Logger.info("[SlotMessageStore] Loading messages", count: limit)

        messages = load_messages(state.consumer, limit)
        %{state | messages: Map.merge(state.messages, messages)}
    end
  end

  defp schedule_process_logging do
    Process.send_after(self(), :process_logging, :timer.seconds(30))
  end

  defp schedule_flush(%State{persisted_mode?: false}), do: :ok

  defp schedule_flush(%State{persisted_mode?: true} = state) do
    Process.send_after(self(), :flush, state.flush_interval)
  end

  defp load_messages(consumer, limit \\ State.max_messages_in_memory())

  defp load_messages(%SinkConsumer{message_kind: :event, id: id} = consumer, limit) do
    now = DateTime.utc_now()
    params = load_params(consumer, limit)

    id
    |> Consumers.list_consumer_events_for_consumer(params, timeout: :timer.seconds(120))
    |> Enum.map(fn msg -> %{msg | flushed_at: msg.updated_at, dirty: false, ingested_at: now} end)
    |> Map.new(&{&1.ack_id, &1})
  end

  defp load_messages(%SinkConsumer{message_kind: :record, id: id} = consumer, limit) do
    now = DateTime.utc_now()
    params = load_params(consumer, limit)

    id
    |> Consumers.list_consumer_records_for_consumer(params, timeout: :timer.seconds(120))
    |> Enum.map(fn msg -> %{msg | flushed_at: msg.updated_at, dirty: false, ingested_at: now} end)
    |> Map.new(&{&1.ack_id, &1})
  end

  defp load_params(%SinkConsumer{id: id}, limit) when id in @trim_allow_consumer_id_allow_list do
    [limit: limit, order_by: [asc: :commit_lsn, asc: :commit_idx]]
  end

  defp load_params(%SinkConsumer{}, _limit), do: []

  defp exit_to_sequin_error({:noproc, _}) do
    Error.invariant(message: "[SlotMessageStore] exited with :noproc")
  end

  defp exit_to_sequin_error(e) when is_exception(e) do
    Error.invariant(message: "[SlotMessageStore] exited with #{Exception.message(e)}")
  end

  defp exit_to_sequin_error(e) do
    Error.invariant(message: "[SlotMessageStore] exited with #{inspect(e)}")
  end
end

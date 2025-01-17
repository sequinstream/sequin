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
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.DatabasesRuntime.TableReader
  alias Sequin.Error
  alias Sequin.Health
  alias Sequin.Health.Event
  alias Sequin.Metrics

  require Logger

  defguardp event_messages?(state) when state.consumer.message_kind == :event
  defguardp record_messages?(state) when state.consumer.message_kind == :record

  defmodule State do
    @moduledoc false
    use TypedStruct

    alias Sequin.Consumers.ConsumerEvent
    alias Sequin.Consumers.ConsumerRecord

    typedstruct do
      field :consumer, SinkConsumer.t()
      field :consumer_id, String.t()
      field :flush_batch_size, non_neg_integer()
      field :flush_interval, non_neg_integer()
      field :messages, %{[String.t()] => ConsumerRecord.t() | ConsumerEvent.t()}
      # Set to false in tests to disable Postgres reads/writes (ie. load messages on boot and flush messages on interval)
      field :persisted_mode?, boolean(), default: true
      field :slot_processor_monitor_ref, reference() | nil
      field :table_reader_batch_id, String.t() | nil
      field :test_pid, pid() | nil
    end

    def put_messages(%State{} = state, messages) do
      messages =
        messages
        |> intake_messages()
        |> reject_record_deletes(state.consumer.message_kind)
        |> Map.new(&{&1.ack_id, &1})

      %{state | messages: Map.merge(state.messages, messages)}
    end

    def put_table_reader_batch(%State{} = state, messages, batch_id) do
      messages = Enum.map(messages, &%{&1 | table_reader_batch_id: batch_id})
      state = put_messages(state, messages)

      %{state | table_reader_batch_id: batch_id}
    end

    @spec ack(%State{}, list(SinkConsumer.ack_id())) :: {%State{}, non_neg_integer()}
    def ack(%State{} = state, ack_ids) do
      {messages, acked_count} =
        Enum.reduce(ack_ids, {state.messages, 0}, fn ack_id, {acc_msgs, acked_count} ->
          case Map.get(acc_msgs, ack_id) do
            nil ->
              {acc_msgs, acked_count}

            %ConsumerRecord{state: :pending_redelivery} = msg ->
              {Map.replace!(acc_msgs, ack_id, %{
                 msg
                 | state: :available,
                   not_visible_until: nil,
                   dirty: true
               }), acked_count + 1}

            _ ->
              {Map.delete(acc_msgs, ack_id), acked_count + 1}
          end
        end)

      {%{state | messages: messages}, acked_count}
    end

    @spec nack(%State{}, %{SinkConsumer.ack_id() => SinkConsumer.not_visible_until()}) :: {%State{}, non_neg_integer()}
    def nack(%State{} = state, ack_ids_with_not_visible_until) do
      {updated_messages, nacked_count} =
        Enum.reduce(ack_ids_with_not_visible_until, {state.messages, 0}, fn {ack_id, not_visible_until},
                                                                            {acc_msgs, nacked_count} ->
          if msg = Map.get(acc_msgs, ack_id) do
            msg = %{msg | not_visible_until: not_visible_until, state: :available, dirty: true}
            {Map.replace(acc_msgs, ack_id, msg), nacked_count + 1}
          else
            {acc_msgs, nacked_count}
          end
        end)

      {%{state | messages: updated_messages}, nacked_count}
    end

    @spec reset_message_visibility(%State{}, SinkConsumer.ack_id()) :: {%State{}, ConsumerRecord.t() | ConsumerEvent.t()}
    def reset_message_visibility(%State{} = state, ack_id) do
      case Map.get(state.messages, ack_id) do
        nil ->
          {state, nil}

        msg ->
          updated_msg = %{msg | not_visible_until: nil, state: :available, dirty: true}
          {update_messages(state, [updated_msg]), updated_msg}
      end
    end

    @spec reset_all_visibility(%State{}) :: {%State{}, list(ConsumerRecord.t() | ConsumerEvent.t())}
    def reset_all_visibility(%State{} = state) do
      updated_messages =
        state.messages
        |> Stream.map(fn
          {_ack_id, %{not_visible_until: nil, state: :available}} -> nil
          {_ack_id, msg} -> %{msg | not_visible_until: nil, state: :available, dirty: true}
        end)
        |> Enum.filter(& &1)

      {update_messages(state, updated_messages), updated_messages}
    end

    @spec min_unflushed_commit_lsn(%State{}, reference()) :: non_neg_integer() | nil
    def min_unflushed_commit_lsn(%State{slot_processor_monitor_ref: ref1} = state, ref2) do
      if ref1 == ref2 do
        state.messages
        |> Map.values()
        |> Stream.filter(&is_nil(&1.flushed_at))
        |> Enum.min_by(& &1.commit_lsn, fn -> nil end)
        |> case do
          nil -> nil
          %ConsumerRecord{commit_lsn: commit_lsn} -> commit_lsn
          %ConsumerEvent{commit_lsn: commit_lsn} -> commit_lsn
        end
      else
        raise "Monitor ref mismatch. Expected #{inspect(ref1)} but got #{inspect(ref2)}"
      end
    end

    def deliverable_messages(%State{} = state, count) do
      now = DateTime.utc_now()

      undeliverable_group_ids =
        state.messages
        |> Map.values()
        |> Enum.reduce(MapSet.new(), fn msg, acc ->
          if not is_nil(msg.not_visible_until) and DateTime.after?(msg.not_visible_until, now) do
            MapSet.put(acc, msg.group_id)
          else
            acc
          end
        end)

      state.messages
      |> Map.values()
      |> Enum.sort_by(& &1.seq)
      |> Sequin.Enum.take_until(count, fn msg ->
        not MapSet.member?(undeliverable_group_ids, msg.group_id)
      end)
    end

    def deliver_messages(%State{} = state, messages) do
      %SinkConsumer{} = consumer = state.consumer
      now = DateTime.utc_now()
      not_visible_until = DateTime.add(now, consumer.ack_wait_ms, :millisecond)

      messages =
        Enum.map(messages, fn msg ->
          %{
            msg
            | not_visible_until: not_visible_until,
              deliver_count: msg.deliver_count + 1,
              last_delivered_at: now,
              state: :delivered,
              dirty: true
          }
        end)

      # Replace old messages with updated ones in state
      state = update_messages(state, messages)

      {state, messages}
    end

    def messages_to_flush(%State{messages: messages, flush_batch_size: flush_batch_size}) do
      messages
      |> Map.values()
      |> Enum.filter(& &1.dirty)
      |> Enum.sort_by(& &1.flushed_at, &compare_flushed_at/2)
      |> Enum.take(flush_batch_size)
    end

    def flush_messages(%State{persisted_mode?: true} = state, messages) do
      flushed_at = DateTime.utc_now()
      messages = Enum.map(messages, fn msg -> %{msg | flushed_at: flushed_at, dirty: false} end)

      update_messages(state, messages)
    end

    def batch_progress(%State{} = state, batch_id) do
      cond do
        state.table_reader_batch_id == nil ->
          Logger.warning("[SlotMessageStore] No batch in progress")
          {:error, Error.invariant(message: "No batch in progress")}

        state.table_reader_batch_id != batch_id ->
          Logger.warning(
            "[SlotMessageStore] Batch mismatch",
            expected_batch_id: batch_id,
            actual_batch_id: state.table_reader_batch_id
          )

          {:error,
           Error.invariant(message: "Batch mismatch. Expected #{batch_id} but got #{state.table_reader_batch_id}")}

        state.messages |> Map.values() |> Enum.any?(&(is_nil(&1.flushed_at) and &1.table_reader_batch_id == batch_id)) ->
          Logger.debug(
            "[SlotMessageStore] Batch is in progress",
            batch_id: state.table_reader_batch_id
          )

          {:ok, :in_progress}

        true ->
          Logger.debug("[SlotMessageStore] Batch is completed", batch_id: state.table_reader_batch_id)
          {:ok, :completed}
      end
    end

    defp update_messages(%State{} = state, messages) do
      messages = Map.new(messages, &{&1.ack_id, &1})

      %{state | messages: Map.merge(state.messages, messages)}
    end

    defp intake_messages(messages) do
      Enum.map(messages, fn msg -> %{msg | ack_id: Sequin.uuid4(), dirty: true} end)
    end

    defp reject_record_deletes(messages, :record) do
      Enum.reject(messages, fn %ConsumerRecord{deleted: deleted} -> deleted end)
    end

    defp reject_record_deletes(messages, :event) do
      messages
    end

    # Helper function to compare flushed_at values where nil is "smaller" than any DateTime
    # nil is "smaller"
    defp compare_flushed_at(nil, _), do: true
    # anything is "larger" than nil
    defp compare_flushed_at(_, nil), do: false
    defp compare_flushed_at(a, b), do: DateTime.compare(a, b) in [:lt, :eq]
  end

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
    # TODO: We need to respect pending_redelivery on consumer records
    # TODO: We can greatly simplify this call now by just deleting events and records
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

  @doc """
  Set the monitor reference for the SlotMessageStore into State

  We use `pid` to call here because we want to confirm the `ref` is generated
  from Process.monitor/1 and we want to double ack that the `pid` is the correct one.
  """
  @spec set_monitor_ref(pid(), reference()) :: :ok
  def set_monitor_ref(pid, ref) do
    GenServer.call(pid, {:set_monitor_ref, ref})
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

    state = %State{
      consumer: consumer,
      consumer_id: consumer_id,
      flush_batch_size: Keyword.get(opts, :flush_batch_size, 1_000),
      flush_interval: Keyword.get(opts, :flush_interval, :timer.seconds(10)),
      messages: %{},
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
    {time, state} = :timer.tc(fn -> State.put_messages(state, messages) end)

    Logger.debug(
      "[SlotMessageStore] Put messages",
      count: map_size(state.messages),
      time_ms: div(time, 1000)
    )

    Health.put_event(state.consumer, %Event{slug: :messages_ingested, status: :success})
    :syn.publish(:consumers, {:messages_ingested, state.consumer.id}, :messages_ingested)

    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_call({:put_table_reader_batch, messages, batch_id}, _from, %State{} = state) do
    {time, state} = :timer.tc(fn -> State.put_table_reader_batch(state, messages, batch_id) end)

    Logger.debug(
      "[SlotMessageStore] Put table reader batch",
      count: length(messages),
      time_ms: div(time, 1000),
      message_count: map_size(state.messages)
    )

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

    if length(messages) > 0 do
      Logger.debug(
        "[SlotMessageStore] Produced messages",
        count: length(messages),
        time_ms: div(time, 1000),
        message_count: map_size(state.messages)
      )

      Health.put_event(state.consumer, %Event{slug: :messages_pending_delivery, status: :success})
    end

    {:reply, {:ok, messages}, state}
  end

  def handle_call({:ack, []}, _from, state) do
    {:reply, {:ok, 0}, state}
  end

  def handle_call({:ack, ack_ids}, _from, state) do
    {time, {state, acked_count}} =
      :timer.tc(fn ->
        {state, acked_count} = State.ack(state, ack_ids)

        Health.put_event(state.consumer, %Event{slug: :messages_delivered, status: :success})
        Metrics.incr_consumer_messages_processed_count(state.consumer, acked_count)
        Metrics.incr_consumer_messages_processed_throughput(state.consumer, acked_count)

        {state, acked_count}
      end)

    Logger.debug(
      "[SlotMessageStore] Acked messages",
      count: acked_count,
      time_ms: div(time, 1000),
      message_count: map_size(state.messages)
    )

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

  def handle_call({:set_monitor_ref, ref}, _from, state) do
    {:reply, :ok, %{state | slot_processor_monitor_ref: ref}}
  end

  @impl GenServer
  def handle_info(:flush, state) do
    to_flush = State.messages_to_flush(state)
    more? = length(to_flush) == state.flush_batch_size

    state = flush_messages(state, to_flush)
    Logger.debug("[SlotMessageStore] Flushed messages", count: length(to_flush))

    if more? do
      Process.send_after(self(), :flush, 0)
    else
      Logger.info("[SlotMessageStore] Finished flushing messages")
      schedule_flush(state)
    end

    {:noreply, state}
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
        Logger.debug("[SlotMessageStore] Loading messages...")
        {time, messages} = :timer.tc(fn -> load_messages(consumer) end)
        Logger.debug("[SlotMessageStore] Loaded messages", count: map_size(messages), time_ms: div(time, 1000))

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

  defp schedule_process_logging do
    Process.send_after(self(), :process_logging, :timer.seconds(30))
  end

  defp schedule_flush(%State{persisted_mode?: false}), do: :ok

  defp schedule_flush(%State{persisted_mode?: true} = state) do
    Process.send_after(self(), :flush, state.flush_interval)
  end

  defp load_messages(%SinkConsumer{message_kind: :event, id: id}) do
    id
    |> Consumers.list_consumer_events_for_consumer()
    |> Enum.map(fn msg -> %{msg | flushed_at: msg.updated_at, dirty: false} end)
    |> Map.new(&{&1.ack_id, &1})
  end

  defp load_messages(%SinkConsumer{message_kind: :record, id: id}) do
    id
    |> Consumers.list_consumer_records_for_consumer()
    |> Enum.map(fn msg -> %{msg | flushed_at: msg.updated_at, dirty: false} end)
    |> Map.new(&{&1.ack_id, &1})
  end

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

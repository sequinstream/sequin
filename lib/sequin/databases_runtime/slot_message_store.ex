defmodule Sequin.DatabasesRuntime.SlotMessageStore do
  @moduledoc """
  A GenServer that manages an in-memory message buffer for sink consumers.

  The SlotMessageStore serves as a central buffer between the SlotProcessor and ConsumerProcessor. It maintains an in-memory buffer for never-delivered messages for a SinkConsumer.

  Message Lifecycle:
  1. SlotProcessor pushes messages to SlotMessageStore
  2. ConsumerProcessor pulls messages for delivery to consumers
  3. On success/failure, ConsumerProcessor acknowledges messages which removes them from SlotMessageStore buffer
  4. If ConsumerProcessor crashes, SlotMessageStore finds out by changing pid in the next pull

  Backpressure:
  - SlotMessageStore buffers grow if ConsumerProcessors slow down or stop consuming
  - When buffers reach configured limits, SlotMessageStore signals SlotProcessor to pause replication
  - SlotProcessor resumes when buffer space becomes available

  Integration Points:
  - Receives messages from SlotProcessor
  - Serves messages to ConsumerProcessors

  The SlotMessageStore is designed to be ephemeral - if it crashes, SlotProcessor will need to reconnect and replay
  messages. This is handled automatically by the supervision tree.

  Note: Failed message handling is managed by ConsumerProducer, not SlotMessageStore. ConsumerProducer writes failed
  messages to persistent storage and handles redelivery attempts.
  """
  @behaviour Sequin.DatabasesRuntime.SlotMessageStoreBehaviour

  use GenServer

  alias Sequin.Consumers
  alias Sequin.Consumers.AcknowledgedMessages
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.ConsumersRuntime.MessageLedgers
  alias Sequin.DatabasesRuntime.SlotMessageStore.State
  alias Sequin.DatabasesRuntime.SlotMessageStoreBehaviour
  alias Sequin.DatabasesRuntime.TableReader
  alias Sequin.Error
  alias Sequin.Health
  alias Sequin.Health.Event
  alias Sequin.Metrics
  alias Sequin.Tracer.Server, as: TracerServer

  require Logger

  @min_log_time_ms 200

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

  @spec put_messages(consumer_id(), list(ConsumerRecord.t() | ConsumerEvent.t())) :: :ok | {:error, Error.t()}
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
  @impl SlotMessageStoreBehaviour
  def produce(consumer_id, count, producer_pid) do
    GenServer.call(via_tuple(consumer_id), {:produce, count, producer_pid})
  catch
    :exit, e ->
      {:error, exit_to_sequin_error(e)}
  end

  @doc """
  Acknowledges messages as successfully processed using their ack_ids.
  """
  @impl SlotMessageStoreBehaviour
  def messages_succeeded(_consumer_id, []), do: :ok

  def messages_succeeded(consumer_id, ack_ids) do
    GenServer.call(via_tuple(consumer_id), {:messages_succeeded, ack_ids})
  catch
    :exit, e ->
      {:error, exit_to_sequin_error(e)}
  end

  @doc """
  Acknowledges messages as already succeeded, likely due to idempotency checks.
  """
  @impl SlotMessageStoreBehaviour
  def messages_already_succeeded(_consumer_id, []), do: :ok

  def messages_already_succeeded(consumer_id, ack_ids) do
    GenServer.call(via_tuple(consumer_id), {:messages_already_succeeded, ack_ids})
  catch
    :exit, e ->
      {:error, exit_to_sequin_error(e)}
  end

  @impl SlotMessageStoreBehaviour
  def messages_failed(_consumer_id, []), do: :ok

  def messages_failed(consumer_id, message_metadatas) do
    GenServer.call(via_tuple(consumer_id), {:messages_failed, message_metadatas})
  catch
    :exit, e ->
      {:error, exit_to_sequin_error(e)}
  end

  def nack_stale_produced_messages(consumer_id) do
    GenServer.call(via_tuple(consumer_id), :nack_stale_produced_messages)
  catch
    :exit, e ->
      {:error, exit_to_sequin_error(e)}
  end

  def reset_message_visibilities(consumer_id, ack_ids) do
    GenServer.call(via_tuple(consumer_id), {:reset_message_visibilities, ack_ids})
  catch
    :exit, e ->
      {:error, exit_to_sequin_error(e)}
  end

  def reset_all_message_visibilities(consumer_id) do
    GenServer.call(via_tuple(consumer_id), :reset_all_message_visibilities)
  catch
    :exit, e ->
      {:error, exit_to_sequin_error(e)}
  end

  @doc """
  Should raise so SlotProcessor cannot continue if this fails.
  """
  @spec min_unpersisted_wal_cursor(consumer_id(), reference()) :: non_neg_integer()
  def min_unpersisted_wal_cursor(consumer_id, monitor_ref) do
    GenServer.call(via_tuple(consumer_id), {:min_unpersisted_wal_cursor, monitor_ref})
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

    consumer =
      if state.consumer do
        state.consumer
      else
        Consumers.get_sink_consumer!(state.consumer_id)
      end

    Logger.metadata(account_id: consumer.account_id, replication_id: consumer.replication_slot_id)

    persisted_messages =
      consumer
      |> Consumers.list_consumer_messages_for_consumer()
      |> Enum.map(fn msg ->
        %{msg | payload_size_bytes: :erlang.external_size(msg.data)}
      end)

    {:ok, state} = State.put_persisted_messages(%{state | consumer: consumer}, persisted_messages)

    schedule_process_logging()
    {:noreply, state}
  end

  @impl GenServer
  def handle_call({:put_messages, messages}, _from, %State{} = state) do
    {to_persist, to_put} = Enum.split_with(messages, &State.is_message_group_persisted?(state, &1.group_id))

    with {:ok, state} <- State.put_messages(state, to_put),
         :ok <- upsert_messages(state, to_persist),
         {:ok, state} <- State.put_persisted_messages(state, to_persist) do
      Health.put_event(state.consumer, %Event{slug: :messages_ingested, status: :success})
      :syn.publish(:consumers, {:messages_ingested, state.consumer.id}, :messages_ingested)

      {:reply, :ok, state}
    else
      {:error, error} ->
        Health.put_event(state.consumer, %Event{slug: :messages_ingested, status: :fail, error: error})
        {:reply, {:error, error}, state}
    end
  end

  @impl GenServer
  def handle_call({:put_table_reader_batch, messages, batch_id}, _from, %State{} = state) do
    {to_persist, to_put} = Enum.split_with(messages, &State.is_message_group_persisted?(state, &1.group_id))

    with {:ok, state} <- State.put_table_reader_batch(state, to_put, batch_id),
         :ok <- upsert_messages(state, to_persist),
         {:ok, state} <- State.put_persisted_messages(state, to_persist) do
      Health.put_event(state.consumer, %Event{slug: :messages_ingested, status: :success})
      :syn.publish(:consumers, {:messages_ingested, state.consumer.id}, :messages_ingested)

      {:reply, :ok, state}
    else
      error ->
        {:reply, error, state}
    end
  end

  @impl GenServer
  def handle_call({:batch_progress, batch_id}, _from, state) do
    {:reply, State.batch_progress(state, batch_id), state}
  end

  def handle_call({:produce, count, producer_pid}, from, %State{last_consumer_producer_pid: last_producer_pid} = state)
      when not is_nil(last_producer_pid) and last_producer_pid != producer_pid do
    # Producer has changed, so we need to reset the state
    {state, _count} = State.nack_produced_messages(state)
    handle_call({:produce, count, producer_pid}, from, %{state | last_consumer_producer_pid: producer_pid})
  end

  def handle_call({:produce, count, producer_pid}, _from, %State{} = state) do
    {time, {messages, state}} =
      :timer.tc(fn ->
        {_messages, _state} = State.produce_messages(state, count)
      end)

    messages
    |> Enum.map(&MessageLedgers.wal_cursor_from_message/1)
    |> then(&MessageLedgers.wal_cursors_reached_checkpoint(state.consumer_id, "slot_message_store.produce", &1))

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

    {:reply, {:ok, messages}, %{state | last_consumer_producer_pid: producer_pid}}
  end

  def handle_call({:messages_succeeded, []}, _from, state) do
    {:reply, {:ok, 0}, state}
  end

  def handle_call({:messages_succeeded, ack_ids}, _from, state) do
    consumer = state.consumer

    persisted_messages_to_drop =
      state
      |> State.peek_messages(ack_ids)
      |> Enum.filter(&State.is_message_persisted?(state, &1))

    {dropped_messages, state} = State.pop_messages(state, ack_ids)

    count = length(dropped_messages)

    :ok = delete_messages(state, Enum.map(persisted_messages_to_drop, & &1.ack_id))

    Health.put_event(state.consumer, %Event{slug: :messages_delivered, status: :success})
    Metrics.incr_consumer_messages_processed_count(state.consumer, count)
    Metrics.incr_consumer_messages_processed_throughput(state.consumer, count)

    :telemetry.execute(
      [:sequin, :posthog, :event],
      %{event: "consumer_ack"},
      %{
        distinct_id: "00000000-0000-0000-0000-000000000000",
        properties: %{
          consumer_id: consumer.id,
          consumer_name: consumer.name,
          message_count: count,
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

    {:reply, {:ok, count}, state}
  end

  def handle_call({:messages_already_succeeded, []}, _from, state) do
    {:reply, {:ok, 0}, state}
  end

  def handle_call({:messages_already_succeeded, ack_ids}, _from, state) do
    persisted_messages_to_drop =
      state
      |> State.peek_messages(ack_ids)
      |> Enum.filter(&State.is_message_persisted?(state, &1))

    {dropped_messages, state} = State.pop_messages(state, ack_ids)

    count = length(dropped_messages)

    :ok = delete_messages(state, Enum.map(persisted_messages_to_drop, & &1.ack_id))

    {:reply, {:ok, count}, state}
  end

  def handle_call({:messages_failed, message_metadatas}, _from, state) do
    message_metas_by_ack_id = Map.new(message_metadatas, &{&1.ack_id, &1})
    {messages, state} = State.pop_messages(state, Map.keys(message_metas_by_ack_id))

    messages =
      Enum.map(messages, fn msg ->
        meta = Map.get(message_metas_by_ack_id, msg.ack_id)

        %{
          msg
          | deliver_count: meta.deliver_count,
            last_delivered_at: meta.last_delivered_at,
            not_visible_until: meta.not_visible_until
        }
      end)

    with {:ok, state} <- State.put_persisted_messages(state, messages),
         {newly_blocked_messages, state} <- State.pop_blocked_messages(state),
         # Now put the blocked messages into persisted_messages
         {:ok, state} <- State.put_persisted_messages(state, newly_blocked_messages),
         :ok <- upsert_messages(state, messages ++ newly_blocked_messages) do
      {:reply, :ok, state}
    else
      error ->
        {:reply, error, state}
    end
  end

  def handle_call(:nack_stale_produced_messages, _from, state) do
    {:reply, :ok, State.nack_stale_produced_messages(state)}
  end

  def handle_call({:reset_message_visibilities, ack_ids}, _from, state) do
    :syn.publish(:consumers, {:messages_ingested, state.consumer.id}, :messages_ingested)
    {:reply, :ok, State.reset_message_visibilities(state, ack_ids)}
  end

  def handle_call(:reset_all_message_visibilities, _from, state) do
    :syn.publish(:consumers, {:messages_ingested, state.consumer.id}, :messages_ingested)
    {:reply, :ok, State.reset_all_message_visibilities(state)}
  end

  def handle_call({:min_unpersisted_wal_cursor, monitor_ref}, _from, state) do
    if monitor_ref == state.slot_processor_monitor_ref do
      {:reply, State.min_unpersisted_wal_cursor(state), state}
    else
      raise "Monitor ref mismatch. Expected #{inspect(state.slot_processor_monitor_ref)} but got #{inspect(monitor_ref)}"
    end
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

  defp schedule_process_logging do
    Process.send_after(self(), :process_logging, :timer.seconds(30))
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

  defp upsert_messages(%State{}, []), do: :ok

  defp upsert_messages(%State{} = state, messages) do
    {:ok, _count} = Consumers.upsert_consumer_messages(state.consumer, messages)
    :ok
  end

  defp delete_messages(%State{}, []), do: :ok

  defp delete_messages(%State{} = state, messages) do
    {:ok, _count} = Consumers.ack_messages(state.consumer, messages)
    :ok
  end
end

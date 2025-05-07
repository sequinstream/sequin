defmodule Sequin.Runtime.SlotMessageStore do
  @moduledoc """
  A GenServer that manages an in-memory message buffer for sink consumers.

  The SlotMessageStore serves as a central buffer between the SlotProcessorServer and ConsumerProcessor. It maintains an in-memory buffer for never-delivered messages for a SinkConsumer.

  Message Lifecycle:
  1. SlotProcessorServer pushes messages to SlotMessageStore
  2. ConsumerProcessor pulls messages for delivery to consumers
  3. On success/failure, ConsumerProcessor acknowledges messages which removes them from SlotMessageStore buffer
  4. If ConsumerProcessor crashes, SlotMessageStore finds out by changing pid in the next pull

  Backpressure:
  - SlotMessageStore buffers grow if ConsumerProcessors slow down or stop consuming
  - When buffers reach configured limits, SlotMessageStore signals SlotProcessorServer to pause replication
  - SlotProcessorServer resumes when buffer space becomes available

  Integration Points:
  - Receives messages from SlotProcessorServer
  - Serves messages to ConsumerProcessors

  The SlotMessageStore is designed to be ephemeral - if it crashes, SlotProcessorServer will need to reconnect and replay
  messages. This is handled automatically by the supervision tree.

  Note: Failed message handling is managed by ConsumerProducer, not SlotMessageStore. ConsumerProducer writes failed
  messages to persistent storage and handles redelivery attempts.
  """
  @behaviour Sequin.Runtime.SlotMessageStoreBehaviour

  use GenServer

  alias Sequin.Consumers
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Error
  alias Sequin.Health
  alias Sequin.Health.Event
  alias Sequin.Replication
  alias Sequin.Runtime.MessageLedgers
  alias Sequin.Runtime.SlotMessageStore.State
  alias Sequin.Runtime.SlotMessageStoreBehaviour
  alias Sequin.Runtime.TableReader

  require Logger

  @min_log_time_ms 200

  @type consumer_id :: String.t()
  @type ack_id :: String.t()
  @type not_visible_until :: DateTime.t()

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    consumer = opts[:consumer]
    consumer_id = if consumer, do: consumer.id, else: Keyword.fetch!(opts, :consumer_id)
    partition = Keyword.fetch!(opts, :partition)
    GenServer.start_link(__MODULE__, opts, id: via_tuple(consumer_id, partition), name: via_tuple(consumer_id, partition))
  end

  @spec via_tuple(consumer_id(), non_neg_integer()) ::
          {:via, :syn, {:replication, {module(), {consumer_id(), non_neg_integer()}}}}
  def via_tuple(consumer_id, partition) when is_integer(partition) do
    {:via, :syn, {:replication, {__MODULE__, {consumer_id, partition}}}}
  end

  @doc """
  Stores new messages in the message store.

  Should raise so SlotProcessorServer cannot continue if this fails.
  """

  @spec put_messages(SinkConsumer.t(), list(ConsumerRecord.t() | ConsumerEvent.t())) :: :ok | {:error, Error.t()}
  def put_messages(consumer, messages) do
    messages
    |> Enum.group_by(&message_partition(&1, consumer.partition_count))
    |> Sequin.Enum.reduce_while_ok(fn {partition, messages} ->
      GenServer.call(via_tuple(consumer.id, partition), {:put_messages, messages})
    end)
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
  @spec put_table_reader_batch(SinkConsumer.t(), list(ConsumerRecord.t() | ConsumerEvent.t()), TableReader.batch_id()) ::
          :ok | {:error, Error.t()}
  def put_table_reader_batch(consumer, messages, batch_id) do
    messages
    |> Enum.group_by(&message_partition(&1, consumer.partition_count))
    |> Sequin.Enum.reduce_while_ok(fn {partition, messages} ->
      GenServer.call(via_tuple(consumer.id, partition), {:put_table_reader_batch, messages, batch_id})
    end)
  catch
    :exit, e ->
      error = exit_to_sequin_error(e)
      Logger.error("[SlotMessageStore] Failed to put table reader batch", error: error)
      raise error
  end

  @doc """
  Should raise so TableReaderServer cannot continue if this fails.
  """
  @spec unpersisted_table_reader_batch_ids(SinkConsumer.t()) :: {:ok, [TableReader.batch_id()]}
  def unpersisted_table_reader_batch_ids(consumer) do
    consumer
    |> partitions()
    |> Enum.reduce_while({:ok, []}, fn partition, {:ok, acc_batch_ids} ->
      case GenServer.call(via_tuple(consumer.id, partition), :unpersisted_table_reader_batch_ids) do
        {:ok, ids} -> {:cont, {:ok, Enum.uniq(acc_batch_ids ++ ids)}}
        error -> {:halt, error}
      end
    end)
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
  def produce(consumer, count, producer_pid) do
    consumer
    |> partitions()
    |> Enum.shuffle()
    |> Enum.reduce_while({:ok, []}, fn partition, {:ok, acc_messages} ->
      case GenServer.call(via_tuple(consumer.id, partition), {:produce, count - length(acc_messages), producer_pid}) do
        {:ok, messages} ->
          messages = messages ++ acc_messages

          if length(messages) == count do
            {:halt, {:ok, messages}}
          else
            {:cont, {:ok, messages}}
          end

        error ->
          {:halt, error}
      end
    end)
  catch
    :exit, e ->
      {:error, exit_to_sequin_error(e)}
  end

  @doc """
  Acknowledges messages as successfully processed using their ack_ids.
  """
  @impl SlotMessageStoreBehaviour
  def messages_succeeded_returning_messages(_consumer, []), do: {:ok, []}

  def messages_succeeded_returning_messages(consumer, ack_ids) do
    consumer
    |> partitions()
    |> Enum.reduce_while({:ok, []}, fn partition, {:ok, acc_messages} ->
      case GenServer.call(via_tuple(consumer.id, partition), {:messages_succeeded, ack_ids, true}) do
        {:ok, messages} -> {:cont, {:ok, acc_messages ++ messages}}
        error -> {:halt, error}
      end
    end)
  catch
    :exit, e ->
      {:error, exit_to_sequin_error(e)}
  end

  @impl SlotMessageStoreBehaviour
  def messages_succeeded(_consumer, []), do: {:ok, 0}

  def messages_succeeded(consumer, ack_ids) do
    consumer
    |> partitions()
    |> Enum.reduce_while({:ok, 0}, fn partition, {:ok, acc_count} ->
      case GenServer.call(via_tuple(consumer.id, partition), {:messages_succeeded, ack_ids, false}) do
        {:ok, count} -> {:cont, {:ok, acc_count + count}}
        error -> {:halt, error}
      end
    end)
  catch
    :exit, e ->
      {:error, exit_to_sequin_error(e)}
  end

  @doc """
  Acknowledges messages as already succeeded, likely due to idempotency checks.
  """
  @impl SlotMessageStoreBehaviour
  def messages_already_succeeded(_consumer, []), do: :ok

  def messages_already_succeeded(consumer, ack_ids) do
    consumer
    |> partitions()
    |> Enum.reduce_while({:ok, 0}, fn partition, {:ok, acc_count} ->
      case GenServer.call(via_tuple(consumer.id, partition), {:messages_already_succeeded, ack_ids}) do
        {:ok, count} -> {:cont, {:ok, count + acc_count}}
        error -> {:halt, error}
      end
    end)
  catch
    :exit, e ->
      {:error, exit_to_sequin_error(e)}
  end

  @impl SlotMessageStoreBehaviour
  def messages_failed(_consumer, []), do: :ok

  def messages_failed(consumer, message_metadatas) do
    message_metadatas
    |> Enum.group_by(&message_partition(&1, consumer.partition_count))
    |> Sequin.Enum.reduce_while_ok(fn {partition, message_metadatas} ->
      GenServer.call(via_tuple(consumer.id, partition), {:messages_failed, message_metadatas})
    end)
  catch
    :exit, e ->
      {:error, exit_to_sequin_error(e)}
  end

  def nack_stale_produced_messages(consumer) do
    consumer
    |> partitions()
    |> Sequin.Enum.reduce_while_ok(fn partition ->
      GenServer.call(via_tuple(consumer.id, partition), :nack_stale_produced_messages)
    end)
  catch
    :exit, e ->
      {:error, exit_to_sequin_error(e)}
  end

  @spec reset_message_visibilities(SinkConsumer.t(), list(SinkConsumer.ack_id())) ::
          {:ok, list(ConsumerRecord.t() | ConsumerEvent.t())} | {:error, Exception.t()}
  def reset_message_visibilities(consumer, ack_ids) do
    consumer
    |> partitions()
    |> Enum.reduce_while({:ok, []}, fn partition, {:ok, acc_reset_messages} ->
      case GenServer.call(via_tuple(consumer.id, partition), {:reset_message_visibilities, ack_ids}) do
        {:ok, reset_messages} -> {:cont, {:ok, acc_reset_messages ++ reset_messages}}
        error -> {:halt, error}
      end
    end)
  catch
    :exit, e ->
      {:error, exit_to_sequin_error(e)}
  end

  @spec reset_all_message_visibilities(SinkConsumer.t()) :: :ok | {:error, Exception.t()}
  def reset_all_message_visibilities(consumer) do
    consumer
    |> partitions()
    |> Sequin.Enum.reduce_while_ok(fn partition ->
      GenServer.call(via_tuple(consumer.id, partition), :reset_all_message_visibilities)
    end)
  catch
    :exit, e ->
      {:error, exit_to_sequin_error(e)}
  end

  @doc """
  Should raise so SlotProcessorServer cannot continue if this fails.
  """
  @spec min_unpersisted_wal_cursors(SinkConsumer.t(), reference()) :: list(Replication.wal_cursor())
  def min_unpersisted_wal_cursors(consumer, monitor_ref) do
    consumer
    |> partitions()
    |> Enum.reduce_while([], fn partition, min_wal_cursors ->
      case GenServer.call(via_tuple(consumer.id, partition), {:min_unpersisted_wal_cursor, monitor_ref}) do
        {:ok, nil} -> {:cont, min_wal_cursors}
        {:ok, min_wal_cursor} -> {:cont, [min_wal_cursor | min_wal_cursors]}
        error -> {:halt, error}
      end
    end)
  catch
    :exit, e ->
      error = exit_to_sequin_error(e)
      Logger.error("[SlotMessageStore] Failed to get min unflushed commit lsn", error: error)
      raise error
  end

  @doc """
  Counts the number of messages in the message store.
  """
  @spec count_messages(SinkConsumer.t()) :: {:ok, non_neg_integer()} | {:error, Exception.t()}
  def count_messages(consumer) do
    consumer
    |> partitions()
    |> Enum.reduce_while({:ok, 0}, fn partition, {:ok, acc_count} ->
      case GenServer.call(via_tuple(consumer.id, partition), :count_messages) do
        {:ok, count} -> {:cont, {:ok, acc_count + count}}
        error -> {:halt, error}
      end
    end)
  catch
    :exit, e ->
      {:error, exit_to_sequin_error(e)}
  end

  @doc """
  Peek at SlotMessageStore state. For use in dev/test only.
  """
  @spec peek(SinkConsumer.t()) :: [State.t()] | {:error, Exception.t()}
  def peek(consumer) do
    consumer
    |> partitions()
    |> Enum.reduce_while([], fn partition, acc_states ->
      case GenServer.call(via_tuple(consumer.id, partition), :peek) do
        {:ok, state} -> {:cont, [state | acc_states]}
        error -> {:halt, error}
      end
    end)
  catch
    :exit, e ->
      {:error, exit_to_sequin_error(e)}
  end

  @spec peek_messages(SinkConsumer.t(), pos_integer()) ::
          list(ConsumerRecord.t() | ConsumerEvent.t()) | {:error, Exception.t()}
  def peek_messages(consumer, count) do
    consumer
    |> partitions()
    |> Enum.reduce_while({:ok, []}, fn partition, {:ok, acc_messages} ->
      case GenServer.call(via_tuple(consumer.id, partition), {:peek_messages, count}) do
        {:ok, messages} -> {:cont, {:ok, acc_messages ++ messages}}
        error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, messages} ->
        messages
        |> Enum.sort_by(&{&1.commit_lsn, &1.commit_idx})
        |> Enum.take(count)

      error ->
        error
    end
  catch
    :exit, e ->
      {:error, exit_to_sequin_error(e)}
  end

  @doc """
  Set the monitor reference for the SlotMessageStore into State
  """
  @spec set_monitor_ref(SinkConsumer.t(), reference()) :: :ok
  def set_monitor_ref(consumer, ref) do
    consumer
    |> partitions()
    |> Sequin.Enum.reduce_while_ok(fn partition ->
      GenServer.call(via_tuple(consumer.id, partition), {:set_monitor_ref, ref}, :timer.seconds(120))
    end)
  end

  def child_spec(opts) do
    consumer = opts[:consumer]
    consumer_id = if consumer, do: consumer.id, else: Keyword.fetch!(opts, :consumer_id)
    partition = opts[:partition]

    %{
      id: via_tuple(consumer_id, partition),
      start: {__MODULE__, :start_link, [opts]}
    }
  end

  @impl GenServer
  def init(opts) do
    consumer = opts[:consumer]
    consumer_id = if consumer, do: consumer.id, else: Keyword.fetch!(opts, :consumer_id)
    partition = Keyword.fetch!(opts, :partition)

    Logger.metadata(consumer_id: consumer_id)
    Logger.info("[SlotMessageStore] Initializing message store for consumer #{consumer_id}")

    state = %State{
      consumer: consumer,
      consumer_id: consumer_id,
      test_pid: Keyword.get(opts, :test_pid),
      setting_system_max_memory_bytes:
        Keyword.get(opts, :setting_system_max_memory_bytes, Application.get_env(:sequin, :max_memory_bytes)),
      partition: partition,
      flush_interval: Keyword.get(opts, :flush_interval, :timer.minutes(1)),
      message_age_before_flush_ms: Keyword.get(opts, :message_age_before_flush_ms, :timer.minutes(2)),
      visibility_check_interval: Keyword.get(opts, :visibility_check_interval, :timer.minutes(5)),
      max_time_since_delivered_ms: Keyword.get(opts, :max_time_since_delivered_ms, :timer.minutes(2))
    }

    {:ok, state, {:continue, :init}}
  end

  @impl GenServer
  def handle_continue(:init, %State{} = state) do
    # If we're not self-hosted, there will be no system-wide max memory bytes
    # so we don't need to recalculate max_memory_bytes on consumers changing
    if state.setting_system_max_memory_bytes do
      :syn.join(:consumers, :consumers_changed, self())
    end

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

    state = %State{state | consumer: consumer}

    Logger.metadata(account_id: consumer.account_id, replication_id: consumer.replication_slot_id)

    :ok = State.setup_ets(state)

    persisted_messages =
      consumer
      |> Consumers.list_consumer_messages_for_consumer()
      |> Stream.filter(&(message_partition(&1, consumer.partition_count) == state.partition))
      |> Enum.map(fn msg ->
        %{msg | payload_size_bytes: :erlang.external_size(msg.data)}
      end)

    state =
      state
      |> put_max_memory_bytes()
      |> State.put_persisted_messages(persisted_messages)

    schedule_process_logging(0)
    schedule_max_memory_check()
    schedule_flush_check(state.flush_interval)
    schedule_visibility_check(state.visibility_check_interval)
    {:noreply, state}
  end

  @impl GenServer
  def handle_call({:put_messages, messages}, from, %State{} = state) do
    execute_timed(:put_messages, fn ->
      # Validate first
      case State.validate_put_messages(state, messages) do
        {:ok, _incoming_payload_size_bytes} ->
          # Reply early since validation passed. This frees up the SlotProcessorServer to continue
          # calling other SMSs, accumulating messages, etc.
          GenServer.reply(from, :ok)

          execute_timed(:put_messages_after_reply, fn ->
            now = Sequin.utc_now()

            messages =
              messages
              |> Stream.reject(&State.message_exists?(state, &1))
              |> Stream.map(&%{&1 | ack_id: Sequin.uuid4(), ingested_at: now})
              |> Enum.to_list()

            {to_persist, to_put} =
              if state.consumer.status == :disabled do
                {messages, []}
              else
                Enum.split_with(messages, &State.is_message_group_persisted?(state, &1.group_id))
              end

            {:ok, state} = State.put_messages(state, to_put)

            :ok = upsert_messages(state, to_persist)
            state = State.put_persisted_messages(state, to_persist)

            Health.put_event(state.consumer, %Event{slug: :messages_ingested, status: :success})
            :syn.publish(:consumers, {:messages_maybe_available, state.consumer.id}, :messages_maybe_available)

            if state.test_pid do
              send(state.test_pid, {:put_messages_done, state.consumer.id})
            end

            {:noreply, state}
          end)

        {:error, error} ->
          # Reply with error if validation fails
          {:reply, {:error, error}, state}
      end
    end)
  end

  @impl GenServer
  def handle_call({:put_table_reader_batch, messages, batch_id}, _from, %State{} = state) do
    execute_timed(:put_table_reader_batch, fn ->
      now = Sequin.utc_now()

      messages = Enum.map(messages, &%{&1 | ack_id: Sequin.uuid4(), ingested_at: now})

      {to_persist, to_put} = Enum.split_with(messages, &State.is_message_group_persisted?(state, &1.group_id))

      with {:ok, state} <- State.put_table_reader_batch(state, to_put, batch_id),
           :ok <- upsert_messages(state, to_persist) do
        state = State.put_persisted_messages(state, to_persist)
        Health.put_event(state.consumer, %Event{slug: :messages_ingested, status: :success})
        :syn.publish(:consumers, {:messages_maybe_available, state.consumer.id}, :messages_maybe_available)

        {:reply, :ok, state}
      else
        error ->
          {:reply, error, state}
      end
    end)
  end

  @impl GenServer
  def handle_call(:unpersisted_table_reader_batch_ids, _from, %State{} = state) do
    {:reply, {:ok, State.unpersisted_table_reader_batch_ids(state)}, state}
  end

  def handle_call({:produce, count, producer_pid}, from, %State{last_consumer_producer_pid: last_producer_pid} = state)
      when not is_nil(last_producer_pid) and last_producer_pid != producer_pid do
    execute_timed(:produce, fn ->
      # Producer has changed, so we need to reset the state
      {state, _count} = State.nack_produced_messages(state)
      handle_call({:produce, count, producer_pid}, from, %{state | last_consumer_producer_pid: producer_pid})
    end)
  end

  def handle_call({:produce, count, producer_pid}, _from, %State{} = state) do
    execute_timed(:produce, fn ->
      {time, {messages, state}} =
        :timer.tc(
          fn ->
            {_messages, _state} = State.produce_messages(state, count)
          end,
          :millisecond
        )

      messages
      |> Enum.map(&MessageLedgers.wal_cursor_from_message/1)
      |> then(&MessageLedgers.wal_cursors_reached_checkpoint(state.consumer_id, "slot_message_store.produce", &1))

      if time > @min_log_time_ms do
        Logger.warning(
          "[SlotMessageStore] Produce messages took longer than expected",
          count: length(messages),
          duration_ms: time,
          message_count: map_size(state.messages)
        )
      end

      state = %{state | last_consumer_producer_pid: producer_pid}

      if length(messages) > 0 do
        Health.put_event(state.consumer, %Event{slug: :messages_pending_delivery, status: :success})
        {:reply, {:ok, messages}, state}
      else
        :syn.publish(:consumers, {:messages_maybe_available, state.consumer.id}, :messages_maybe_available)
        {:reply, {:ok, []}, state, :hibernate}
      end
    end)
  end

  def handle_call({:messages_succeeded, [], _return_messages?}, _from, state) do
    {:reply, {:ok, 0}, state}
  end

  def handle_call({:messages_succeeded, ack_ids, return_messages?}, _from, state) do
    prev_state = state

    execute_timed(:messages_succeeded, fn ->
      cursor_tuples = State.ack_ids_to_cursor_tuples(state, ack_ids)

      persisted_messages_to_drop =
        state
        |> State.peek_messages(cursor_tuples)
        |> Enum.filter(&State.is_message_persisted?(state, &1))

      {dropped_messages, state} = State.pop_messages(state, cursor_tuples)

      :ok = delete_messages(state, Enum.map(persisted_messages_to_drop, & &1.ack_id))

      maybe_finish_table_reader_batch(prev_state, state)

      :syn.publish(:consumers, {:messages_maybe_available, state.consumer.id}, :messages_maybe_available)

      if return_messages? do
        {:reply, {:ok, dropped_messages}, state}
      else
        # Optionally, avoid returning full messages (more CPU/memory intensive)
        {:reply, {:ok, length(dropped_messages)}, state}
      end
    end)
  end

  def handle_call({:messages_already_succeeded, []}, _from, state) do
    {:reply, {:ok, 0}, state}
  end

  def handle_call({:messages_already_succeeded, ack_ids}, _from, state) do
    prev_state = state

    execute_timed(:messages_already_succeeded, fn ->
      cursor_tuples = State.ack_ids_to_cursor_tuples(state, ack_ids)

      persisted_messages_to_drop =
        state
        |> State.peek_messages(cursor_tuples)
        |> Enum.filter(&State.is_message_persisted?(state, &1))

      {dropped_messages, state} = State.pop_messages(state, cursor_tuples)

      count = length(dropped_messages)

      :ok = delete_messages(state, Enum.map(persisted_messages_to_drop, & &1.ack_id))

      maybe_finish_table_reader_batch(prev_state, state)

      :syn.publish(:consumers, {:messages_maybe_available, state.consumer.id}, :messages_maybe_available)

      {:reply, {:ok, count}, state}
    end)
  end

  def handle_call({:messages_failed, message_metadatas}, _from, state) do
    prev_state = state

    execute_timed(:messages_failed, fn ->
      cursor_tuples = State.ack_ids_to_cursor_tuples(state, Enum.map(message_metadatas, & &1.ack_id))

      message_metas_by_ack_id = Map.new(message_metadatas, &{&1.ack_id, &1})
      {messages, state} = State.pop_messages(state, cursor_tuples)

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

      state = State.put_persisted_messages(state, messages)

      with {newly_blocked_messages, state} <- State.pop_blocked_messages(state),
           # Now put the blocked messages into persisted_messages
           state = State.put_persisted_messages(state, newly_blocked_messages),
           :ok <- upsert_messages(state, messages ++ newly_blocked_messages) do
        maybe_finish_table_reader_batch(prev_state, state)
        {:reply, :ok, state}
      else
        error ->
          {:reply, error, state}
      end
    end)
  end

  def handle_call(:nack_stale_produced_messages, _from, state) do
    execute_timed(:nack_stale_produced_messages, fn ->
      {:reply, :ok, State.nack_stale_produced_messages(state)}
    end)
  end

  def handle_call({:reset_message_visibilities, ack_ids}, _from, state) do
    execute_timed(:reset_message_visibilities, fn ->
      :syn.publish(:consumers, {:messages_maybe_available, state.consumer.id}, :messages_maybe_available)
      {state, reset_messages} = State.reset_message_visibilities(state, ack_ids)
      {:reply, {:ok, reset_messages}, state}
    end)
  end

  def handle_call(:reset_all_message_visibilities, _from, state) do
    execute_timed(:reset_all_message_visibilities, fn ->
      :syn.publish(:consumers, {:messages_maybe_available, state.consumer.id}, :messages_maybe_available)
      {:reply, :ok, State.reset_all_message_visibilities(state)}
    end)
  end

  def handle_call({:min_unpersisted_wal_cursor, monitor_ref}, _from, state) do
    execute_timed(:min_unpersisted_wal_cursor, fn ->
      if monitor_ref == state.slot_processor_monitor_ref do
        {:reply, {:ok, State.min_unpersisted_wal_cursor(state)}, state}
      else
        raise "Monitor ref mismatch. Expected #{inspect(state.slot_processor_monitor_ref)} but got #{inspect(monitor_ref)}"
      end
    end)
  end

  def handle_call(:count_messages, _from, state) do
    execute_timed(:count_messages, fn ->
      {:reply, {:ok, map_size(state.messages)}, state}
    end)
  end

  def handle_call(:peek, _from, state) do
    execute_timed(:peek, fn ->
      {:reply, {:ok, state}, state}
    end)
  end

  def handle_call({:peek_messages, count}, _from, state) do
    execute_timed(:peek_messages, fn ->
      {:reply, {:ok, State.peek_messages(state, count)}, state}
    end)
  end

  def handle_call({:set_monitor_ref, ref}, _from, state) do
    execute_timed(:set_monitor_ref, fn ->
      {:reply, :ok, %{state | slot_processor_monitor_ref: ref}}
    end)
  end

  def handle_call(:payload_size_bytes, _from, state) do
    {:reply, {:ok, state.payload_size_bytes}, state}
  end

  @impl GenServer
  def handle_info(:process_logging, %State{} = state) do
    now = System.monotonic_time(:millisecond)
    interval_ms = if state.last_logged_stats_at, do: now - state.last_logged_stats_at

    info =
      Process.info(self(), [
        # Total memory used by process in bytes
        :memory,
        # Number of messages in queue
        :message_queue_len
      ])

    metadata = [
      memory_mb: Float.round(info[:memory] / 1_024 / 1_024, 2),
      message_queue_len: info[:message_queue_len],
      message_count: map_size(state.messages),
      payload_size_bytes: state.payload_size_bytes,
      produced_message_groups: map_size(state.produced_message_groups)
    ]

    # Get all timing metrics from process dictionary
    timing_metrics =
      Process.get()
      |> Enum.filter(fn {key, _} ->
        key |> to_string() |> String.ends_with?("_total_ms")
      end)
      |> Keyword.new()

    # Log all timing metrics as histograms with operation tag
    Enum.each(timing_metrics, fn {key, value} ->
      operation = key |> to_string() |> String.replace("_total_ms", "")

      Sequin.Statsd.histogram("sequin.slot_message_store.operation_time_ms", value,
        tags: %{
          consumer_id: state.consumer_id,
          operation: operation
        }
      )
    end)

    unaccounted_ms =
      if interval_ms do
        # Calculate total accounted time
        total_accounted_ms = Enum.reduce(timing_metrics, 0, fn {_key, value}, acc -> acc + value end)

        # Calculate unaccounted time
        max(0, interval_ms - total_accounted_ms)
      end

    if unaccounted_ms do
      # Log unaccounted time with same metric but different operation tag
      Sequin.Statsd.histogram("sequin.slot_message_store.operation_time_ms", unaccounted_ms,
        tags: %{
          consumer_id: state.consumer_id,
          operation: "unaccounted"
        }
      )
    end

    metadata =
      metadata
      |> Keyword.merge(timing_metrics)
      |> Sequin.Keyword.put_if_present(:unaccounted_total_ms, unaccounted_ms)

    Logger.info("[SlotMessageStore] Process metrics", metadata)

    # Clear timing metrics after logging
    timing_metrics
    |> Keyword.keys()
    |> Enum.each(&clear_counter/1)

    schedule_process_logging()
    {:noreply, %{state | last_logged_stats_at: now}}
  end

  @impl GenServer
  # :syn notification
  def handle_info(:consumers_changed, %State{} = state) do
    state = put_max_memory_bytes(state)
    {:noreply, state}
  end

  def handle_info(:max_memory_check, %State{} = state) do
    schedule_max_memory_check(state.setting_max_memory_check_interval)
    state = put_max_memory_bytes(state)
    {:noreply, state}
  end

  def handle_info(:flush_messages, %State{} = state) do
    Logger.info("[SlotMessageStore] Checking for messages to flush")

    messages = State.messages_to_flush(state)

    state =
      if length(messages) > 0 do
        Logger.warning(
          "[SlotMessageStore] Flushing #{length(messages)} old messages to allow slot advancement",
          consumer_id: state.consumer_id,
          partition: state.partition
        )

        upsert_messages(state, messages)

        state = State.put_persisted_messages(state, messages)

        if state.test_pid do
          send(state.test_pid, {:flush_messages_done, state.consumer_id})
        end

        Logger.info(
          "[SlotMessageStore] Flushed #{length(messages)} old messages to allow slot advancement",
          consumer_id: state.consumer_id,
          partition: state.partition
        )

        state
      else
        state
      end

    schedule_flush_check(state.flush_interval)
    {:noreply, state}
  end

  def handle_info(:check_visibility, %State{} = state) do
    Logger.info("[SlotMessageStore] Checking for messages to make visible")

    messages = State.messages_to_make_visible(state)

    state =
      if length(messages) > 0 do
        Logger.info(
          "[SlotMessageStore] Making #{length(messages)} messages visible for delivery",
          consumer_id: state.consumer_id,
          partition: state.partition
        )

        # Reset visibility for all messages that are no longer not visible
        {state, _reset_messages} = State.reset_message_visibilities(state, Enum.map(messages, & &1.ack_id))

        if state.test_pid do
          send(state.test_pid, {:visibility_check_done, state.consumer_id})
        end

        Logger.info(
          "[SlotMessageStore] Made #{length(messages)} messages visible for delivery",
          consumer_id: state.consumer_id,
          partition: state.partition
        )

        state
      else
        state
      end

    schedule_visibility_check(state.visibility_check_interval)
    {:noreply, state}
  end

  defp schedule_process_logging(interval \\ :timer.seconds(30)) do
    Process.send_after(self(), :process_logging, interval)
  end

  # Can be very infrequent, as we use :syn to learn about changes to consumer count
  defp schedule_max_memory_check(interval \\ :timer.minutes(5)) do
    Process.send_after(self(), :max_memory_check, interval)
  end

  defp schedule_flush_check(interval) do
    Process.send_after(self(), :flush_messages, interval)
  end

  defp schedule_visibility_check(interval) do
    Process.send_after(self(), :check_visibility, interval)
  end

  defp put_max_memory_bytes(%State{} = state) do
    consumer = state.consumer

    max_memory_bytes =
      if state.setting_system_max_memory_bytes do
        # Must be self-hosted/dev. Check consumer's max_memory_mb setting
        consumer_count = Consumers.count_non_disabled_sink_consumers()

        Consumers.max_system_memory_bytes_for_consumer(consumer, consumer_count, state.setting_system_max_memory_bytes)
      else
        Consumers.max_memory_bytes_for_consumer(consumer)
      end

    %{state | max_memory_bytes: div(max_memory_bytes, consumer.partition_count)}
  end

  defp exit_to_sequin_error({:timeout, {GenServer, :call, [_, _, timeout]}}) do
    Error.invariant(message: "[SlotMessageStore] call timed out after #{timeout}ms")
  end

  defp exit_to_sequin_error({:noproc, _}) do
    Error.invariant(message: "Call to SlotMessageStore failed with :noproc")
  end

  defp exit_to_sequin_error(e) when is_exception(e) do
    Error.invariant(message: "[SlotMessageStore] exited with #{Exception.message(e)}")
  end

  defp exit_to_sequin_error(e) do
    Error.invariant(message: "[SlotMessageStore] exited with #{inspect(e)}")
  end

  defp upsert_messages(%State{}, []), do: :ok

  defp upsert_messages(%State{} = state, messages) do
    messages
    # This value is calculated based on the number of parameters in our consumer_events/consumer_records
    # upserts and the Postgres limit of 65535 parameters per query.
    |> Enum.chunk_every(2_000)
    |> Enum.each(fn chunk ->
      {:ok, _count} = Consumers.upsert_consumer_messages(state.consumer, chunk)
    end)
  end

  defp delete_messages(%State{}, []), do: :ok

  defp delete_messages(%State{} = state, messages) do
    messages
    |> Enum.chunk_every(10_000)
    |> Enum.each(fn chunk ->
      {:ok, _count} = Consumers.ack_messages(state.consumer, chunk)
    end)
  end

  defp maybe_finish_table_reader_batch(%State{} = prev_state, %State{} = state) do
    unless State.unpersisted_table_reader_batch_ids(prev_state) == State.unpersisted_table_reader_batch_ids(state) do
      :syn.publish(
        :consumers,
        {:table_reader_batches_changed, state.consumer.id},
        :table_reader_batches_changed
      )
    end
  end

  defp incr_counter(name, amount) do
    current = Process.get(name, 0)
    Process.put(name, current + amount)
  end

  defp clear_counter(name) do
    Process.delete(name)
  end

  defp execute_timed(name, fun) do
    {time, result} = :timer.tc(fun, :millisecond)
    incr_counter(:"#{name}_total_ms", time)
    result
  end

  def partitions(%SinkConsumer{partition_count: partition_count}) do
    Enum.to_list(0..(partition_count - 1))
  end

  defp message_partition(message, partition_count) do
    :erlang.phash2(message.group_id, partition_count)
  end
end

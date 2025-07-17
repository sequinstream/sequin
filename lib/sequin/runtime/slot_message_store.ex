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

  use Sequin.ProcessMetrics,
    metric_prefix: "sequin.slot_message_store",
    on_log: {__MODULE__, :process_metrics_on_log, []}

  use Sequin.ProcessMetrics.Decorator

  alias Ecto.Adapters.SQL.Sandbox
  alias Sequin.Consumers
  alias Sequin.Consumers.AcknowledgedMessages
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Error
  alias Sequin.Error.InvariantError
  alias Sequin.Error.NotFoundError
  alias Sequin.Health
  alias Sequin.Health.Event
  alias Sequin.ProcessMetrics
  alias Sequin.Prometheus
  alias Sequin.Replication
  alias Sequin.Repo
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
    consumer_id = Keyword.fetch!(opts, :consumer_id)
    partition = Keyword.fetch!(opts, :partition)
    GenServer.start_link(__MODULE__, opts, id: via_tuple(consumer_id, partition), name: via_tuple(consumer_id, partition))
  end

  @spec via_tuple(consumer_id(), non_neg_integer()) ::
          {:via, :syn, {:replication, {module(), {consumer_id(), non_neg_integer()}}}}
  def via_tuple(consumer_id, partition) when is_integer(partition) do
    {:via, :syn, {:replication, {__MODULE__, {consumer_id, partition}}}}
  end

  @impl GenServer
  def init(opts) do
    consumer_id = Keyword.fetch!(opts, :consumer_id)
    partition = Keyword.fetch!(opts, :partition)

    if test_pid = opts[:test_pid] do
      Sandbox.allow(Sequin.Repo, test_pid, self())
    end

    Logger.metadata(consumer_id: consumer_id)
    Sequin.name_process({__MODULE__, {consumer_id, partition}})
    Logger.info("[SlotMessageStore] Initializing message store for consumer #{consumer_id}")

    state = %State{
      consumer_id: consumer_id,
      test_pid: Keyword.get(opts, :test_pid),
      setting_system_max_memory_bytes:
        Keyword.get(opts, :setting_system_max_memory_bytes, Application.get_env(:sequin, :max_memory_bytes)),
      max_memory_bytes: Keyword.get(opts, :max_memory_bytes),
      partition: partition,
      flush_interval: Keyword.get(opts, :flush_interval, to_timeout(second: 15)),
      message_age_before_flush_ms: Keyword.get(opts, :message_age_before_flush_ms, to_timeout(minute: 2)),
      visibility_check_interval: Keyword.get(opts, :visibility_check_interval, to_timeout(minute: 5)),
      max_time_since_delivered_ms: Keyword.get(opts, :max_time_since_delivered_ms, to_timeout(minute: 2))
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
      Sandbox.allow(Sequin.Repo, state.test_pid, self())
      Mox.allow(Sequin.TestSupport.DateTimeMock, state.test_pid, self())
      Mox.allow(Sequin.TestSupport.UUIDMock, state.test_pid, self())
    end

    consumer = Consumers.get_sink_consumer!(state.consumer_id)

    Sequin.ProcessMetrics.start()
    ProcessMetrics.metadata(%{consumer_id: consumer.id, consumer_name: consumer.name, partition: state.partition})

    state = %{state | consumer: consumer}

    Logger.metadata(account_id: consumer.account_id, replication_id: consumer.replication_slot_id)

    :ok = State.setup_ets(state)

    # First set max_memory_bytes to have the limits available
    state = put_max_memory_bytes(state)

    case stream_messages_into_state(state) do
      {:ok, state} ->
        # Reclaim memory after using the database struct
        :erlang.garbage_collect()

        Sequin.ProcessMetrics.gauge("payload_size_bytes", state.payload_size_bytes)

        schedule_max_memory_check()
        schedule_flush_check(state.flush_interval)
        schedule_visibility_check(state.visibility_check_interval)

        if test_pid = state.test_pid do
          send(test_pid, :init_complete)
        end

        {:noreply, state}

      error ->
        {:stop, error, state}
    end
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
    |> Enum.reduce_while({:ok, [], []}, fn partition, {:ok, acc_messages, acc_ack_ids} ->
      case GenServer.call(via_tuple(consumer.id, partition), {:messages_succeeded, ack_ids, true}) do
        {:ok, messages, ack_ids} -> {:cont, {:ok, acc_messages ++ messages, acc_ack_ids ++ ack_ids}}
        error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, messages, ack_ids} ->
        Consumers.ack_messages(consumer, ack_ids)
        {:ok, messages}

      {:error, error} ->
        {:error, error}
    end
  catch
    :exit, e ->
      {:error, exit_to_sequin_error(e)}
  end

  @impl SlotMessageStoreBehaviour
  def messages_succeeded(_consumer, []), do: {:ok, 0}

  def messages_succeeded(consumer, consumer_messages) do
    ack_ids_by_partition = Enum.group_by(consumer_messages, &message_partition(&1, consumer.partition_count), & &1.ack_id)

    consumer
    |> partitions()
    |> Enum.reduce_while({:ok, 0, []}, fn partition, {:ok, acc_count, acc_ack_ids} ->
      case Map.get(ack_ids_by_partition, partition, []) do
        [] ->
          {:cont, {:ok, acc_count, acc_ack_ids}}

        ack_ids ->
          case GenServer.call(via_tuple(consumer.id, partition), {:messages_succeeded, ack_ids, false}) do
            {:ok, count, ack_ids} -> {:cont, {:ok, acc_count + count, acc_ack_ids ++ ack_ids}}
            error -> {:halt, error}
          end
      end
    end)
    |> case do
      {:ok, count, ack_ids} ->
        Consumers.ack_messages(consumer, ack_ids)
        {:ok, count}

      {:error, error} ->
        {:error, error}
    end
  catch
    :exit, e ->
      {:error, exit_to_sequin_error(e)}
  end

  @doc """
  Acknowledges messages as already succeeded, likely due to idempotency checks.
  """
  @impl SlotMessageStoreBehaviour
  def messages_already_succeeded(_consumer, []), do: :ok

  def messages_already_succeeded(consumer, consumer_messages) do
    ack_ids_by_partition = Enum.group_by(consumer_messages, &message_partition(&1, consumer.partition_count), & &1.ack_id)

    consumer
    |> partitions()
    |> Enum.reduce_while({:ok, 0, []}, fn partition, {:ok, acc_count, acc_ack_ids} ->
      case Map.get(ack_ids_by_partition, partition, []) do
        [] ->
          {:cont, {:ok, acc_count, acc_ack_ids}}

        ack_ids ->
          case GenServer.call(via_tuple(consumer.id, partition), {:messages_already_succeeded, ack_ids}) do
            {:ok, count, ack_ids} -> {:cont, {:ok, count + acc_count, acc_ack_ids ++ ack_ids}}
            error -> {:halt, error}
          end
      end
    end)
    |> case do
      {:ok, count, ack_ids} ->
        Consumers.ack_messages(consumer, ack_ids)
        {:ok, count}

      {:error, error} ->
        {:error, error}
    end
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
  Discards all messages in the message store.
  """
  @spec discard_all_messages(SinkConsumer.t()) :: {:ok, non_neg_integer()} | {:error, Exception.t()}
  def discard_all_messages(consumer) do
    call_all_partitions(consumer, :discard_all_messages)
  end

  @doc """
  Discards only messages that are currently in a failed state (have not_visible_until set in the future).
  """
  @spec discard_failing_messages(SinkConsumer.t()) :: {:ok, non_neg_integer()} | {:error, Exception.t()}
  def discard_failing_messages(consumer) do
    call_all_partitions(consumer, :discard_failing_messages)
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
  Updates the high watermark WAL cursor across all partitions.
  """
  @spec put_high_watermark_wal_cursor(SinkConsumer.t(), {batch_idx :: non_neg_integer(), Replication.wal_cursor()}) ::
          :ok | {:error, Exception.t()}
  def put_high_watermark_wal_cursor(consumer, {batch_idx, wal_cursor}) do
    consumer
    |> partitions()
    |> Sequin.Enum.reduce_while_ok(fn partition ->
      GenServer.cast(via_tuple(consumer.id, partition), {:put_high_watermark_wal_cursor, {batch_idx, wal_cursor}})
    end)
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
  def peek_messages(consumer, count) when is_integer(count) do
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

  @decorate track_metrics("peek_message")
  def peek_message(consumer, ack_id) do
    not_found = {:error, Error.not_found(entity: :message, params: %{ack_id: ack_id})}

    consumer
    |> partitions()
    |> Enum.reduce_while(not_found, fn partition, not_found ->
      case GenServer.call(via_tuple(consumer.id, partition), {:peek_message, ack_id}) do
        # When we find the message, we halt with the message
        {:ok, message} -> {:halt, {:ok, message}}
        # When we don't find the message, we continue to the next partition
        {:error, %NotFoundError{}} -> {:cont, not_found}
        error -> {:halt, error}
      end
    end)
  end

  @doc """
  Set the monitor reference for the SlotMessageStore into State
  """
  @decorate track_metrics("set_monitor_ref")
  @spec set_monitor_ref(SinkConsumer.t(), reference()) :: :ok
  def set_monitor_ref(consumer, ref) do
    consumer
    |> partitions()
    |> Sequin.Enum.reduce_while_ok(fn partition ->
      GenServer.call(via_tuple(consumer.id, partition), {:set_monitor_ref, ref}, to_timeout(second: 120))
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
  @decorate track_metrics("put_messages")
  def handle_call({:put_messages, messages}, from, %State{} = state) do
    # Validate first
    case State.validate_put_messages(state, messages) do
      {:ok, _incoming_payload_size_bytes} ->
        # Reply early since validation passed. This frees up the SlotProcessorServer to continue
        # calling other SMSs, accumulating messages, etc.
        GenServer.reply(from, :ok)

        {:noreply, put_messages_after_reply(state, messages)}

      {:error, %InvariantError{code: :payload_size_limit_exceeded} = error} ->
        # Reply with error if validation fails
        {:reply, {:error, error}, state}
    end
  end

  @impl GenServer
  @decorate track_metrics("put_table_reader_batch")
  def handle_call({:put_table_reader_batch, messages, batch_id}, _from, %State{} = state) do
    now = Sequin.utc_now()

    messages = Enum.map(messages, &%{&1 | ack_id: Sequin.uuid4(), ingested_at: now})

    {to_persist, to_put} = Enum.split_with(messages, &State.message_group_persisted?(state, &1.table_oid, &1.group_id))

    with {:ok, state} <- State.put_table_reader_batch(state, to_put, batch_id),
         :ok <- upsert_messages(state, to_persist) do
      state = State.put_persisted_messages(state, to_persist)
      Health.put_event(state.consumer, %Event{slug: :messages_ingested, status: :success})
      :syn.publish(:consumers, {:messages_maybe_available, state.consumer.id}, :messages_maybe_available)

      Sequin.ProcessMetrics.increment_throughput("table_reader_batch_messages", length(messages))
      Sequin.ProcessMetrics.gauge("payload_size_bytes", state.payload_size_bytes)
      {:reply, :ok, state}
    else
      error ->
        {:reply, error, state}
    end
  end

  @decorate track_metrics("unpersisted_table_reader_batch_ids")
  def handle_call(:unpersisted_table_reader_batch_ids, _from, %State{} = state) do
    {:reply, {:ok, State.unpersisted_table_reader_batch_ids(state)}, state}
  end

  @decorate track_metrics("produce")
  def handle_call({:produce, count, producer_pid}, from, %State{last_consumer_producer_pid: last_producer_pid} = state)
      when not is_nil(last_producer_pid) and last_producer_pid != producer_pid do
    # Producer has changed, so we need to reset the state
    {state, _count} = State.nack_produced_messages(state)
    handle_call({:produce, count, producer_pid}, from, %{state | last_consumer_producer_pid: producer_pid})
  end

  @decorate track_metrics("produce")
  def handle_call({:produce, count, producer_pid}, _from, %State{} = state) do
    {time, {messages, state}} = :timer.tc(State, :produce_messages, [state, count], :millisecond)

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

    Sequin.ProcessMetrics.gauge("message_count", map_size(state.messages))
    Sequin.ProcessMetrics.gauge("produced_message_groups", map_size(state.produced_message_groups))

    if length(messages) > 0 do
      Sequin.ProcessMetrics.increment_throughput("messages_produced", length(messages))
      Health.put_event(state.consumer, %Event{slug: :messages_pending_delivery, status: :success})
      {:reply, {:ok, messages}, state}
    else
      {:reply, {:ok, []}, state, :hibernate}
    end
  end

  @decorate track_metrics("messages_succeeded_noop")
  def handle_call({:messages_succeeded, [], _return_messages?}, _from, state) do
    {:reply, {:ok, 0, []}, state}
  end

  @decorate track_metrics("messages_succeeded")
  def handle_call({:messages_succeeded, ack_ids, return_messages?}, _from, state) do
    prev_state = state

    cursor_tuples = State.ack_ids_to_cursor_tuples(state, ack_ids)

    ack_ids_to_ack =
      state
      |> State.peek_messages(cursor_tuples)
      |> Enum.filter(&State.message_persisted?(state, &1))
      |> Enum.map(& &1.ack_id)

    {dropped_messages, state} = State.pop_messages(state, cursor_tuples)

    maybe_finish_table_reader_batch(prev_state, state)

    :syn.publish(:consumers, {:messages_maybe_available, state.consumer.id}, :messages_maybe_available)

    Sequin.ProcessMetrics.increment_throughput("messages_succeeded", length(dropped_messages))
    Sequin.ProcessMetrics.gauge("message_count", map_size(state.messages))

    if return_messages? do
      {:reply, {:ok, dropped_messages, ack_ids_to_ack}, state}
    else
      # Optionally, avoid returning full messages (more CPU/memory intensive)
      {:reply, {:ok, length(dropped_messages), ack_ids_to_ack}, state}
    end
  end

  @decorate track_metrics("messages_already_succeeded_noop")
  def handle_call({:messages_already_succeeded, []}, _from, state) do
    {:reply, {:ok, 0}, state}
  end

  @decorate track_metrics("messages_already_succeeded")
  def handle_call({:messages_already_succeeded, ack_ids}, _from, state) do
    prev_state = state

    cursor_tuples = State.ack_ids_to_cursor_tuples(state, ack_ids)

    ack_ids_to_ack =
      state
      |> State.peek_messages(cursor_tuples)
      |> Enum.filter(&State.message_persisted?(state, &1))
      |> Enum.map(& &1.ack_id)

    {dropped_messages, state} = State.pop_messages(state, cursor_tuples)

    count = length(dropped_messages)

    maybe_finish_table_reader_batch(prev_state, state)

    :syn.publish(:consumers, {:messages_maybe_available, state.consumer.id}, :messages_maybe_available)

    Sequin.ProcessMetrics.increment_throughput("messages_already_succeeded", count)
    Sequin.ProcessMetrics.gauge("message_count", map_size(state.messages))

    {:reply, {:ok, count, ack_ids_to_ack}, state}
  end

  @decorate track_metrics("messages_failed")
  def handle_call({:messages_failed, message_metadatas}, _from, state) do
    prev_state = state

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

    {discarded_messages, messages} =
      maybe_discard_messages(messages, state.consumer.max_retry_count)

    Sequin.ProcessMetrics.increment_throughput("messages_failed", length(messages))
    Sequin.ProcessMetrics.increment_throughput("messages_discarded", length(discarded_messages))
    Sequin.ProcessMetrics.gauge("message_count", map_size(state.messages))

    with :ok <- handle_discarded_messages(state, discarded_messages),
         :ok <- upsert_messages(state, messages) do
      state = State.put_persisted_messages(state, messages)
      maybe_finish_table_reader_batch(prev_state, state)
      {:reply, :ok, state}
    else
      error ->
        {:reply, error, state}
    end
  end

  @decorate track_metrics("nack_stale_produced_messages")
  def handle_call(:nack_stale_produced_messages, _from, state) do
    state = State.nack_stale_produced_messages(state)
    Sequin.ProcessMetrics.gauge("produced_message_groups", map_size(state.produced_message_groups))
    {:reply, :ok, state}
  end

  @decorate track_metrics("reset_message_visibilities")
  def handle_call({:reset_message_visibilities, ack_ids}, _from, state) do
    :syn.publish(:consumers, {:messages_maybe_available, state.consumer.id}, :messages_maybe_available)
    {state, reset_messages} = State.reset_message_visibilities(state, ack_ids)

    Sequin.ProcessMetrics.increment_throughput("visibility_resets", length(reset_messages))

    {:reply, {:ok, reset_messages}, state}
  end

  @decorate track_metrics("reset_all_message_visibilities")
  def handle_call(:reset_all_message_visibilities, _from, state) do
    :syn.publish(:consumers, {:messages_maybe_available, state.consumer.id}, :messages_maybe_available)
    state = State.reset_all_message_visibilities(state)
    {:reply, :ok, state}
  end

  @decorate track_metrics("discard_all_messages")
  def handle_call(:discard_all_messages, _from, state) do
    all_messages = State.all_messages(state)
    do_discard_messages(state, all_messages)
  end

  @decorate track_metrics("discard_failing_messages")
  def handle_call(:discard_failing_messages, _from, state) do
    failing_messages = State.failing_messages(state)
    do_discard_messages(state, failing_messages)
  end

  @decorate track_metrics("min_unpersisted_wal_cursor")
  def handle_call({:min_unpersisted_wal_cursor, monitor_ref}, _from, state) do
    if monitor_ref == state.slot_processor_monitor_ref do
      {:reply, {:ok, State.min_unpersisted_wal_cursor(state)}, state}
    else
      raise "Monitor ref mismatch. Expected #{inspect(state.slot_processor_monitor_ref)} but got #{inspect(monitor_ref)}"
    end
  end

  @decorate track_metrics("count_messages")
  def handle_call(:count_messages, _from, state) do
    count = map_size(state.messages)
    Sequin.ProcessMetrics.gauge("message_count", count)
    {:reply, {:ok, count}, state}
  end

  @decorate track_metrics("peek")
  def handle_call(:peek, _from, state) do
    {:reply, {:ok, state}, state}
  end

  @decorate track_metrics("peek_messages")
  def handle_call({:peek_messages, count}, _from, state) do
    {:reply, {:ok, State.peek_messages(state, count)}, state}
  end

  @decorate track_metrics("peek_message")
  def handle_call({:peek_message, ack_id}, _from, state) do
    {:reply, State.peek_message(state, ack_id), state}
  end

  @decorate track_metrics("set_monitor_ref")
  def handle_call({:set_monitor_ref, ref}, _from, state) do
    {:reply, :ok, %{state | slot_processor_monitor_ref: ref}}
  end

  def handle_call(:payload_size_bytes, _from, state) do
    {:reply, {:ok, state.payload_size_bytes}, state}
  end

  @impl GenServer

  def handle_cast(
        {:put_high_watermark_wal_cursor, {batch_idx, wal_cursor}},
        %State{high_watermark_wal_cursor: nil} = state
      ) do
    state = %{state | high_watermark_wal_cursor: {batch_idx, wal_cursor}}
    {:noreply, state}
  end

  def handle_cast({:put_high_watermark_wal_cursor, {batch_idx, wal_cursor}}, %State{} = state) do
    {current_idx, _current_cursor} = state.high_watermark_wal_cursor

    # Do not raise if batch_idx is 0, means SlotProducer et al may have restarted
    # We may get repeated batches, in the instance
    if batch_idx != current_idx + 1 and batch_idx > 0 do
      raise "Unexpected high watermark WAL cursor: #{batch_idx} (given) != #{current_idx + 1} (expected)"
    end

    state = %{state | high_watermark_wal_cursor: {batch_idx, wal_cursor}}
    {:noreply, state}
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
    with_metrics("flush_messages", fn ->
      Logger.debug("[SlotMessageStore] Checking for messages to flush")
      start_time = System.monotonic_time(:millisecond)

      batch_size = flush_batch_size()
      messages = State.messages_to_flush(state, batch_size)

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
            send(state.test_pid, {:flush_messages_count, length(messages)})
          end

          end_time = System.monotonic_time(:millisecond)
          duration_ms = end_time - start_time

          Logger.info(
            "[SlotMessageStore] Flushed #{length(messages)} old messages to allow slot advancement",
            consumer_id: state.consumer_id,
            partition: state.partition,
            duration_ms: duration_ms
          )

          Sequin.ProcessMetrics.increment_throughput("messages_flushed", length(messages))
          state
        else
          state
        end

      # Flush again immediately we are likely to have more work to do
      schedule_flush_check(if length(messages) == batch_size, do: 0, else: state.flush_interval)

      {:noreply, state}
    end)
  end

  def handle_info(:check_visibility, %State{} = state) do
    Logger.debug("[SlotMessageStore] Checking for messages to make visible")

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

        Sequin.ProcessMetrics.increment_throughput("visibility_resets", length(messages))
        state
      else
        state
      end

    schedule_visibility_check(state.visibility_check_interval)
    {:noreply, state}
  end

  @decorate track_metrics("put_messages_after_reply")
  defp put_messages_after_reply(%State{} = state, messages) do
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
        Enum.split_with(messages, &State.message_group_persisted?(state, &1.table_oid, &1.group_id))
      end

    {:ok, state} = State.put_messages(state, to_put)

    :ok = upsert_messages(state, to_persist)
    state = State.put_persisted_messages(state, to_persist)

    Health.put_event(state.consumer, %Event{slug: :messages_ingested, status: :success})
    :syn.publish(:consumers, {:messages_maybe_available, state.consumer.id}, :messages_maybe_available)

    if state.test_pid do
      send(state.test_pid, {:put_messages_done, state.consumer.id})
    end

    state
  end

  defp schedule_max_memory_check(interval \\ to_timeout(minute: 5)) do
    Process.send_after(self(), :max_memory_check, interval)
  end

  defp schedule_flush_check(interval) do
    Process.send_after(self(), :flush_messages, interval)
  end

  defp schedule_visibility_check(interval) do
    Process.send_after(self(), :check_visibility, interval)
  end

  # Helper function for manually instrumenting functions that can't use decorators
  defp with_metrics(name, fun) when is_binary(name) and is_function(fun, 0) do
    {time, result} = :timer.tc(fun)
    time_ms = div(time, 1000)
    Sequin.ProcessMetrics.update_timing(name, time_ms)
    result
  end

  defp put_max_memory_bytes(%State{max_memory_bytes: nil} = state) do
    consumer = state.consumer

    max_memory_bytes = Consumers.max_memory_bytes_for_consumer(consumer)

    max_memory_bytes = div(max_memory_bytes, consumer.partition_count)
    Sequin.ProcessMetrics.gauge("max_memory_bytes", max_memory_bytes)
    %{state | max_memory_bytes: max_memory_bytes}
  end

  # This clause is when max_memory_bytes is already set in the state
  # ie. for test
  defp put_max_memory_bytes(%State{} = state) do
    state
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

  # Helper function to call a function on all partitions and aggregate the results
  defp call_all_partitions(consumer, call_name) do
    consumer
    |> partitions()
    |> Enum.reduce_while({:ok, 0}, fn partition, {:ok, acc_count} ->
      case GenServer.call(via_tuple(consumer.id, partition), call_name) do
        {:ok, count} -> {:cont, {:ok, acc_count + count}}
        error -> {:halt, error}
      end
    end)
  catch
    :exit, e ->
      {:error, exit_to_sequin_error(e)}
  end

  @decorate track_metrics("upsert_messages")
  defp upsert_messages(%State{}, []), do: :ok

  @decorate track_metrics("upsert_messages")
  defp upsert_messages(%State{} = state, messages) do
    messages
    # This value is calculated based on the number of parameters in our consumer_events/consumer_records
    # upserts and the Postgres limit of 65535 parameters per query.
    |> Enum.chunk_every(2_000)
    |> Enum.each(fn chunk ->
      {:ok, _count} = Consumers.upsert_consumer_messages(state.consumer, chunk)
    end)
  end

  defp stream_messages_into_state(%State{} = state) do
    %SinkConsumer{} = consumer = Repo.preload(state.consumer, :postgres_database)
    %PostgresDatabase{} = database = consumer.postgres_database

    # Stream messages and stop when we reach max_memory_bytes
    {time, persisted_messages} =
      :timer.tc(fn ->
        consumer
        |> Consumers.stream_consumer_messages_for_consumer()
        |> Stream.map(&Consumers.put_dynamic_fields(&1, consumer, database))
        |> Stream.filter(&(message_partition(&1, state.consumer.partition_count) == state.partition))
        |> Stream.reject(&State.message_exists?(state, &1))
        |> Enum.map(fn msg ->
          %{msg | payload_size_bytes: :erlang.external_size(msg.data)}
        end)
      end)

    duration_ms = time / 1000

    if duration_ms > 100 do
      Logger.warning("[SlotMessageStore] Loaded messages from disk (duration=#{duration_ms}ms)",
        consumer_id: state.consumer_id,
        partition: state.partition,
        message_count: length(persisted_messages),
        duration_ms: duration_ms
      )
    else
      if persisted_messages != [] do
        Logger.info("[SlotMessageStore] Loaded messages from disk",
          consumer_id: state.consumer_id,
          partition: state.partition,
          message_count: length(persisted_messages),
          duration_ms: duration_ms
        )
      end
    end

    # Now put the messages into state
    {:ok, State.put_persisted_messages(state, persisted_messages)}
  end

  @decorate track_metrics("delete_messages_noop")
  defp delete_messages(%State{}, []), do: :ok

  @decorate track_metrics("delete_messages")
  defp delete_messages(%State{} = state, messages) do
    messages
    |> Enum.chunk_every(10_000)
    |> Enum.each(fn chunk ->
      {:ok, _count} = Consumers.ack_messages(state.consumer, chunk)
    end)
  end

  defp maybe_finish_table_reader_batch(%State{} = prev_state, %State{} = state) do
    if State.unpersisted_table_reader_batch_ids(prev_state) != State.unpersisted_table_reader_batch_ids(state) do
      :syn.publish(
        :consumers,
        {:table_reader_batches_changed, state.consumer.id},
        :table_reader_batches_changed
      )
    end
  end

  def partitions(%SinkConsumer{partition_count: partition_count}) do
    Enum.to_list(0..(partition_count - 1))
  end

  defp message_partition(message, partition_count) do
    :erlang.phash2(message.group_id, partition_count)
  end

  @spec maybe_discard_messages([State.message()], non_neg_integer()) :: {[State.message()], [State.message()]}
  defp maybe_discard_messages(messages, max_retry_count) when max_retry_count != nil do
    Enum.reduce(messages, {[], []}, fn message, {discarded_messages, kept_messages} ->
      if message.deliver_count >= max_retry_count,
        do: {[%{message | state: :discarded, not_visible_until: nil} | discarded_messages], kept_messages},
        else: {discarded_messages, [message | kept_messages]}
    end)
  end

  defp maybe_discard_messages(messages, _), do: {[], messages}

  @spec handle_discarded_messages(State.t(), [State.message()]) :: :ok | {:error, Error.t()}
  defp handle_discarded_messages(_, []), do: :ok

  defp handle_discarded_messages(%State{} = state, discarded_messages) do
    with :ok <- delete_messages(state, Enum.map(discarded_messages, & &1.ack_id)) do
      AcknowledgedMessages.store_messages(state.consumer.id, discarded_messages)
    end
  end

  # Common logic for discarding messages through the existing ack path
  defp do_discard_messages(state, messages) do
    {all_ack_ids, discarded_messages} = State.prepare_messages_for_discard(messages)

    case AcknowledgedMessages.store_messages(state.consumer.id, discarded_messages) do
      :ok ->
        case handle_call({:messages_succeeded, all_ack_ids, false}, nil, state) do
          {:reply, {:ok, count, _ack_ids}, new_state} ->
            {:reply, {:ok, count}, new_state}

          other ->
            other
        end

      error ->
        {:reply, error, state}
    end
  end

  @default_flush_batch_size 2000
  defp flush_batch_size do
    conf = Application.get_env(:sequin, :slot_message_store, [])
    conf[:flush_batch_size] || @default_flush_batch_size
  end

  def process_metrics_on_log(%ProcessMetrics.Metrics{busy_percent: nil}), do: :ok

  def process_metrics_on_log(%ProcessMetrics.Metrics{} = metrics) do
    %{consumer_id: consumer_id, consumer_name: consumer_name, partition: partition} = metrics.metadata

    # Busy percent
    Prometheus.set_slot_message_store_busy_percent(consumer_id, consumer_name, partition, metrics.busy_percent)

    # Operation percent
    Enum.each(metrics.timing, fn {name, %{percent: percent}} ->
      Prometheus.set_slot_message_store_operation_percent(consumer_id, consumer_name, partition, name, percent)
    end)
  end
end

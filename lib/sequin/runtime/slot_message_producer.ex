defmodule Sequin.Runtime.SlotMessageProducer do
  @moduledoc """
  A GenServer that manages an in-memory message buffer for sink consumers and acts as a Broadway producer.

  The SlotMessageProducer serves as a central buffer between the SlotProcessor and ConsumerProcessor. It maintains an in-memory buffer for never-delivered messages for a SinkConsumer.

  Message Lifecycle:
  1. SlotProcessor pushes messages to SlotMessageProducer
  2. ConsumerProcessor pulls messages for delivery to consumers
  3. On success/failure, ConsumerProcessor acknowledges messages which removes them from SlotMessageProducer buffer
  4. If ConsumerProcessor crashes, SlotMessageProducer finds out by changing pid in the next pull

  Backpressure:
  - SlotMessageProducer buffers grow if ConsumerProcessors slow down or stop consuming
  - When buffers reach configured limits, SlotMessageProducer signals SlotProcessor to pause replication
  - SlotProcessor resumes when buffer space becomes available

  Integration Points:
  - Receives messages from SlotProcessor
  - Serves messages to ConsumerProcessors
  - Acts as a Broadway producer for pipeline implementations

  The SlotMessageProducer is designed to be ephemeral - if it crashes, SlotProcessor will need to reconnect and replay
  messages. This is handled automatically by the supervision tree.

  Note: Failed message handling is managed by the Broadway pipeline, not SlotMessageProducer. Failed messages are written
  to persistent storage and handled for redelivery attempts.
  """
  @behaviour Broadway.Producer
  @behaviour Sequin.Runtime.SlotMessageProducerBehaviour

  use GenStage

  alias Broadway.Message
  alias Ecto.Adapters.SQL.Sandbox
  alias Sequin.Consumers
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerEventData
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.ConsumerRecordData
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Error
  alias Sequin.Health
  alias Sequin.Health.Event
  alias Sequin.Postgres
  alias Sequin.Repo
  alias Sequin.Runtime.MessageLedgers
  alias Sequin.Runtime.SlotMessageProducer.State
  alias Sequin.Runtime.SlotMessageProducerBehaviour
  alias Sequin.Runtime.TableReader
  alias Sequin.TestSupport.DateTimeMock

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
      Logger.error("[SlotMessageProducer] Failed to put messages", error: error)
      raise error
  end

  @doc """
  Similar to `put_messages/2` but for a batch of messages that state will track
  as a single unit. Used for TableReaderServer.

  Should raise so TableReaderServer cannot continue if this fails.
  """
  @spec put_table_reader_batch(consumer_id(), list(ConsumerRecord.t() | ConsumerEvent.t()), TableReader.batch_id()) ::
          :ok | {:error, Error.t()}
  def put_table_reader_batch(consumer_id, messages, batch_id) do
    GenServer.call(via_tuple(consumer_id), {:put_table_reader_batch, messages, batch_id})
  catch
    :exit, e ->
      error = exit_to_sequin_error(e)
      Logger.error("[SlotMessageProducer] Failed to put table reader batch", error: error)
      raise error
  end

  @doc """
  Should raise so TableReaderServer cannot continue if this fails.
  """
  @spec unpersisted_table_reader_batch_ids(consumer_id()) :: {:ok, [TableReader.batch_id()]}
  def unpersisted_table_reader_batch_ids(consumer_id) do
    GenServer.call(via_tuple(consumer_id), :unpersisted_table_reader_batch_ids)
  catch
    :exit, e ->
      error = exit_to_sequin_error(e)
      Logger.error("[SlotMessageProducer] Failed to get batch progress", error: error)
      raise error
  end

  @doc """
  Produces the next batch of deliverable messages, up to the specified count.
  Returns `{:ok, messages}` where messages is a list of deliverable messages.
  """
  @impl SlotMessageProducerBehaviour
  def produce(consumer_id, count) do
    GenServer.call(via_tuple(consumer_id), {:produce, count})
  catch
    :exit, e ->
      {:error, exit_to_sequin_error(e)}
  end

  @doc """
  Acknowledges messages as successfully processed using their ack_ids.
  """
  @impl SlotMessageProducerBehaviour
  def messages_succeeded_returning_messages(_consumer_id, []), do: {:ok, []}

  def messages_succeeded_returning_messages(consumer_id, ack_ids) do
    GenServer.call(via_tuple(consumer_id), {:messages_succeeded, ack_ids, true})
  catch
    :exit, e ->
      {:error, exit_to_sequin_error(e)}
  end

  @impl SlotMessageProducerBehaviour
  def messages_succeeded(_consumer_id, []), do: {:ok, 0}

  def messages_succeeded(consumer_id, ack_ids) do
    GenServer.call(via_tuple(consumer_id), {:messages_succeeded, ack_ids, false})
  catch
    :exit, e ->
      {:error, exit_to_sequin_error(e)}
  end

  @doc """
  Acknowledges messages as already succeeded, likely due to idempotency checks.
  """
  @impl SlotMessageProducerBehaviour
  def messages_already_succeeded(_consumer_id, []), do: :ok

  def messages_already_succeeded(consumer_id, ack_ids) do
    GenServer.call(via_tuple(consumer_id), {:messages_already_succeeded, ack_ids})
  catch
    :exit, e ->
      {:error, exit_to_sequin_error(e)}
  end

  @impl SlotMessageProducerBehaviour
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
      Logger.error("[SlotMessageProducer] Failed to get min unflushed commit lsn", error: error)
      raise error
  end

  @doc """
  Counts the number of messages in the message store.
  """
  @spec count_messages(consumer_id()) :: {:ok, non_neg_integer()} | {:error, Exception.t()}
  def count_messages(consumer_id) do
    GenServer.call(via_tuple(consumer_id), :count_messages)
  catch
    :exit, e ->
      {:error, exit_to_sequin_error(e)}
  end

  @doc """
  Peek at SlotMessageProducer state. For use in dev/test only.
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
  Updates the consumer in state when its configuration changes.
  """
  @spec consumer_updated(SinkConsumer.t()) :: :ok | {:error, Error.t()}
  def consumer_updated(consumer) do
    GenServer.call(via_tuple(consumer.id), {:consumer_updated, consumer})
  end

  @spec payload_size_bytes(SinkConsumer.t()) :: non_neg_integer()
  def payload_size_bytes(consumer) do
    GenServer.call(via_tuple(consumer.id), :payload_size_bytes)
  end

  @doc """
  Set the monitor reference for the SlotMessageProducer into State

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

  @impl GenStage
  def init(opts) do
    consumer = opts[:consumer]
    consumer_id = if consumer, do: consumer.id, else: Keyword.fetch!(opts, :consumer_id)
    test_pid = Keyword.get(opts, :test_pid)

    Logger.metadata(consumer_id: consumer_id)
    Logger.info("[SlotMessageProducer] Initializing message store and producer for consumer #{consumer_id}")

    if test_pid do
      Sandbox.allow(Sequin.Repo, test_pid, self())
      Mox.allow(DateTimeMock, test_pid, self())
      Mox.allow(Sequin.Runtime.SlotMessageProducerMock, test_pid, self())
    end

    :syn.join(:consumers, {:messages_ingested, consumer_id}, self())

    state = %State{
      consumer: consumer,
      consumer_id: consumer_id,
      test_pid: test_pid,
      setting_system_max_memory_bytes:
        Keyword.get(opts, :setting_system_max_memory_bytes, Application.get_env(:sequin, :max_memory_bytes)),
      # Broadway producer state
      demand: 0,
      receive_timer: nil,
      trim_timer: nil,
      batch_size: Keyword.get(opts, :batch_size, 10),
      batch_timeout: Keyword.get(opts, :batch_timeout, :timer.seconds(10)),
      scheduled_handle_demand: false,
      last_logged_stats_at: nil
    }

    state =
      state
      |> schedule_receive_messages()
      |> schedule_trim_idempotency()

    Process.send_after(self(), :init, 0)
    schedule_process_logging(0)

    {:producer, state}
  end

  @impl GenStage
  def handle_call({:put_messages, messages}, from, %State{} = state) do
    execute_timed(:put_messages, fn ->
      # Validate first
      case State.validate_put_messages(state, messages) do
        {:ok, _incoming_payload_size_bytes} ->
          # Reply early since validation passed. This frees up the SlotProcessor to continue
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
            {:ok, state} = State.put_persisted_messages(state, to_persist)

            Health.put_event(state.consumer, %Event{slug: :messages_ingested, status: :success})
            :syn.publish(:consumers, {:messages_ingested, state.consumer.id}, :messages_ingested)

            if state.test_pid do
              send(state.test_pid, {:put_messages_done, state.consumer.id})
            end

            {:noreply, [], state}
          end)

        {:error, error} ->
          # Reply with error if validation fails
          {:reply, {:error, error}, state}
      end
    end)
  end

  @impl GenStage
  def handle_call({:put_table_reader_batch, messages, batch_id}, _from, %State{} = state) do
    execute_timed(:put_table_reader_batch, fn ->
      now = Sequin.utc_now()

      messages = Enum.map(messages, &%{&1 | ack_id: Sequin.uuid4(), ingested_at: now})

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
    end)
  end

  @impl GenStage
  def handle_call(:unpersisted_table_reader_batch_ids, _from, %State{} = state) do
    {:reply, {:ok, State.unpersisted_table_reader_batch_ids(state)}, state}
  end

  def handle_call({:produce, count}, _from, %State{} = state) do
    execute_timed(:produce, fn ->
      {:ok, {messages, state}} = produce_messages(state, count)

      {:reply, {:ok, messages}, state}
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

      with {:ok, state} <- State.put_persisted_messages(state, messages),
           {newly_blocked_messages, state} <- State.pop_blocked_messages(state),
           # Now put the blocked messages into persisted_messages
           {:ok, state} <- State.put_persisted_messages(state, newly_blocked_messages),
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
      :syn.publish(:consumers, {:messages_ingested, state.consumer.id}, :messages_ingested)
      {:reply, :ok, State.reset_message_visibilities(state, ack_ids)}
    end)
  end

  def handle_call(:reset_all_message_visibilities, _from, state) do
    execute_timed(:reset_all_message_visibilities, fn ->
      :syn.publish(:consumers, {:messages_ingested, state.consumer.id}, :messages_ingested)
      {:reply, :ok, State.reset_all_message_visibilities(state)}
    end)
  end

  def handle_call({:min_unpersisted_wal_cursor, monitor_ref}, _from, state) do
    execute_timed(:min_unpersisted_wal_cursor, fn ->
      if monitor_ref == state.slot_processor_monitor_ref do
        {:reply, State.min_unpersisted_wal_cursor(state), state}
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
      {:reply, state, state}
    end)
  end

  def handle_call({:peek_messages, count}, _from, state) do
    execute_timed(:peek_messages, fn ->
      {:reply, State.peek_messages(state, count), state}
    end)
  end

  def handle_call({:set_monitor_ref, ref}, _from, state) do
    execute_timed(:set_monitor_ref, fn ->
      {:reply, :ok, %{state | slot_processor_monitor_ref: ref}}
    end)
  end

  def handle_call({:consumer_updated, consumer}, from, state) do
    old_consumer = state.consumer
    state = %{state | consumer: consumer}

    # Reply early as to not block. Could take a moment to upsert messages
    GenServer.reply(from, :ok)

    cond do
      old_consumer.status == :active and consumer.status == :disabled ->
        # Get all messages from memory
        {messages, state} = State.pop_all_messages(state)

        # Persist them all to disk
        with :ok <- upsert_messages(state, messages),
             {:ok, state} <- State.put_persisted_messages(state, messages) do
          {:noreply, [], state}
        end

      old_consumer.status != :active and consumer.status == :active ->
        # Give the ConsumerProducer a kick to come fetch messages
        :syn.publish(:consumers, {:messages_ingested, state.consumer.id}, :messages_ingested)
        {:noreply, [], state}

      true ->
        {:noreply, [], state}
    end
  end

  def handle_call(:payload_size_bytes, _from, state) do
    {:reply, {:ok, state.payload_size_bytes}, state}
  end

  @impl GenStage
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

    Logger.info("[SlotMessageProducer] Process metrics", metadata)

    # Clear timing metrics after logging
    timing_metrics
    |> Keyword.keys()
    |> Enum.each(&clear_counter/1)

    schedule_process_logging()
    {:noreply, [], %{state | last_logged_stats_at: now}}
  end

  @impl GenStage
  # :syn notification
  def handle_info(:consumers_changed, %State{} = state) do
    state = put_max_memory_bytes(state)
    {:noreply, [], state}
  end

  @impl GenStage
  def handle_info(:max_memory_check, %State{} = state) do
    schedule_max_memory_check(state.setting_max_memory_check_interval)
    state = put_max_memory_bytes(state)
    {:noreply, [], state}
  end

  @impl GenStage
  def handle_info(:init, state) do
    # If we're not self-hosted, there will be no system-wide max memory bytes
    # so we don't need to recalculate max_memory_bytes on consumers changing
    if state.setting_system_max_memory_bytes do
      :syn.join(:consumers, :consumers_changed, self())
    end

    # Allow test process to access the database connection
    if state.test_pid do
      Ecto.Adapters.SQL.Sandbox.allow(Sequin.Repo, state.test_pid, self())
      Mox.allow(DateTimeMock, state.test_pid, self())
      Mox.allow(Sequin.TestSupport.UUIDMock, state.test_pid, self())
    end

    consumer =
      if state.consumer do
        state.consumer
      else
        state.consumer_id
        |> Consumers.get_sink_consumer!()
        |> Repo.lazy_preload(postgres_database: [:replication_slot])
      end

    state = %{state | consumer: consumer}

    Logger.metadata(account_id: consumer.account_id, replication_id: consumer.replication_slot_id)

    :ok = State.setup_ets(consumer)

    persisted_messages =
      consumer
      |> Consumers.list_consumer_messages_for_consumer()
      |> Enum.map(fn msg ->
        %{msg | payload_size_bytes: :erlang.external_size(msg.data)}
      end)

    {:ok, state} =
      state
      |> put_max_memory_bytes()
      |> State.put_persisted_messages(persisted_messages)

    schedule_process_logging(0)
    schedule_max_memory_check()

    {:noreply, [], state}
  end

  @impl GenStage
  def handle_info(:handle_demand, state) do
    handle_receive_messages(%{state | scheduled_handle_demand: false})
  end

  @impl GenStage
  def handle_info(:receive_messages, state) do
    new_state = schedule_receive_messages(state)
    handle_receive_messages(new_state)
  end

  @impl GenStage
  def handle_info(:messages_ingested, state) do
    new_state = maybe_schedule_demand(state)
    {:noreply, [], new_state}
  end

  @impl GenStage
  def handle_info(:trim_idempotency, state) do
    %SinkConsumer{} = consumer = state.consumer

    case Postgres.confirmed_flush_lsn(consumer.postgres_database) do
      {:ok, nil} ->
        :ok

      {:ok, lsn} ->
        MessageLedgers.trim_delivered_cursors_set(state.consumer.id, %{commit_lsn: lsn, commit_idx: 0})

      {:error, error} when is_exception(error) ->
        Logger.error("Error trimming idempotency seqs", error: Exception.message(error))

      {:error, error} ->
        Logger.error("Error trimming idempotency seqs", error: inspect(error))
    end

    {:noreply, [], schedule_trim_idempotency(state)}
  end

  defp schedule_process_logging(interval \\ :timer.seconds(30)) do
    Process.send_after(self(), :process_logging, interval)
  end

  # Can be very infrequent, as we use :syn to learn about changes to consumer count
  defp schedule_max_memory_check(interval \\ :timer.minutes(5)) do
    Process.send_after(self(), :max_memory_check, interval)
  end

  defp put_max_memory_bytes(%State{} = state) do
    consumer = state.consumer

    if state.setting_system_max_memory_bytes do
      # Must be self-hosted/dev. Check consumer's max_memory_mb setting
      consumer_count = Consumers.count_non_disabled_sink_consumers()

      max_memory_bytes =
        Consumers.max_system_memory_bytes_for_consumer(consumer, consumer_count, state.setting_system_max_memory_bytes)

      %{state | max_memory_bytes: max_memory_bytes}
    else
      %{state | max_memory_bytes: Consumers.max_memory_bytes_for_consumer(consumer)}
    end
  end

  defp exit_to_sequin_error({:timeout, {GenServer, :call, [_, _, timeout]}}) do
    Error.invariant(message: "[SlotMessageProducer] call timed out after #{timeout}ms")
  end

  defp exit_to_sequin_error({:noproc, _}) do
    Error.invariant(message: "Call to SlotMessageProducer failed with :noproc")
  end

  defp exit_to_sequin_error(e) when is_exception(e) do
    Error.invariant(message: "[SlotMessageProducer] exited with #{Exception.message(e)}")
  end

  defp exit_to_sequin_error(e) do
    Error.invariant(message: "[SlotMessageProducer] exited with #{inspect(e)}")
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
    {time, result} = :timer.tc(fun)
    # Convert microseconds to milliseconds
    incr_counter(:"#{name}_total_ms", div(time, 1000))
    result
  end

  @impl GenStage
  def handle_demand(incoming_demand, %{demand: demand} = state) do
    new_state = maybe_schedule_demand(state)
    new_state = %{new_state | demand: demand + incoming_demand}

    {:noreply, [], new_state}
  end

  @impl Broadway.Producer
  def prepare_for_draining(%{receive_timer: receive_timer, trim_timer: trim_timer} = state) do
    if receive_timer, do: Process.cancel_timer(receive_timer)
    if trim_timer, do: Process.cancel_timer(trim_timer)
    {:noreply, [], %{state | receive_timer: nil, trim_timer: nil}}
  end

  @spec ack({SinkConsumer.t(), pid(), slot_message_store_mod :: atom()}, list(Message.t()), list(Message.t())) :: :ok
  def ack({%SinkConsumer{} = consumer, test_pid, slot_message_store_mod}, successful, failed) do
    successful_messages = Enum.flat_map(successful, & &1.data)
    failed_messages = Enum.flat_map(failed, & &1.data)

    successful_ack_ids = Enum.map(successful_messages, & &1.ack_id)

    # Ack all messages in SlotMessageProducer to remove from buffer
    unless successful_ack_ids == [] do
      case slot_message_store_mod.messages_succeeded(consumer.id, successful_ack_ids) do
        {:ok, _count} -> :ok
        {:error, error} -> raise error
      end

      {:ok, _count} = Consumers.after_messages_acked(consumer, successful_messages)
    end

    failed_message_metadatas =
      failed_messages
      |> Stream.map(&Consumers.advance_delivery_state_for_failure/1)
      |> Enum.map(&%{&1 | data: nil})

    unless failed_message_metadatas == [] do
      :ok = slot_message_store_mod.messages_failed(consumer.id, failed_message_metadatas)
    end

    if test_pid do
      failed_ack_ids = Enum.map(failed_messages, & &1.ack_id)
      send(test_pid, {__MODULE__, :ack_finished, successful_ack_ids, failed_ack_ids})
    end

    :ok
  end

  @spec pre_ack_delivered_messages(SinkConsumer.t(), list(Message.t())) :: :ok
  def pre_ack_delivered_messages(consumer, broadway_messages) do
    delivered_messages = Enum.flat_map(broadway_messages, & &1.data)

    wal_cursors =
      Enum.map(delivered_messages, fn message -> %{commit_lsn: message.commit_lsn, commit_idx: message.commit_idx} end)

    :ok = MessageLedgers.wal_cursors_delivered(consumer.id, wal_cursors)
    :ok = MessageLedgers.wal_cursors_reached_checkpoint(consumer.id, "consumer_producer.ack", wal_cursors)
  end

  defp produce_messages(%State{} = state, count) do
    {messages, state} = State.produce_messages(state, count)

    messages
    |> Enum.map(&MessageLedgers.wal_cursor_from_message/1)
    |> then(&MessageLedgers.wal_cursors_reached_checkpoint(state.consumer_id, "slot_message_store.produce", &1))

    if length(messages) > 0 do
      Health.put_event(state.consumer, %Event{slug: :messages_pending_delivery, status: :success})
    end

    {:ok, {messages, state}}
  end

  defp handle_receive_messages(%{demand: demand} = state) when demand > 0 do
    execute_timed(:handle_receive_messages, fn ->
      desired_count = demand * state.batch_size
      {time, {:ok, {messages, state}}} = :timer.tc(fn -> produce_messages(state, desired_count) end)
      more_upstream_messages? = length(messages) == desired_count

      if div(time, 1000) > @min_log_time_ms do
        Logger.warning(
          "[SlotMessageProducer] produce_messages took longer than expected",
          count: length(messages),
          demand: demand,
          batch_size: state.batch_size,
          duration_ms: div(time, 1000)
        )
      end

      {time, messages} = :timer.tc(fn -> reject_delivered_messages(state, messages) end)

      if div(time, 1000) > @min_log_time_ms do
        Logger.warning(
          "[SlotMessageProducer] reject_delivered_messages took longer than expected",
          duration_ms: div(time, 1000),
          message_count: length(messages)
        )
      end

      # Cut this struct down as it will be passed to each process
      # Processes already have the consumer in context, but this is for the acknowledger. When we
      # consolidate pipelines, we can `configure_ack` to add the consumer to the acknowledger context.
      bare_consumer =
        Map.drop(state.consumer, [
          :source_tables,
          :active_backfill,
          :sequence,
          :postgres_database,
          :replication_slot,
          :account
        ])

      broadway_messages =
        messages
        |> Enum.chunk_every(state.batch_size)
        |> Enum.map(fn batch ->
          %Message{
            data: batch,
            acknowledger: {__MODULE__, {bare_consumer, state.test_pid, __MODULE__}, nil}
          }
        end)

      new_demand = demand - length(broadway_messages)
      new_demand = if new_demand < 0, do: 0, else: new_demand
      state = %{state | demand: new_demand}

      if new_demand > 0 and more_upstream_messages? do
        {:noreply, broadway_messages, maybe_schedule_demand(state)}
      else
        {:noreply, broadway_messages, state}
      end
    end)
  end

  defp handle_receive_messages(state) do
    {:noreply, [], state}
  end

  defp reject_delivered_messages(state, messages) do
    execute_timed(:reject_delivered_messages, fn ->
      wal_cursors_to_deliver =
        messages
        |> Stream.reject(fn
          # We don't enforce idempotency for read actions
          %ConsumerEvent{data: %ConsumerEventData{action: :read}} -> true
          %ConsumerRecord{data: %ConsumerRecordData{action: :read}} -> true
          # We only recently added :action to ConsumerRecordData, so we need to ignore
          # any messages that don't have it for backwards compatibility
          %ConsumerRecord{data: %ConsumerRecordData{action: nil}} -> true
          _ -> false
        end)
        |> Enum.map(fn message -> %{commit_lsn: message.commit_lsn, commit_idx: message.commit_idx} end)

      {:ok, delivered_wal_cursors} =
        MessageLedgers.filter_delivered_wal_cursors(state.consumer.id, wal_cursors_to_deliver)

      :ok = MessageLedgers.wal_cursors_delivered(state.consumer.id, delivered_wal_cursors)

      delivered_cursor_set = MapSet.new(delivered_wal_cursors)

      {delivered_messages, filtered_messages} =
        Enum.split_with(messages, fn message ->
          MapSet.member?(delivered_cursor_set, %{commit_lsn: message.commit_lsn, commit_idx: message.commit_idx})
        end)

      if delivered_messages == [] do
        filtered_messages
      else
        Logger.info(
          "[SlotMessageProducer] Rejected messages for idempotency",
          rejected_message_count: length(delivered_messages),
          commits: delivered_wal_cursors,
          message_count: length(filtered_messages)
        )

        messages_already_succeeded(
          state.consumer.id,
          Enum.map(delivered_messages, & &1.ack_id)
        )

        filtered_messages
      end
    end)
  end

  defp schedule_receive_messages(state) do
    receive_timer = Process.send_after(self(), :receive_messages, state.batch_timeout)
    %{state | receive_timer: receive_timer}
  end

  defp schedule_trim_idempotency(state) do
    trim_timer = Process.send_after(self(), :trim_idempotency, :timer.seconds(10))
    %{state | trim_timer: trim_timer}
  end

  defp maybe_schedule_demand(%{scheduled_handle_demand: false} = state) do
    Process.send_after(self(), :handle_demand, 10)
    %{state | scheduled_handle_demand: true}
  end

  defp maybe_schedule_demand(state), do: state
end

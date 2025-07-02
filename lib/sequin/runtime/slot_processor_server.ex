defmodule Sequin.Runtime.SlotProcessorServer do
  @moduledoc """
  Subscribes to the Postgres replication slot, decodes write ahead log binary messages
  and publishes them to a stream.
  """
  # See below, where we set restart: :temporary
  use GenServer

  use Sequin.ProcessMetrics,
    metric_prefix: "sequin.slot_processor_server",
    on_log: {__MODULE__, :process_metrics_on_log, []}

  use Sequin.ProcessMetrics.Decorator

  alias Ecto.Adapters.SQL.Sandbox
  alias Sequin.Constants
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Databases.ConnectionCache
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Error
  alias Sequin.Error.InvariantError
  alias Sequin.Health
  alias Sequin.Health.Event
  alias Sequin.Postgres
  alias Sequin.ProcessMetrics
  alias Sequin.Prometheus
  alias Sequin.Replication
  alias Sequin.Repo
  alias Sequin.Runtime.MessageHandler
  alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.LogicalMessage
  alias Sequin.Runtime.SlotMessageStore
  alias Sequin.Runtime.SlotProcessor
  alias Sequin.Runtime.SlotProducer.Batch
  alias Sequin.Runtime.SlotProducer.Message

  require Logger

  @backfill_batch_high_watermark Constants.backfill_batch_high_watermark()

  @logical_message_table_name Constants.logical_messages_table_name()

  defmodule State do
    @moduledoc false
    use TypedStruct

    alias Sequin.Consumers.SinkConsumer
    alias Sequin.Replication.PostgresReplicationSlot

    typedstruct do
      # Replication slot info
      field :id, String.t()
      field :postgres_database, PostgresDatabase.t()
      field :replication_slot, PostgresReplicationSlot.t()
      field :slot_name, String.t()
      field :test_pid, pid()

      # Buffers
      field :backfill_watermark_messages, [LogicalMessage.t()], default: []

      # Message handlers
      field :message_handler_ctx, any()
      field :message_handler_module, atom()
      field :message_store_refs, %{SinkConsumer.id() => reference()}, default: %{}

      # Settings
      field :heartbeat_interval, non_neg_integer()

      # Heartbeats
      field :current_heartbeat_id, nil | String.t()
      field :heartbeat_emitted_at, nil | DateTime.t()
      field :heartbeat_emitted_lsn, nil | integer()
      field :heartbeat_timer, nil | reference()
      field :heartbeat_verification_timer, nil | reference()
      field :message_received_since_last_heartbeat, boolean(), default: false

      # Reference to primary in case slot lives on replica
      field :primary_database, nil | PostgresDatabase.t()

      # Current batch tracking
      field :last_flushed_high_watermark, nil | Replication.wal_cursor()
    end
  end

  def start_link(opts) do
    id = Keyword.fetch!(opts, :id)
    slot_name = Keyword.fetch!(opts, :slot_name)
    postgres_database = Keyword.fetch!(opts, :postgres_database)

    primary_database =
      if postgres_database.primary do
        # Make sure to avoid aliasing in connectioncache!
        Map.put(PostgresDatabase.from_primary(postgres_database.primary), :id, "primaryof-#{postgres_database.id}")
      end

    test_pid = Keyword.get(opts, :test_pid)
    replication_slot = Keyword.fetch!(opts, :replication_slot)
    message_handler_module = Keyword.fetch!(opts, :message_handler_module)
    message_handler_ctx_fn = Keyword.fetch!(opts, :message_handler_ctx_fn)
    message_handler_ctx = message_handler_ctx_fn.(replication_slot)

    init = %State{
      id: id,
      slot_name: slot_name,
      postgres_database: postgres_database,
      primary_database: primary_database,
      replication_slot: replication_slot,
      test_pid: test_pid,
      message_handler_ctx: message_handler_ctx,
      message_handler_module: message_handler_module,
      heartbeat_interval: Keyword.get(opts, :heartbeat_interval, :timer.seconds(15))
    }

    GenServer.start_link(__MODULE__, init, name: via_tuple(id))
  end

  def child_spec(opts) do
    # Not used by DynamicSupervisor, but used by Supervisor in test
    id = Keyword.fetch!(opts, :id)

    spec = %{
      id: via_tuple(id),
      start: {__MODULE__, :start_link, [opts]}
    }

    # Eventually, we can wrap this in a Supervisor, to get faster retries on restarts before "giving up"
    # The Starter will eventually restart this SlotProcessorServer GenServer.
    Supervisor.child_spec(spec, restart: :permanent)
  end

  @spec update_message_handler_ctx(id :: String.t(), ctx :: any()) :: :ok | {:error, :not_running}
  def update_message_handler_ctx(id, ctx) do
    GenServer.call(via_tuple(id), {:update_message_handler_ctx, ctx})
  catch
    :exit, {:noproc, {GenServer, :call, _}} ->
      {:error, :not_running}
  end

  @spec restart_wal_cursor(id :: String.t()) :: Replication.wal_cursor()
  def restart_wal_cursor(id) do
    GenServer.call(via_tuple(id), :restart_wal_cursor)
  end

  @spec handle_batch(id :: String.t(), batch :: Batch.t()) :: :ok | :error
  def handle_batch(id, %Batch{} = batch) do
    GenServer.call(via_tuple(id), {:handle_batch, batch})
  end

  def monitor_message_store(id, consumer) do
    GenServer.call(via_tuple(id), {:monitor_message_store, consumer}, to_timeout(second: 120))
  end

  def demonitor_message_store(id, consumer_id) do
    GenServer.call(via_tuple(id), {:demonitor_message_store, consumer_id})
  end

  def via_tuple(id) do
    {:via, :syn, {:replication, {__MODULE__, id}}}
  end

  @impl GenServer
  def init(%State{} = state) do
    Logger.metadata(
      account_id: state.postgres_database.account_id,
      replication_id: state.id,
      database_id: state.postgres_database.id
    )

    Sequin.name_process({__MODULE__, state.id})

    ProcessMetrics.metadata(%{replication_id: state.id, slot_name: state.slot_name})

    Logger.info("[SlotProcessorServer] Initialized")

    if state.test_pid do
      Mox.allow(Sequin.Runtime.MessageHandlerMock, state.test_pid, self())
      Mox.allow(Sequin.TestSupport.DateTimeMock, state.test_pid, self())
      Mox.allow(Sequin.TestSupport.EnumMock, state.test_pid, self())
      Sandbox.allow(Sequin.Repo, state.test_pid, self())
    end

    state = schedule_heartbeat(state, 0)
    state = schedule_heartbeat_verification(state)
    Process.send_after(self(), :process_logging, 0)
    schedule_observe_ingestion_latency()

    {:ok, state}
  end

  @impl GenServer
  @decorate track_metrics("update_message_handler_ctx")
  def handle_call({:update_message_handler_ctx, ctx}, _from, %State{} = state) do
    state = %{state | message_handler_ctx: ctx}
    {:reply, :ok, state}
  end

  @decorate track_metrics("monitor_message_store")
  def handle_call({:monitor_message_store, consumer}, _from, state) do
    if Map.has_key?(state.message_store_refs, consumer.id) do
      {:reply, :ok, state}
    else
      # Monitor just the first partition (there's always at least one) and if any crash they will all restart
      # due to supervisor setting of :one_for_all
      pid = GenServer.whereis(SlotMessageStore.via_tuple(consumer.id, 0))
      ref = Process.monitor(pid)
      :ok = SlotMessageStore.set_monitor_ref(consumer, ref)
      Logger.info("Monitoring message store for consumer #{consumer.id}")
      {:reply, :ok, %{state | message_store_refs: Map.put(state.message_store_refs, consumer.id, ref)}}
    end
  end

  @decorate track_metrics("demonitor_message_store")
  def handle_call({:demonitor_message_store, consumer_id}, _from, state) do
    case state.message_store_refs[consumer_id] do
      nil ->
        Logger.warning("No monitor found for consumer #{consumer_id}")
        {:reply, :ok, state}

      ref ->
        res = Process.demonitor(ref)
        Logger.info("Demonitored message store for consumer #{consumer_id}: (res=#{inspect(res)})")
        {:reply, :ok, %{state | message_store_refs: Map.delete(state.message_store_refs, consumer_id)}}
    end
  end

  @decorate track_metrics("handle_batch")
  def handle_call({:handle_batch, %Batch{} = batch}, _from, %State{} = state) do
    case flush_messages(state, batch) do
      {:ok, state} ->
        {:reply, :ok, state}

      {:error, %InvariantError{code: :payload_size_limit_exceeded} = error} ->
        Logger.warning("Hit payload size limit for one or more slot message stores. Backing off.")

        {:reply, {:error, error}, state}

      {:error, error} ->
        raise error
    end
  end

  def handle_call(:restart_wal_cursor, _from, %State{} = state) do
    {:reply, restart_wal_cursor!(state), state}
  end

  @impl GenServer
  def handle_info({:DOWN, ref, :process, _pid, reason}, %State{} = state) do
    if Application.get_env(:sequin, :env) == :test and reason == :shutdown do
      {:noreply, state}
    else
      {consumer_id, ^ref} = Enum.find(state.message_store_refs, fn {_, r} -> r == ref end)

      Logger.error(
        "[SlotProcessorServer] SlotMessageStore died. Shutting down.",
        consumer_id: consumer_id,
        reason: reason
      )

      raise "SlotMessageStore died (consumer_id=#{consumer_id}, reason=#{inspect(reason)})"
    end
  end

  @decorate track_metrics("emit_heartbeat")
  def handle_info(:emit_heartbeat, %State{} = state) do
    heartbeat_id = UUID.uuid4()
    emitted_at = Sequin.utc_now()

    payload =
      Jason.encode!(%{
        id: heartbeat_id,
        emitted_at: DateTime.to_iso8601(emitted_at),
        version: "1.0"
      })

    case send_heartbeat(state, payload) do
      {:ok, lsn} ->
        Logger.info("Emitted heartbeat", heartbeat_id: heartbeat_id, emitted_at: emitted_at)

        {:noreply,
         %{
           state
           | heartbeat_timer: nil,
             current_heartbeat_id: heartbeat_id,
             heartbeat_emitted_at: emitted_at,
             heartbeat_emitted_lsn: lsn,
             message_received_since_last_heartbeat: false
         }}

      {:error,
       %Postgrex.Error{
         postgres: %{code: :undefined_table, message: "relation \"public.sequin_logical_messages\" does not exist"}
       }} ->
        Logger.warning("Heartbeat table does not exist.")
        {:noreply, schedule_heartbeat(%{state | heartbeat_timer: nil}, :timer.seconds(30))}

      {:error, error} ->
        Logger.error("Error emitting heartbeat: #{inspect(error)}")
        {:noreply, schedule_heartbeat(%{state | heartbeat_timer: nil}, :timer.seconds(10))}
    end
  end

  @max_time_between_heartbeat_emissions_min 5
  @max_time_between_heartbeat_emit_and_receive_min 10
  @decorate track_metrics("verify_heartbeat")
  def handle_info(:verify_heartbeat, %State{} = state) do
    next_state = schedule_heartbeat_verification(state)

    case verify_heartbeat(state) do
      {:ok, reason} ->
        Logger.info("[SlotProcessorServer] Heartbeat verification successful: #{reason}",
          heartbeat_id: state.current_heartbeat_id
        )

        Health.put_event(
          state.replication_slot,
          %Event{slug: :replication_heartbeat_verification, status: :success}
        )

        {:noreply, next_state}

      {:error, :no_recent_heartbeat} ->
        Logger.error(
          "[SlotProcessorServer] Heartbeat verification failed (no heartbeat emitted in last #{@max_time_between_heartbeat_emissions_min} min)",
          heartbeat_id: state.current_heartbeat_id
        )

        Health.put_event(
          state.replication_slot,
          %Event{
            slug: :replication_heartbeat_verification,
            status: :fail,
            error:
              Error.service(
                service: :replication,
                message:
                  "Replication slot connection is stale - no heartbeat emitted in last #{@max_time_between_heartbeat_emissions_min} min"
              )
          }
        )

        {:stop, :heartbeat_verification_failed, next_state}

      {:error, :no_heartbeat} ->
        Logger.error(
          "[SlotProcessorServer] Heartbeat verification failed (no heartbeat emitted)",
          heartbeat_id: state.current_heartbeat_id
        )

        Health.put_event(
          state.replication_slot,
          %Event{
            slug: :replication_heartbeat_verification,
            status: :fail,
            error: Error.service(service: :replication, message: "No heartbeat emitted")
          }
        )

        {:stop, :heartbeat_verification_failed, next_state}

      {:error, :lsn_advanced} ->
        Logger.error(
          "[SlotProcessorServer] Heartbeat verification failed - LSN has advanced past outsanding heartbeat LSN?",
          heartbeat_id: state.current_heartbeat_id
        )

        Health.put_event(
          state.replication_slot,
          %Event{
            slug: :replication_heartbeat_verification,
            status: :fail,
            error:
              Error.service(
                service: :replication,
                message:
                  "Replication slot connection not receiving heartbeats - LSN has advanced past outstanding heartbeat LSN?"
              )
          }
        )

        {:noreply, next_state}

      {:error, :stale_connection} ->
        Logger.error(
          "[SlotProcessorServer] Heartbeat verification failed (no messages or heartbeat received in last #{@max_time_between_heartbeat_emit_and_receive_min} min)",
          heartbeat_id: state.current_heartbeat_id
        )

        Health.put_event(
          state.replication_slot,
          %Event{
            slug: :replication_heartbeat_verification,
            status: :fail,
            error:
              Error.service(
                service: :replication,
                message:
                  "Replication slot connection is stale - no messages or heartbeat received in last #{@max_time_between_heartbeat_emit_and_receive_min} min"
              )
          }
        )

        {:stop, :heartbeat_verification_failed, next_state}

      {:error, :too_soon} ->
        Logger.info("[SlotProcessorServer] Heartbeat verification indeterminate (outstanding heartbeat recently emitted)",
          heartbeat_id: state.current_heartbeat_id
        )

        {:noreply, next_state}

      {:error, :no_last_commit_lsn} ->
        Logger.error(
          "[SlotProcessorServer] Heartbeat verification failed - no last commit LSN yet (no bytes yet received)",
          heartbeat_id: state.current_heartbeat_id
        )

        Health.put_event(
          state.replication_slot,
          %Event{
            slug: :replication_heartbeat_verification,
            status: :fail,
            error:
              Error.service(
                service: :replication,
                message: "Replication connection is up, but Sequin has not received any messages since connecting"
              )
          }
        )

        {:noreply, next_state}
    end
  end

  def handle_info(:observe_ingestion_latency, %State{} = state) do
    # Check if we have an outstanding heartbeat
    if not is_nil(state.current_heartbeat_id) and not is_nil(state.heartbeat_emitted_at) do
      observe_ingestion_latency(state, state.heartbeat_emitted_at)
    end

    schedule_observe_ingestion_latency()

    {:noreply, state}
  end

  # Add the handle_info callback for process_logging
  # We have to vendor this because use ReplicationConnection which does play nicely with the ProcessMetrics module
  def handle_info(:process_logging, state) do
    # This is imported by `use Sequin.ProcessMetrics`
    handle_process_logging()

    Process.send_after(self(), :process_logging, process_metrics_interval())
    {:noreply, state}
  end

  defp verify_heartbeat(%State{} = state) do
    cond do
      # No outstanding heartbeat but we have emitted one in the last few minutes
      # This is the most likely clause we hit because we usually receive the heartbeat message
      # pretty quickly after emitting it
      is_nil(state.current_heartbeat_id) and not is_nil(state.heartbeat_emitted_at) and
          Sequin.Time.after_min_ago?(state.heartbeat_emitted_at, @max_time_between_heartbeat_emissions_min) ->
        {:ok, "Last heartbeat was received"}

      # We have no outstanding heartbeat and it has been too long since we emitted one !
      # This is a bug, as we should always either be regularly emitting heartbeats or have one outstanding
      is_nil(state.current_heartbeat_id) and not is_nil(state.heartbeat_emitted_at) ->
        {:error, :no_recent_heartbeat}

      is_nil(state.current_heartbeat_id) and is_nil(state.heartbeat_emitted_at) ->
        {:error, :no_heartbeat}

      not is_nil(state.heartbeat_emitted_lsn) and not is_nil(state.last_flushed_high_watermark) and
          state.last_flushed_high_watermark.commit_lsn > state.heartbeat_emitted_lsn ->
        {:error, :lsn_advanced}

      # We have an outstanding heartbeat and we have received a message since it was emitted
      # This occurs when there is significant replication lag
      not is_nil(state.current_heartbeat_id) and state.message_received_since_last_heartbeat ->
        {:ok, "Outstanding heartbeat but messages received since it was emitted"}

      # We have an outstanding heartbeat but it was emitted less than 20s ago (too recent to verify)
      # This should only occur when latency between Sequin and the Postgres is high
      not is_nil(state.current_heartbeat_id) and
          Sequin.Time.after_min_ago?(state.heartbeat_emitted_at, @max_time_between_heartbeat_emit_and_receive_min) ->
        {:error, :too_soon}

      # We have an outstanding heartbeat but have not received the heartbeat or any other messages recently
      # This means the replication slot is somehow disconnected
      not is_nil(state.current_heartbeat_id) ->
        {:error, :stale_connection}

      is_nil(state.last_flushed_high_watermark) ->
        {:error, :no_last_commit_lsn}
    end
  end

  @heartbeat_message_prefix "sequin.heartbeat.1"
  defp send_heartbeat(%State{} = state, payload) do
    conn = get_primary_conn(state)
    pg_major_version = state.postgres_database.pg_major_version

    if is_integer(pg_major_version) and pg_major_version < 14 do
      case Postgres.upsert_logical_message(conn, state.id, @heartbeat_message_prefix, payload) do
        {:ok, lsn} -> {:ok, lsn}
        {:error, error} -> {:error, error}
      end
    else
      case Postgres.query(conn, "SELECT pg_logical_emit_message(true, '#{@heartbeat_message_prefix}', $1)", [payload]) do
        {:ok, %Postgrex.Result{rows: [[lsn_int]]}} ->
          {:ok, lsn_int}

        {:error, error} ->
          {:error, error}
      end
    end
  end

  defp schedule_heartbeat(state, interval \\ nil)

  defp schedule_heartbeat(%State{heartbeat_timer: nil} = state, interval) do
    ref = Process.send_after(self(), :emit_heartbeat, interval || state.heartbeat_interval)
    %{state | heartbeat_timer: ref}
  end

  defp schedule_heartbeat(%State{heartbeat_timer: ref} = state, _interval) when is_reference(ref) do
    state
  end

  defp schedule_heartbeat_verification(%State{} = state) do
    verification_ref = Process.send_after(self(), :verify_heartbeat, to_timeout(second: 30))
    %{state | heartbeat_verification_timer: verification_ref}
  end

  @spec flush_messages(State.t(), Batch.t()) :: {:ok, State.t()} | {:error, Error.t()}
  @decorate track_metrics("flush_messages")
  defp flush_messages(%State{} = state, %Batch{} = batch) do
    state =
      Enum.reduce(batch.messages, state, fn %Message{} = msg, state ->
        fold_message(state, msg.message)
      end)

    non_heartbeat_message? =
      Enum.any?(
        batch.messages,
        &(not match?(%Message{message: %LogicalMessage{prefix: "sequin.heartbeat" <> _rest}}, &1))
      )

    if non_heartbeat_message? do
      Health.put_event(
        state.replication_slot,
        %Event{slug: :replication_message_processed, status: :success}
      )
    end

    count = length(batch.messages)
    ProcessMetrics.increment_throughput("messages_processed", count)

    # Flush accumulated messages
    # Temp: Do this here, as handle_messages call is going to become async
    unwrapped_messages = batch.messages |> Enum.map(& &1.message) |> Enum.filter(&is_struct(&1, SlotProcessor.Message))
    state.message_handler_module.before_handle_messages(state.message_handler_ctx, unwrapped_messages)

    {time_ms, res} =
      :timer.tc(
        state.message_handler_module,
        :handle_messages,
        [state.message_handler_ctx, unwrapped_messages],
        :millisecond
      )

    state.message_handler_module.put_high_watermark_wal_cursor(state.message_handler_ctx, batch.high_watermark_wal_cursor)

    if state.test_pid do
      send(state.test_pid, {__MODULE__, :flush_messages, length(unwrapped_messages)})
    end

    state.backfill_watermark_messages
    |> Enum.reverse()
    |> Enum.each(fn %LogicalMessage{} = msg ->
      lsn = Postgres.lsn_to_int(msg.lsn)
      state.message_handler_module.handle_logical_message(state.message_handler_ctx, lsn, msg)
    end)

    if time_ms > 100 do
      Logger.warning("[SlotProcessorServer] Flushed messages took longer than 100ms",
        duration_ms: time_ms,
        message_count: count
      )
    end

    case res do
      {:ok, _count} ->
        ProcessMetrics.increment_throughput("messages_ingested", count)
        Prometheus.increment_messages_ingested(state.replication_slot.id, state.replication_slot.slot_name, count)

        state =
          %{state | backfill_watermark_messages: [], last_flushed_high_watermark: batch.high_watermark_wal_cursor}

        {:ok, state}

      {:error, %InvariantError{code: :payload_size_limit_exceeded} = error} ->
        {:error, error}

      {:error, error} ->
        raise error
    end
  end

  defp fold_message(%State{} = state, %SlotProcessor.Message{} = msg) do
    if logical_message_table_upsert?(state, msg) do
      content = Enum.find(msg.fields, fn field -> field.column_name == "content" end)
      handle_logical_message_content(state, content.value)
    else
      state
    end
  end

  defp fold_message(%State{} = state, %LogicalMessage{prefix: "sequin.heartbeat.1", content: content}) do
    handle_logical_message_content(state, content)
  end

  defp fold_message(%State{} = state, %LogicalMessage{prefix: "sequin.heartbeat.0", content: emitted_at}) do
    Logger.info("[SlotProcessorServer] Legacy heartbeat received", emitted_at: emitted_at)
    state
  end

  defp fold_message(%State{} = state, %LogicalMessage{prefix: @backfill_batch_high_watermark} = msg) do
    %State{state | backfill_watermark_messages: [msg | state.backfill_watermark_messages]}
  end

  # Ignore other logical messages
  defp fold_message(%State{} = state, %LogicalMessage{}) do
    state
  end

  defp logical_message_table_upsert?(
         %State{postgres_database: %{pg_major_version: pg_major_version}},
         %SlotProcessor.Message{table_name: @logical_message_table_name}
       )
       when pg_major_version < 14 do
    true
  end

  defp logical_message_table_upsert?(%State{}, %SlotProcessor.Message{}), do: false

  defp handle_logical_message_content(%State{} = state, content) do
    case Jason.decode(content) do
      {:ok, %{"id" => heartbeat_id, "emitted_at" => emitted_at, "version" => "1.0"}} ->
        # Only log if this is the current heartbeat we're waiting for
        if heartbeat_id == state.current_heartbeat_id do
          Logger.info("[SlotProcessorServer] Current heartbeat received",
            heartbeat_id: heartbeat_id,
            emitted_at: emitted_at
          )

          {:ok, emitted_at, _} = DateTime.from_iso8601(emitted_at)
          observe_ingestion_latency(state, emitted_at)

          Health.put_event(
            state.replication_slot,
            %Event{slug: :replication_heartbeat_received, status: :success}
          )

          if state.test_pid do
            send(state.test_pid, {__MODULE__, :heartbeat_received})
          end

          state = schedule_heartbeat(state)
          %{state | current_heartbeat_id: nil, message_received_since_last_heartbeat: false}
        else
          Logger.debug("[SlotProcessorServer] Received stale heartbeat",
            received_id: heartbeat_id,
            current_id: state.current_heartbeat_id,
            emitted_at: emitted_at
          )

          state
        end

      {:error, error} ->
        Logger.error("Error decoding heartbeat message: #{inspect(error)}")
        state
    end
  end

  defp restart_wal_cursor!(%State{} = state) do
    consumers =
      Repo.preload(state.replication_slot, :not_disabled_sink_consumers, force: true).not_disabled_sink_consumers

    # Early return with current batch high watermark if no consumers
    if Enum.empty?(consumers) do
      state.last_flushed_high_watermark
    else
      case verify_monitor_refs(state) do
        :ok ->
          consumers_with_refs =
            Enum.map(state.message_store_refs, fn {consumer_id, ref} ->
              consumer = Sequin.Enum.find!(consumers, &(&1.id == consumer_id))
              {consumer, ref}
            end)

          lows_for_message_stores =
            Sequin.TaskSupervisor
            |> Task.Supervisor.async_stream(
              consumers_with_refs,
              fn {consumer, ref} ->
                SlotMessageStore.min_unpersisted_wal_cursors(consumer, ref)
              end,
              max_concurrency: max(map_size(state.message_store_refs), 1),
              timeout: :timer.seconds(15)
            )
            |> Enum.flat_map(fn {:ok, cursors} -> cursors end)
            |> Enum.reject(&is_nil/1)

          count_stores = Enum.reduce(consumers, 0, fn %SinkConsumer{} = con, acc -> con.partition_count + acc end)

          low_for_message_stores =
            if count_stores == length(lows_for_message_stores) do
              Enum.min_by(lows_for_message_stores, &{&1.commit_lsn, &1.commit_idx})
            end

          if is_nil(low_for_message_stores) do
            # We just recently started up, and may have accumulated some messages, and may have even flushed them.
            # But our highwatermark wal cursor has not made it to all SlotMessageStores yet.
            # So, it is not safe to advance the slot.
            Logger.info("[SlotProcessorServer] no unpersisted messages in stores")

            nil
          else
            # We have flushed our highwatermark wal cursor to all SlotMessageStores at least once
            # So it's safe to use the low among all stores.
            low_for_message_stores
          end

        {:error, error} ->
          raise error
      end
    end
  end

  defp verify_monitor_refs(%State{} = state) do
    sink_consumer_ids =
      state.replication_slot
      |> Sequin.Repo.preload(:not_disabled_sink_consumers, force: true)
      |> Map.fetch!(:not_disabled_sink_consumers)
      # Filter out sink consumers that have been inserted in the last 10 minutes
      |> Enum.filter(&DateTime.before?(&1.inserted_at, DateTime.add(Sequin.utc_now(), -10, :minute)))
      |> Enum.map(& &1.id)

    monitored_sink_consumer_ids = Map.keys(state.message_store_refs)

    %MessageHandler.Context{consumers: message_handler_consumers} = state.message_handler_ctx
    message_handler_sink_consumer_ids = Enum.map(message_handler_consumers, & &1.id)

    cond do
      not Enum.all?(sink_consumer_ids, &(&1 in message_handler_sink_consumer_ids)) ->
        msg = """
        Sink consumer IDs do not match message handler sink consumer IDs.
        Sink consumers: #{inspect(sink_consumer_ids)}.
        Message handler: #{inspect(message_handler_sink_consumer_ids)}
        """

        Logger.error(msg)

        {:error, Error.invariant(message: msg)}

      not Enum.all?(sink_consumer_ids, &(&1 in monitored_sink_consumer_ids)) ->
        msg = """
        Sink consumer IDs do not match monitored sink consumer IDs.
        Sink consumers: #{inspect(sink_consumer_ids)}.
        Monitored: #{inspect(monitored_sink_consumer_ids)}
        """

        Logger.error(msg)

        {:error, Error.invariant(message: msg)}

      true ->
        :ok
    end
  end

  defp get_primary_conn(%State{} = state) do
    if is_nil(state.primary_database) do
      {:ok, conn} = ConnectionCache.connection(state.postgres_database)
      conn
    else
      {:ok, conn} = ConnectionCache.connection(state.primary_database)
      conn
    end
  end

  defp schedule_observe_ingestion_latency do
    Process.send_after(self(), :observe_ingestion_latency, to_timeout(second: 5))
  end

  defp observe_ingestion_latency(%State{} = state, ts) do
    latency_us = DateTime.diff(Sequin.utc_now(), ts, :microsecond)
    Prometheus.observe_ingestion_latency(state.replication_slot.id, state.replication_slot.slot_name, latency_us)
  end

  def process_metrics_on_log(%ProcessMetrics.Metrics{busy_percent: nil}), do: :ok

  def process_metrics_on_log(%ProcessMetrics.Metrics{} = metrics) do
    %{replication_id: replication_id, slot_name: slot_name} = metrics.metadata

    # Busy percent
    Prometheus.set_slot_processor_server_busy_percent(replication_id, slot_name, metrics.busy_percent)

    # Operation percent
    Enum.each(metrics.timing, fn {name, %{percent: percent}} ->
      Prometheus.set_slot_processor_server_operation_percent(replication_id, slot_name, name, percent)
    end)
  end
end

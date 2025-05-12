defmodule Sequin.Runtime.SlotProcessorServer do
  @moduledoc """
  Subscribes to the Postgres replication slot, decodes write ahead log binary messages
  and publishes them to a stream.
  """

  # See below, where we set restart: :temporary
  use Sequin.Postgres.ReplicationConnection

  import Sequin.Error.Guards, only: [is_error: 1]

  alias __MODULE__
  alias Ecto.Adapters.SQL.Sandbox
  alias Sequin.Constants
  alias Sequin.Databases.ConnectionCache
  alias Sequin.Databases.DatabaseUpdateWorker
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Error
  alias Sequin.Error.InvariantError
  alias Sequin.Error.ServiceError
  alias Sequin.Health
  alias Sequin.Health.Event
  alias Sequin.Postgres
  alias Sequin.Postgres.ReplicationConnection
  alias Sequin.Postgres.ValueCaster
  alias Sequin.Prometheus
  alias Sequin.Replication
  alias Sequin.Repo
  alias Sequin.Runtime.MessageHandler
  alias Sequin.Runtime.PostgresAdapter.Decoder
  alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.Begin
  alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.Commit
  alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.Delete
  alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.Insert
  alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.LogicalMessage
  alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.Origin
  alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.Relation
  alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.Update
  alias Sequin.Runtime.PostgresRelationHashCache
  alias Sequin.Runtime.SlotMessageStore
  alias Sequin.Runtime.SlotProcessor.Message
  alias Sequin.Workers.CreateReplicationSlotWorker

  require Logger

  # 100 MB
  @max_accumulated_bytes 100 * 1024 * 1024
  @max_accumulated_messages 100_000
  @backfill_batch_high_watermark Constants.backfill_batch_high_watermark()

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])
  @stream_schema Application.compile_env(:sequin, [Sequin.Repo, :stream_schema_prefix])

  @logical_message_table_name Constants.logical_messages_table_name()

  @slots_ids_with_old_postgres [
    "59d70fc1-e6a2-4c0e-9f4d-c5ced151cec1",
    "dcfba45f-d503-4fef-bb11-9221b9efa70a"
  ]

  def max_accumulated_bytes do
    Application.get_env(:sequin, :slot_processor_max_accumulated_bytes) || @max_accumulated_bytes
  end

  def max_accumulated_messages do
    Application.get_env(:sequin, :slot_processor_max_accumulated_messages) || @max_accumulated_messages
  end

  def max_accumulated_messages_time_ms do
    case Application.get_env(:sequin, :slot_processor_max_accumulated_messages_time_ms) do
      nil ->
        if Application.get_env(:sequin, :env) == :test do
          # We can lower this even more when we handle heartbeat messages sync
          # Right now, there are races where we receive a heartbeat message before our
          # regular messages get a chance to come through and be part of the batch.
          30
        else
          10
        end

      value ->
        value
    end
  end

  def set_max_accumulated_messages(value) do
    Application.put_env(:sequin, :slot_processor_max_accumulated_messages, value)
  end

  def set_max_accumulated_bytes(value) do
    Application.put_env(:sequin, :slot_processor_max_accumulated_bytes, value)
  end

  def set_max_accumulated_messages_time_ms(value) do
    Application.put_env(:sequin, :slot_processor_max_accumulated_messages_time_ms, value)
  end

  def ets_table, do: __MODULE__

  defmodule State do
    @moduledoc false
    use TypedStruct

    alias Sequin.Consumers.SinkConsumer
    alias Sequin.Replication.PostgresReplicationSlot

    typedstruct do
      # Replication slot info
      field :connection, map()
      field :id, String.t()
      field :postgres_database, PostgresDatabase.t()
      field :publication, String.t()
      field :replication_slot, PostgresReplicationSlot.t()
      field :schemas, %{}, default: %{}
      field :slot_name, String.t()
      field :test_pid, pid()

      # Current state tracking
      field :current_commit_idx, nil | integer()
      field :current_commit_ts, nil | integer()
      field :current_xaction_lsn, nil | integer()
      field :current_xid, nil | integer()
      field :last_commit_lsn, integer()
      field :connection_state, :disconnected | :streaming
      field :transaction_annotations, nil | String.t()

      # Wal cursors
      field :low_watermark_wal_cursor, Replication.wal_cursor()
      field :safe_wal_cursor_fn, (State.t() -> Replication.wal_cursor())
      field :last_flushed_wal_cursor, Replication.wal_cursor()
      field :last_lsn_acked_at, DateTime.t() | nil

      # Buffers
      field :accumulated_msg_binaries, %{count: non_neg_integer(), bytes: non_neg_integer(), binaries: [binary()]},
        default: %{count: 0, bytes: 0, binaries: []}

      field :backfill_watermark_messages, [LogicalMessage.t()], default: []
      field :flush_timer, nil | reference()

      # Message handlers
      field :message_handler_ctx, any()
      field :message_handler_module, atom()
      field :message_store_refs, %{SinkConsumer.id() => reference()}, default: %{}

      # Health and monitoring
      field :bytes_received_since_last_limit_check, non_neg_integer(), default: 0
      field :check_memory_fn, nil | (-> non_neg_integer())
      field :dirty, boolean(), default: false

      # Settings
      field :bytes_between_limit_checks, non_neg_integer()
      field :heartbeat_interval, non_neg_integer()
      field :max_memory_bytes, non_neg_integer()
      field :setting_reconnect_interval, non_neg_integer()

      # Heartbeats
      field :current_heartbeat_id, nil | String.t()
      field :heartbeat_emitted_at, nil | DateTime.t()
      field :heartbeat_timer, nil | reference()
      field :heartbeat_verification_timer, nil | reference()
      field :message_received_since_last_heartbeat, boolean(), default: false

      # Reference to primary in case slot lives on replica
      field :primary_database, nil | PostgresDatabase.t()
    end
  end

  def start_link(opts) do
    id = Keyword.fetch!(opts, :id)
    connection = Keyword.fetch!(opts, :connection)
    publication = Keyword.fetch!(opts, :publication)
    slot_name = Keyword.fetch!(opts, :slot_name)
    postgres_database = Keyword.fetch!(opts, :postgres_database)

    primary_database =
      if postgres_database.primary do
        # Make sure to avoid aliasing in connectioncache!
        Map.put(PostgresDatabase.from_primary(postgres_database.primary), :id, "primaryof-#{postgres_database.id}")
      end

    replication_slot = Keyword.fetch!(opts, :replication_slot)
    test_pid = Keyword.get(opts, :test_pid)
    message_handler_ctx = Keyword.fetch!(opts, :message_handler_ctx)
    message_handler_module = Keyword.fetch!(opts, :message_handler_module)
    max_memory_bytes = Keyword.get_lazy(opts, :max_memory_bytes, &default_max_memory_bytes/0)
    bytes_between_limit_checks = Keyword.get(opts, :bytes_between_limit_checks, div(max_memory_bytes, 100))

    rep_conn_opts =
      [name: via_tuple(id)]
      |> Keyword.merge(connection)
      # Very important. If we don't add this, ReplicationConnection will block start_link (and the
      # calling process!) while it connects.
      |> Keyword.put(:sync_connect, false)

    init = %State{
      id: id,
      publication: publication,
      slot_name: slot_name,
      postgres_database: postgres_database,
      primary_database: primary_database,
      replication_slot: replication_slot,
      test_pid: test_pid,
      message_handler_ctx: message_handler_ctx,
      message_handler_module: message_handler_module,
      connection: connection,
      last_commit_lsn: nil,
      heartbeat_interval: Keyword.get(opts, :heartbeat_interval, :timer.seconds(15)),
      max_memory_bytes: max_memory_bytes,
      bytes_between_limit_checks: bytes_between_limit_checks,
      check_memory_fn: Keyword.get(opts, :check_memory_fn, &default_check_memory_fn/0),
      safe_wal_cursor_fn: Keyword.get(opts, :safe_wal_cursor_fn, &default_safe_wal_cursor_fn/1),
      setting_reconnect_interval: Keyword.get(opts, :reconnect_interval, :timer.seconds(10))
    }

    ReplicationConnection.start_link(SlotProcessorServer, init, rep_conn_opts)
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

  def monitor_message_store(id, consumer) do
    GenServer.call(via_tuple(id), {:monitor_message_store, consumer}, :timer.seconds(120))
  end

  def demonitor_message_store(id, consumer_id) do
    GenServer.call(via_tuple(id), {:demonitor_message_store, consumer_id})
  end

  def via_tuple(id) do
    {:via, :syn, {:replication, {__MODULE__, id}}}
  end

  @impl ReplicationConnection
  def init(%State{} = state) do
    Logger.metadata(
      account_id: state.postgres_database.account_id,
      replication_id: state.id,
      database_id: state.postgres_database.id
    )

    Logger.info(
      "[SlotProcessorServer] Initialized with opts: #{inspect(Keyword.delete(state.connection, :password), pretty: true)}"
    )

    if state.test_pid do
      Mox.allow(Sequin.Runtime.SlotMessageHandlerMock, state.test_pid, self())
      Mox.allow(Sequin.TestSupport.DateTimeMock, state.test_pid, self())
      Mox.allow(Sequin.TestSupport.EnumMock, state.test_pid, self())
      Sandbox.allow(Sequin.Repo, state.test_pid, self())
    end

    state = schedule_heartbeat(state, 0)
    state = schedule_heartbeat_verification(state)
    schedule_process_logging(0)
    schedule_observe_ingestion_latency()

    {:ok, %{state | connection_state: :disconnected}}
  end

  @impl ReplicationConnection
  def handle_connect_failed(reason, %State{} = state) do
    Logger.error("[SlotProcessorServer] Failed to connect to replication slot: #{inspect(reason)}", reason: reason)
    on_connect_failure(state, reason)

    {:keep_state, state}
  end

  @impl ReplicationConnection
  def handle_connect(state) do
    Logger.debug("[SlotProcessorServer] Handling connect")

    {:ok, low_watermark_wal_cursor} = Sequin.Replication.low_watermark_wal_cursor(state.id)

    Logger.info("[SlotProcessorServer] Low watermark wal cursor for slot: #{inspect(low_watermark_wal_cursor)}")

    query =
      case state.postgres_database.pg_major_version do
        nil ->
          # Assume Postgres 14+ if we don't know the version
          Logger.warning(
            "[SlotProcessorServer] No PG major version found for database #{state.postgres_database.id}, assuming Postgres 14+"
          )

          "START_REPLICATION SLOT #{state.slot_name} LOGICAL 0/0 (proto_version '1', publication_names '#{state.publication}', messages 'true')"

        pg_major_version when pg_major_version < 14 ->
          "START_REPLICATION SLOT #{state.slot_name} LOGICAL 0/0 (proto_version '1', publication_names '#{state.publication}')"

        pg_major_version when pg_major_version >= 14 ->
          "START_REPLICATION SLOT #{state.slot_name} LOGICAL 0/0 (proto_version '1', publication_names '#{state.publication}', messages 'true')"
      end

    current_memory = state.check_memory_fn.()

    if current_memory > state.max_memory_bytes do
      Logger.warning("[SlotProcessorServer] System at memory limit, disconnecting",
        limit: state.max_memory_bytes,
        current_memory: current_memory
      )

      {:disconnect, :over_system_memory_limit, state}
    else
      {:stream, query, [],
       %{
         state
         | connection_state: :streaming,
           low_watermark_wal_cursor: low_watermark_wal_cursor
       }}
    end
  end

  @impl ReplicationConnection
  def handle_disconnect(error, reason, %State{} = state) do
    Logger.warning("[SlotProcessorServer] Disconnected from replication slot: #{inspect(error)}",
      error: error,
      reason: reason
    )

    if state.test_pid do
      send(state.test_pid, {__MODULE__, :disconnected})
    end

    case reason do
      reason when reason in [:payload_size_limit_exceeded, :over_system_memory_limit] ->
        actions = [
          {{:timeout, :reconnect}, state.setting_reconnect_interval, nil}
        ]

        state = %{
          state
          | current_commit_idx: nil,
            current_commit_ts: nil,
            current_xaction_lsn: nil,
            current_xid: nil,
            last_commit_lsn: nil,
            connection_state: :disconnected,
            accumulated_msg_binaries: %{count: 0, bytes: 0, binaries: []},
            backfill_watermark_messages: []
        }

        {:keep_state, state, actions}

      reason ->
        on_connect_failure(state, reason)

        if state.test_pid do
          send(state.test_pid, {:stop_replication, state.id})
        end

        {:stop, :replication_connection_failed, state}
    end
  end

  @impl ReplicationConnection
  def handle_result(result, state) do
    Logger.warning("Unknown result: #{inspect(result)}")
    {:keep_state, state}
  end

  @spec stop(pid) :: :ok
  def stop(pid), do: GenServer.stop(pid)

  @impl ReplicationConnection
  # Handle relation messages synchronously because we update state.schemas and there are dependencies
  # on the state.schemas in maybe_cast_message/2 which occurs async.
  def handle_data(<<?w, _header::192, ?R, _::binary>> = binary, %State{} = state) do
    <<?w, _header::192, msg::binary>> = binary
    relation_msg = Decoder.decode_message(msg)
    state = put_relation_message(relation_msg, state)
    {:keep_state, state}
  end

  def handle_data(<<?w, _header::192, msg::binary>>, %State{} = state) do
    execute_timed(:handle_data_sequin, fn ->
      raw_bytes_received = byte_size(msg)
      incr_counter(:raw_bytes_received, raw_bytes_received)
      incr_counter(:raw_bytes_received_since_last_log, raw_bytes_received)

      state = maybe_schedule_flush(state)
      state = %{state | message_received_since_last_heartbeat: true}

      state =
        Map.update!(state, :accumulated_msg_binaries, fn acc ->
          %{acc | count: acc.count + 1, bytes: acc.bytes + raw_bytes_received, binaries: [msg | acc.binaries]}
        end)

      # Update bytes processed and check limits
      with {:ok, state} <- check_limit(state, raw_bytes_received),
           {:ok, state} <- maybe_flush_messages(state) do
        {:keep_state, state}
      else
        {:error, %InvariantError{code: :payload_size_limit_exceeded}} ->
          Logger.warning("Hit payload size limit for one or more slot message stores. Disconnecting temporarily.")
          {:disconnect, :payload_size_limit_exceeded, state}

        {:error, %InvariantError{code: :over_system_memory_limit}} ->
          Health.put_event(
            state.replication_slot,
            %Event{slug: :replication_memory_limit_exceeded, status: :info}
          )

          Logger.warning("[SlotProcessorServer] System hit memory limit, shutting down",
            limit: state.max_memory_bytes,
            current_memory: state.check_memory_fn.()
          )

          {:ok, state} = flush_messages(state)

          if state.test_pid do
            send(state.test_pid, {:stop_replication, state.id})
          end

          {:disconnect, :over_system_memory_limit, state}
      end
    end)
  rescue
    e ->
      Logger.error("Error processing message: #{inspect(e)}")

      error = if is_error(e), do: e, else: Error.service(service: :replication, message: Exception.message(e))

      Health.put_event(
        state.replication_slot,
        %Event{slug: :replication_message_processed, status: :fail, error: error}
      )

      reraise e, __STACKTRACE__
  end

  # Primary keepalive message from server:
  # https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-PRIMARY-KEEPALIVE-MESSAGE
  #
  # Byte1('k')      - Identifies message as a sender keepalive
  # Int64           - Current end of WAL on the server
  # Int64           - Server's system clock (microseconds since 2000-01-01 midnight)
  # Byte1           - 1 if reply requested immediately to avoid timeout, 0 otherwise
  # The server is not asking for a reply
  def handle_data(<<?k, wal_end::64, _clock::64, 0>>, %State{} = state) do
    execute_timed(:handle_data_keepalive, fn ->
      # Because these are <14 Postgres databases, they will not receive heartbeat messages
      # temporarily mark them as healthy if we receive a keepalive message
      if state.id in @slots_ids_with_old_postgres do
        Health.put_event(
          state.replication_slot,
          %Health.Event{slug: :replication_heartbeat_received, status: :success}
        )
      end

      # Check if we should send an ack even though not requested
      if should_send_ack?(state) do
        commit_lsn = get_commit_lsn(state, wal_end)
        reply = ack_message(commit_lsn)
        state = %{state | last_lsn_acked_at: Sequin.utc_now()}
        {:keep_state_and_ack, reply, state}
      else
        {:keep_state, state}
      end
    end)
  end

  # The server is asking for a reply
  def handle_data(<<?k, wal_end::64, _clock::64, 1>>, %State{} = state) do
    execute_timed(:handle_data_keepalive, fn ->
      commit_lsn = get_commit_lsn(state, wal_end)
      reply = ack_message(commit_lsn)
      state = %{state | last_lsn_acked_at: Sequin.utc_now()}
      {:keep_state_and_ack, reply, state}
    end)
  end

  def handle_data(data, %State{} = state) do
    Logger.error("Unknown data: #{inspect(data)}")
    {:keep_state, state}
  end

  @impl ReplicationConnection
  def handle_call({:update_message_handler_ctx, ctx}, from, %State{} = state) do
    execute_timed(:update_message_handler_ctx, fn ->
      :ok = state.message_handler_module.reload_entities(ctx)
      state = %{state | message_handler_ctx: ctx}
      # Need to manually send reply
      GenServer.reply(from, :ok)
      {:keep_state, state}
    end)
  end

  @impl ReplicationConnection
  def handle_call({:monitor_message_store, consumer}, from, state) do
    execute_timed(:monitor_message_store, fn ->
      if Map.has_key?(state.message_store_refs, consumer.id) do
        GenServer.reply(from, :ok)
        {:keep_state, state}
      else
        # Monitor just the first partition (there's always at least one) and if any crash they will all restart
        # due to supervisor setting of :one_for_all
        pid = GenServer.whereis(SlotMessageStore.via_tuple(consumer.id, 0))
        ref = Process.monitor(pid)
        :ok = SlotMessageStore.set_monitor_ref(consumer, ref)
        Logger.info("Monitoring message store for consumer #{consumer.id}")
        GenServer.reply(from, :ok)
        {:keep_state, %{state | message_store_refs: Map.put(state.message_store_refs, consumer.id, ref)}}
      end
    end)
  end

  @impl ReplicationConnection
  def handle_call({:demonitor_message_store, consumer_id}, from, state) do
    execute_timed(:demonitor_message_store, fn ->
      case state.message_store_refs[consumer_id] do
        nil ->
          Logger.warning("No monitor found for consumer #{consumer_id}")
          GenServer.reply(from, :ok)
          {:keep_state, state}

        ref ->
          res = Process.demonitor(ref)
          Logger.info("Demonitored message store for consumer #{consumer_id}: (res=#{inspect(res)})")
          GenServer.reply(from, :ok)
          {:keep_state, %{state | message_store_refs: Map.delete(state.message_store_refs, consumer_id)}}
      end
    end)
  end

  @impl ReplicationConnection
  def handle_info(:flush_messages, %State{} = state) do
    execute_timed(:handle_info_flush_messages, fn ->
      case flush_messages(state) do
        {:ok, state} ->
          {:keep_state, %{state | flush_timer: nil}}

        {:error, %InvariantError{code: :payload_size_limit_exceeded}} ->
          Logger.warning("Hit payload size limit for one or more slot message stores. Disconnecting temporarily.")
          {:disconnect, :payload_size_limit_exceeded, %{state | flush_timer: nil}}

        {:error, error} ->
          raise error
      end
    end)
  end

  @impl ReplicationConnection
  def handle_info({:EXIT, _pid, :normal}, %State{} = state) do
    # Probably a Flow process
    {:keep_state, state}
  end

  def handle_info({:EXIT, _pid, :shutdown}, %State{} = state) do
    # We'll get this when we disconnect from the replication slot
    {:keep_state, state}
  end

  @impl ReplicationConnection
  def handle_info({:DOWN, ref, :process, _pid, reason}, %State{} = state) do
    if Application.get_env(:sequin, :env) == :test and reason == :shutdown do
      {:keep_state, state}
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

  @impl ReplicationConnection
  def handle_info(:emit_heartbeat, %State{id: id} = state) when id in @slots_ids_with_old_postgres do
    execute_timed(:handle_info_emit_heartbeat, fn ->
      # Carve out for individual cloud customer who still needs to upgrade to Postgres 14+
      # This heartbeat is not used for health, but rather to advance the slot even if tables are dormant.
      conn = get_primary_conn(state)

      # We can schedule right away, as we'll not be receiving a heartbeat message
      state = schedule_heartbeat(%{state | heartbeat_timer: nil})

      q =
        "insert into sequin_heartbeat(id, updated_at) values (1, now()) on conflict (id) do update set updated_at = now()"

      case Postgres.query(conn, q) do
        {:ok, _res} ->
          Logger.info("Emitted legacy heartbeat", emitted_at: Sequin.utc_now())

        {:error, error} ->
          Logger.error("Error emitting legacy heartbeat: #{inspect(error)}")
      end

      {:keep_state, state}
    end)
  end

  def handle_info(:emit_heartbeat, %State{} = state) do
    execute_timed(:emit_heartbeat, fn ->
      heartbeat_id = UUID.uuid4()
      emitted_at = Sequin.utc_now()

      payload =
        Jason.encode!(%{
          id: heartbeat_id,
          emitted_at: DateTime.to_iso8601(emitted_at),
          version: "1.0"
        })

      case send_heartbeat(state, payload) do
        :ok ->
          Logger.info("Emitted heartbeat", heartbeat_id: heartbeat_id, emitted_at: emitted_at)

          {:keep_state,
           %{
             state
             | heartbeat_timer: nil,
               current_heartbeat_id: heartbeat_id,
               heartbeat_emitted_at: emitted_at,
               message_received_since_last_heartbeat: false
           }}

        {:error,
         %Postgrex.Error{
           postgres: %{code: :undefined_table, message: "relation \"public.sequin_logical_messages\" does not exist"}
         }} ->
          Logger.warning("Heartbeat table does not exist.")
          {:keep_state, schedule_heartbeat(%{state | heartbeat_timer: nil}, :timer.seconds(30))}

        {:error, error} ->
          Logger.error("Error emitting heartbeat: #{inspect(error)}")
          {:keep_state, schedule_heartbeat(%{state | heartbeat_timer: nil}, :timer.seconds(10))}
      end
    end)
  end

  @max_time_between_heartbeat_emissions_min 5
  @max_time_between_heartbeat_emit_and_receive_min 10
  def handle_info(:verify_heartbeat, %State{} = state) do
    next_state = schedule_heartbeat_verification(state)

    cond do
      # Carve out for individual cloud customer who still needs to upgrade to Postgres 14+
      state.id in @slots_ids_with_old_postgres ->
        Health.put_event(
          state.replication_slot,
          %Event{slug: :replication_heartbeat_verification, status: :success}
        )

        {:keep_state, next_state}

      # No outstanding heartbeat but we have emitted one in the last few minutes
      # This is the most likely clause we hit because we usually receive the heartbeat message
      # pretty quickly after emitting it
      is_nil(state.current_heartbeat_id) and not is_nil(state.heartbeat_emitted_at) and
          Sequin.Time.after_min_ago?(state.heartbeat_emitted_at, @max_time_between_heartbeat_emissions_min) ->
        Logger.info("[SlotProcessorServer] Heartbeat verification successful (no outstanding heartbeat)",
          heartbeat_id: state.current_heartbeat_id
        )

        Health.put_event(
          state.replication_slot,
          %Event{slug: :replication_heartbeat_verification, status: :success}
        )

        {:keep_state, next_state}

      # We have no outstanding heartbeat and it has been too long since we emitted one !
      # This is a bug, as we should always either be regularly emitting heartbeats or have one outstanding
      is_nil(state.current_heartbeat_id) ->
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

      # We have an outstanding heartbeat and we have received a message since it was emitted
      # This occurs when there is significant replication lag
      not is_nil(state.current_heartbeat_id) and state.message_received_since_last_heartbeat ->
        Logger.info("[SlotProcessorServer] Heartbeat verification successful (messages received since last heartbeat)",
          heartbeat_id: state.current_heartbeat_id
        )

        Health.put_event(
          state.replication_slot,
          %Event{slug: :replication_heartbeat_verification, status: :success}
        )

        {:keep_state, next_state}

      # We have an outstanding heartbeat but it was emitted less than 20s ago (too recent to verify)
      # This should only occur when latency between Sequin and the Postgres is high
      not is_nil(state.current_heartbeat_id) and
          Sequin.Time.after_min_ago?(state.heartbeat_emitted_at, @max_time_between_heartbeat_emit_and_receive_min) ->
        Logger.info("[SlotProcessorServer] Heartbeat verification indeterminate (outstanding heartbeat recently emitted)",
          heartbeat_id: state.current_heartbeat_id
        )

        {:keep_state, next_state}

      # We have an outstanding heartbeat but have not received the heartbeat or any other messages recently
      # This means the replication slot is somehow disconnected
      not is_nil(state.current_heartbeat_id) ->
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
    end
  end

  @impl ReplicationConnection
  def handle_info(:process_logging, state) do
    info =
      Process.info(self(), [
        # Total memory used by process in bytes
        :memory,
        # Number of messages in queue
        :message_queue_len
      ])

    last_logged_at = Process.get(:last_logged_at)
    raw_bytes_received_since_last_log = Process.get(:raw_bytes_received_since_last_log)
    messages_processed_since_last_log = Process.get(:messages_processed_since_last_log)
    seconds_diff = if last_logged_at, do: DateTime.diff(Sequin.utc_now(), last_logged_at, :second), else: 0

    ms_since_last_logged_at =
      if last_logged_at, do: DateTime.diff(Sequin.utc_now(), last_logged_at, :millisecond)

    {messages_per_second, raw_bytes_per_second} =
      if is_integer(messages_processed_since_last_log) and is_integer(raw_bytes_received_since_last_log) and
           seconds_diff > 0 do
        {messages_processed_since_last_log / seconds_diff, raw_bytes_received_since_last_log / seconds_diff}
      else
        {0.0, 0.0}
      end

    Logger.info(
      "[SlotProcessorServer] #{Float.round(messages_per_second, 1)} messages/s, #{Sequin.String.format_bytes(raw_bytes_per_second)}/s"
    )

    # Get all timing metrics from process dictionary
    timing_metrics =
      Process.get()
      |> Enum.filter(fn {key, _} ->
        key |> to_string() |> String.ends_with?("_total_ms")
      end)
      |> Keyword.new()

    count_metrics =
      Process.get()
      |> Enum.filter(fn {key, _} ->
        key |> to_string() |> String.ends_with?("_count")
      end)
      |> Keyword.new()

    # Log all timing metrics as histograms with operation tag
    Enum.each(timing_metrics, fn {key, value} ->
      operation = key |> to_string() |> String.replace("_total_ms", "")

      Sequin.Statsd.histogram("sequin.slot_processor.operation_time_ms", value,
        tags: %{
          replication_slot_id: state.replication_slot.id,
          operation: operation
        }
      )
    end)

    unaccounted_ms =
      if ms_since_last_logged_at do
        # Calculate total accounted time
        slot_processor_metrics =
          MapSet.new([
            :handle_data_total_ms,
            :handle_data_keepalive_total_ms,
            :update_message_handler_ctx_total_ms,
            :monitor_message_store_total_ms,
            :demonitor_message_store_total_ms,
            :handle_info_emit_heartbeat_total_ms,
            :emit_heartbeat_total_ms,
            :flush_messages_total_ms
          ])

        total_accounted_ms =
          Enum.reduce(timing_metrics, 0, fn {key, value}, acc ->
            if MapSet.member?(slot_processor_metrics, key), do: acc + value, else: acc
          end)

        # Calculate unaccounted time
        max(0, ms_since_last_logged_at - total_accounted_ms)
      end

    if unaccounted_ms do
      # Log unaccounted time with same metric but different operation tag
      Sequin.Statsd.histogram("sequin.slot_processor.operation_time_ms", unaccounted_ms,
        tags: %{
          replication_slot_id: state.replication_slot.id,
          operation: "unaccounted"
        }
      )
    end

    metadata =
      [
        memory_mb: Float.round(info[:memory] / 1_024 / 1_024, 2),
        message_queue_len: info[:message_queue_len],
        accumulated_payload_size_bytes: state.accumulated_msg_binaries.bytes,
        accumulated_message_count: state.accumulated_msg_binaries.count,
        last_commit_lsn: state.last_commit_lsn,
        low_watermark_wal_cursor_lsn: state.low_watermark_wal_cursor[:commit_lsn],
        low_watermark_wal_cursor_idx: state.low_watermark_wal_cursor[:commit_idx],
        messages_processed: Process.get(:messages_processed, 0),
        raw_bytes_received: Process.get(:raw_bytes_received, 0),
        messages_per_second: messages_per_second,
        raw_bytes_per_second: raw_bytes_per_second
      ]
      |> Keyword.merge(timing_metrics)
      |> Keyword.merge(count_metrics)
      |> Sequin.Keyword.put_if_present(:unaccounted_total_ms, unaccounted_ms)

    Logger.info("[SlotProcessorServer] Process metrics", metadata)

    # Clear timing metrics after logging
    timing_metrics
    |> Keyword.keys()
    |> Enum.each(&clear_counter/1)

    count_metrics
    |> Keyword.keys()
    |> Enum.each(&clear_counter/1)

    clear_counter(:messages_processed)
    clear_counter(:messages_processed_since_last_log)
    clear_counter(:raw_bytes_received)
    clear_counter(:raw_bytes_received_since_last_log)

    Process.put(:last_logged_at, Sequin.utc_now())
    schedule_process_logging()

    {:keep_state, state}
  end

  def handle_info(:observe_ingestion_latency, %State{} = state) do
    # Check if we have an outstanding heartbeat
    if not is_nil(state.current_heartbeat_id) and not is_nil(state.heartbeat_emitted_at) do
      observe_ingestion_latency(state, state.heartbeat_emitted_at)
    end

    schedule_observe_ingestion_latency()

    {:keep_state, state}
  end

  defp on_connect_failure(%State{} = state, error) do
    conn = get_cached_conn(state)

    error_msg =
      case Postgres.fetch_replication_slot(conn, state.slot_name) do
        {:ok, %{"active" => false}} ->
          if is_exception(error) do
            Exception.message(error)
          else
            inspect(error)
          end

        {:ok, %{"active" => true}} ->
          "Replication slot '#{state.slot_name}' is currently in use by another connection"

        {:error, %Error.NotFoundError{}} ->
          maybe_recreate_slot(state)
          "Replication slot '#{state.slot_name}' does not exist"

        {:error, error} ->
          Exception.message(error)
      end

    Health.put_event(
      state.replication_slot,
      %Event{slug: :replication_connected, status: :fail, error: Error.service(service: :replication, message: error_msg)}
    )

    :ok
  end

  @heartbeat_message_prefix "sequin.heartbeat.1"
  defp send_heartbeat(%State{} = state, payload) do
    conn = get_primary_conn(state)
    pg_major_version = state.postgres_database.pg_major_version

    if is_integer(pg_major_version) and pg_major_version < 14 do
      case Postgres.upsert_logical_message(conn, state.id, @heartbeat_message_prefix, payload) do
        :ok -> :ok
        {:error, error} -> {:error, error}
      end
    else
      case Postgres.query(conn, "SELECT pg_logical_emit_message(true, '#{@heartbeat_message_prefix}', $1)", [payload]) do
        {:ok, _res} -> :ok
        {:error, error} -> {:error, error}
      end
    end
  end

  defp maybe_schedule_flush(%State{flush_timer: nil} = state) do
    ref = Process.send_after(self(), :flush_messages, max_accumulated_messages_time_ms())
    %{state | flush_timer: ref}
  end

  defp maybe_schedule_flush(%State{flush_timer: ref} = state) when is_reference(ref) do
    state
  end

  defp schedule_process_logging(interval \\ :timer.seconds(10)) do
    Process.send_after(self(), :process_logging, interval)
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
    verification_ref = Process.send_after(self(), :verify_heartbeat, :timer.seconds(30))
    %{state | heartbeat_verification_timer: verification_ref}
  end

  defp maybe_recreate_slot(%State{connection: connection} = state) do
    # Neon databases have ephemeral replication slots. At time of writing, this
    # happens after 45min of inactivity.
    # In the future, we will want to "rewind" the slot on create to last known good LSN
    if String.ends_with?(connection[:hostname], ".aws.neon.tech") do
      CreateReplicationSlotWorker.enqueue(state.id)
    end
  end

  # The receiving process can send replies back to the sender at any time, using one of the following message formats (also in the payload of a CopyData message):
  # https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-STANDBY-STATUS-UPDATE
  #
  # Byte1('r')      - Identifies message as receiver status update
  # Int64           - Last WAL byte + 1 received and written to disk
  # Int64           - Last WAL byte + 1 flushed to disk
  # Int64           - Last WAL byte + 1 applied in standby
  # Int64           - Client timestamp
  # Byte1           - 1 if reply requested immediately, 0 otherwise
  defp ack_message(lsn) when is_integer(lsn) do
    [<<?r, lsn::64, lsn::64, lsn::64, current_time()::64, 0>>]
  end

  defp skip_message?(%Message{} = msg, %State{} = state) do
    %{commit_lsn: low_watermark_lsn, commit_idx: low_watermark_idx} = state.low_watermark_wal_cursor
    {message_lsn, message_idx} = {msg.commit_lsn, msg.commit_idx}

    lte_watermark? =
      message_lsn < low_watermark_lsn or (message_lsn == low_watermark_lsn and message_idx < low_watermark_idx)

    # We'll re-receive already-flushed messages if we reconnect to the replication slot (without killing the GenServer) but haven't advanced the slot's cursor yet
    lte_flushed? =
      if state.last_flushed_wal_cursor do
        %{commit_lsn: last_flushed_lsn, commit_idx: last_flushed_idx} = state.last_flushed_wal_cursor
        message_lsn < last_flushed_lsn or (message_lsn == last_flushed_lsn and message_idx <= last_flushed_idx)
      else
        false
      end

    lte_watermark? or lte_flushed? or
      (msg.table_schema in [@config_schema, @stream_schema] and msg.table_schema != "public")
  end

  @spec put_relation_message(Relation.t(), State.t()) :: State.t()
  defp put_relation_message(
         %Relation{id: id, columns: columns, namespace: schema, name: table} = relation,
         %State{} = state
       ) do
    conn = get_cached_conn(state)

    # First, determine if this is a partition and get its parent table info
    partition_query = """
    SELECT
      p.inhparent as parent_id,
      pn.nspname as parent_schema,
      pc.relname as parent_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_inherits p ON p.inhrelid = c.oid
    JOIN pg_class pc ON pc.oid = p.inhparent
    JOIN pg_namespace pn ON pn.oid = pc.relnamespace
    WHERE c.oid = $1;
    """

    parent_info =
      case Postgres.query(conn, partition_query, [id]) do
        {:ok, %{rows: [[parent_id, parent_schema, parent_name]]}} ->
          # It's a partition, use the parent info
          %{id: parent_id, schema: parent_schema, name: parent_name}

        {:ok, %{rows: []}} ->
          # Not a partition, use its own info
          %{id: id, schema: schema, name: table}
      end

    # Get attnums for the actual table
    attnum_query = """
    with pk_columns as (
      select a.attname
      from pg_index i
      join pg_attribute a on a.attrelid = i.indrelid and a.attnum = any(i.indkey)
      where i.indrelid = $1
      and i.indisprimary
    ),
    column_info as (
      select a.attname, a.attnum,
             (select typname
              from pg_type
              where oid = case when t.typtype = 'd'
                              then t.typbasetype
                              else t.oid
                         end) as base_type
      from pg_attribute a
      join pg_type t on t.oid = a.atttypid
      where a.attrelid = $1
      and a.attnum > 0
      and not a.attisdropped
    )
    select c.attname, c.attnum, c.base_type, (pk.attname is not null) as is_pk
    from column_info c
    left join pk_columns pk on pk.attname = c.attname;
    """

    {:ok, %{rows: attnum_rows}} = Postgres.query(conn, attnum_query, [parent_info.id])

    # Enrich columns with primary key information and attnums
    enriched_columns =
      Enum.map(columns, fn %{name: name} = column ->
        case Enum.find(attnum_rows, fn [col_name, _, _, _] -> col_name == name end) do
          [_, attnum, base_type, is_pk] ->
            %{column | pk?: is_pk, attnum: attnum, type: base_type}

          nil ->
            column
        end
      end)

    # Create a relation with enriched columns
    enriched_relation = %{relation | columns: enriched_columns}

    # Compare schema hashes to detect changes
    current_hash = PostgresRelationHashCache.compute_schema_hash(enriched_relation)
    stored_hash = PostgresRelationHashCache.get_schema_hash(state.postgres_database.id, id)

    unless stored_hash == current_hash do
      Logger.info("[SlotProcessorServer] Schema changes detected for table, enqueueing database update",
        relation_id: id,
        schema: parent_info.schema,
        table: parent_info.name
      )

      PostgresRelationHashCache.update_schema_hash(state.postgres_database.id, id, current_hash)
      DatabaseUpdateWorker.enqueue(state.postgres_database.id, unique_period: 0)
    end

    # Store using the actual relation_id but with parent table info
    updated_schemas =
      Map.put(state.schemas, id, %{
        columns: enriched_columns,
        schema: parent_info.schema,
        table: parent_info.name,
        parent_table_id: parent_info.id
      })

    %{state | schemas: updated_schemas}
  end

  @spec process_message(State.t(), map()) :: State.t() | {State.t(), Message.t()}
  defp process_message(%State{last_commit_lsn: last_commit_lsn} = state, %Begin{
         commit_timestamp: ts,
         final_lsn: lsn,
         xid: xid
       }) do
    begin_lsn = Postgres.lsn_to_int(lsn)

    state =
      if not is_nil(last_commit_lsn) and begin_lsn < last_commit_lsn do
        Logger.error(
          "Received a Begin message with an LSN that is less than the last commit LSN (#{begin_lsn} < #{last_commit_lsn})"
        )

        %{state | dirty: true}
      else
        state
      end

    %State{state | current_commit_ts: ts, current_commit_idx: 0, current_xaction_lsn: begin_lsn, current_xid: xid}
  end

  # Ensure we do not have an out-of-order bug by asserting equality
  defp process_message(%State{current_xaction_lsn: current_lsn, current_commit_ts: ts, id: id} = state, %Commit{
         lsn: lsn,
         commit_timestamp: ts
       }) do
    lsn = Postgres.lsn_to_int(lsn)

    unless current_lsn == lsn do
      raise "Unexpectedly received a commit LSN that does not match current LSN (#{current_lsn} != #{lsn})"
    end

    :ets.insert(ets_table(), {{id, :last_committed_at}, ts})

    %State{
      state
      | last_commit_lsn: lsn,
        current_xaction_lsn: nil,
        current_xid: nil,
        current_commit_ts: nil,
        current_commit_idx: 0,
        dirty: false,
        transaction_annotations: nil
    }
  end

  defp process_message(%State{} = state, %LogicalMessage{prefix: "sequin:transaction_annotations.set", content: content}) do
    %{state | transaction_annotations: content}
  end

  defp process_message(%State{} = state, %LogicalMessage{prefix: "sequin:transaction_annotations.clear"}) do
    %{state | transaction_annotations: nil}
  end

  defp process_message(%State{} = state, %Message{} = msg) do
    state =
      if is_logical_message_table_upsert?(state, msg) do
        content = Enum.find(msg.fields, fn field -> field.column_name == "content" end)
        handle_logical_message_content(state, content.value)
      else
        state
      end

    msg = %Message{
      msg
      | commit_lsn: state.current_xaction_lsn,
        commit_idx: state.current_commit_idx,
        commit_timestamp: state.current_commit_ts,
        transaction_annotations: state.transaction_annotations
    }

    # TracerServer.message_replicated(state.postgres_database, msg)

    {%State{state | current_commit_idx: state.current_commit_idx + 1}, msg}
  end

  # Ignore type messages, we receive them before type columns:
  # %Sequin.Extensions.PostgresAdapter.Decoder.Messages.Type{id: 551312, namespace: "public", name: "citext"}
  # Custom enum:
  # %Sequin.Extensions.PostgresAdapter.Decoder.Messages.Type{id: 3577319, namespace: "public", name: "character_status"}
  defp process_message(%State{} = state, %Decoder.Messages.Type{}) do
    state
  end

  defp process_message(%State{} = state, %LogicalMessage{prefix: "sequin.heartbeat.1", content: content}) do
    handle_logical_message_content(state, content)
  end

  defp process_message(%State{} = state, %LogicalMessage{prefix: "sequin.heartbeat.0", content: emitted_at}) do
    Logger.info("[SlotProcessorServer] Legacy heartbeat received", emitted_at: emitted_at)
    state
  end

  defp process_message(%State{} = state, %LogicalMessage{prefix: @backfill_batch_high_watermark} = msg) do
    %State{state | backfill_watermark_messages: [msg | state.backfill_watermark_messages]}
  end

  # Ignore other logical messages
  defp process_message(%State{} = state, %LogicalMessage{}) do
    state
  end

  defp process_message(%State{} = state, %Origin{}) do
    state
  end

  # It's important we assert this message is not a message that we *should* have a handler for
  defp process_message(%State{} = state, %struct{} = msg)
       when struct not in [Begin, Commit, Message, LogicalMessage, Relation] do
    Logger.error("Unknown message: #{inspect(msg)}")
    state
  end

  defp is_logical_message_table_upsert?(%State{postgres_database: %{pg_major_version: pg_major_version}}, %Message{
         table_name: @logical_message_table_name
       })
       when pg_major_version < 14 do
    true
  end

  defp is_logical_message_table_upsert?(%State{}, %Message{}), do: false

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

  @spec cast_message(decoded_message :: map(), schemas :: map()) :: Message.t() | map()
  defp cast_message(%Insert{} = msg, schemas) do
    %{columns: columns, schema: schema, table: table, parent_table_id: parent_table_id} =
      Map.get(schemas, msg.relation_id)

    ids = data_tuple_to_ids(columns, msg.tuple_data)

    %Message{
      action: :insert,
      errors: nil,
      ids: ids,
      table_schema: schema,
      table_name: table,
      table_oid: parent_table_id,
      fields: data_tuple_to_fields(ids, columns, msg.tuple_data),
      trace_id: UUID.uuid4()
    }
  end

  defp cast_message(%Update{} = msg, schemas) do
    %{columns: columns, schema: schema, table: table, parent_table_id: parent_table_id} =
      Map.get(schemas, msg.relation_id)

    ids = data_tuple_to_ids(columns, msg.tuple_data)

    old_fields =
      if msg.old_tuple_data do
        data_tuple_to_fields(ids, columns, msg.old_tuple_data)
      end

    %Message{
      action: :update,
      errors: nil,
      ids: ids,
      table_schema: schema,
      table_name: table,
      table_oid: parent_table_id,
      old_fields: old_fields,
      fields: data_tuple_to_fields(ids, columns, msg.tuple_data),
      trace_id: UUID.uuid4()
    }
  end

  defp cast_message(%Delete{} = msg, schemas) do
    %{columns: columns, schema: schema, table: table, parent_table_id: parent_table_id} =
      Map.get(schemas, msg.relation_id)

    prev_tuple_data =
      if msg.old_tuple_data do
        msg.old_tuple_data
      else
        msg.changed_key_tuple_data
      end

    ids = data_tuple_to_ids(columns, prev_tuple_data)

    %Message{
      action: :delete,
      errors: nil,
      ids: ids,
      table_schema: schema,
      table_name: table,
      table_oid: parent_table_id,
      old_fields: data_tuple_to_fields(ids, columns, prev_tuple_data),
      trace_id: UUID.uuid4()
    }
  end

  defp maybe_flush_messages(%State{} = state) do
    should_flush? =
      state.accumulated_msg_binaries.count > max_accumulated_messages() or
        state.accumulated_msg_binaries.bytes > max_accumulated_bytes()

    if should_flush? do
      flush_messages(state)
    else
      {:ok, state}
    end
  end

  @spec flush_messages(State.t()) :: {:ok, State.t()} | {:error, Error.t()}
  defp flush_messages(%State{} = state) do
    execute_timed(:flush_messages, fn ->
      if ref = state.flush_timer do
        Process.cancel_timer(ref)
      end

      state = %{state | flush_timer: nil}

      accumulated_binares = state.accumulated_msg_binaries.binaries
      schemas = state.schemas

      # TODO: Move to an after_connect callback after we augment ReplicationConnection
      Health.put_event(
        state.replication_slot,
        %Event{slug: :replication_connected, status: :success}
      )

      messages =
        accumulated_binares
        |> Enum.reverse()
        |> Enum.with_index()
        |> Flow.from_enumerable(max_demand: 50, min_demand: 25)
        |> Flow.map(fn {msg, idx} ->
          case Decoder.decode_message(msg) do
            %type{} = msg when type in [Insert, Update, Delete] ->
              msg = cast_message(msg, schemas)
              {msg, idx}

            msg ->
              {msg, idx}
          end
        end)
        # Merge back to single partition - always use 1 stage to ensure ordering
        |> Flow.partition(stages: 1)
        # Sort by original index
        |> Enum.sort_by(&elem(&1, 1))
        # Extract just the messages
        |> Enum.map(&elem(&1, 0))

      if Enum.any?(messages, &(not match?(%LogicalMessage{prefix: "sequin.heartbeat.0"}, &1))) do
        # A replication message is *their* message(s), not our message.
        Health.put_event(
          state.replication_slot,
          %Event{slug: :replication_message_processed, status: :success}
        )
      end

      {state, messages} =
        Enum.reduce(messages, {state, []}, fn msg, {state, messages} ->
          case process_message(state, msg) do
            {%State{} = state, %Message{} = message} ->
              if skip_message?(message, state) do
                {state, messages}
              else
                {state, [message | messages]}
              end

            %State{} = state ->
              {state, messages}
          end
        end)

      last_message = List.first(messages)
      messages = Enum.reverse(messages)

      count = length(messages)
      incr_counter(:messages_processed, count)
      incr_counter(:messages_processed_since_last_log, count)

      # Flush accumulated messages
      # Temp: Do this here, as handle_messages call is going to become async
      state.message_handler_module.before_handle_messages(state.message_handler_ctx, messages)

      {time_ms, res} =
        :timer.tc(
          fn ->
            state.message_handler_module.handle_messages(state.message_handler_ctx, messages)
          end,
          :millisecond
        )

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
        :ok ->
          Prometheus.increment_messages_ingested(state.replication_slot.id, state.replication_slot.slot_name, count)

          if state.test_pid do
            state.message_handler_module.flush_messages(state.message_handler_ctx)
            send(state.test_pid, {__MODULE__, :flush_messages})
          end

          last_flushed_wal_cursor =
            if last_message do
              %Message{commit_lsn: commit_lsn, commit_idx: commit_idx} = last_message

              %{commit_lsn: commit_lsn, commit_idx: commit_idx}
            else
              state.last_flushed_wal_cursor
            end

          state = %{
            state
            | accumulated_msg_binaries: %{count: 0, bytes: 0, binaries: []},
              last_flushed_wal_cursor: last_flushed_wal_cursor,
              backfill_watermark_messages: []
          }

          {:ok, state}

        {:error, %InvariantError{code: :payload_size_limit_exceeded} = error} ->
          {:error, error}

        {:error, error} ->
          raise error
      end
    end)
  end

  defp default_safe_wal_cursor_fn(%State{last_commit_lsn: nil}),
    do: raise("Unsafe to call safe_wal_cursor when last_commit_lsn is nil")

  defp default_safe_wal_cursor_fn(%State{} = state) do
    consumers =
      Repo.preload(state.replication_slot, :not_disabled_sink_consumers, force: true).not_disabled_sink_consumers

    with :ok <- verify_messages_flushed(state),
         :ok <- verify_monitor_refs(state) do
      low_for_message_stores =
        state.message_store_refs
        |> Enum.flat_map(fn {consumer_id, ref} ->
          consumer = Sequin.Enum.find!(consumers, &(&1.id == consumer_id))
          SlotMessageStore.min_unpersisted_wal_cursors(consumer, ref)
        end)
        |> Enum.filter(& &1)
        |> case do
          [] -> nil
          cursors -> Enum.min_by(cursors, &{&1.commit_lsn, &1.commit_idx})
        end

      cond do
        not is_nil(low_for_message_stores) ->
          # Use the minimum unpersisted WAL cursor from the message stores.
          Logger.info(
            "[SlotProcessorServer] safe_wal_cursor/1: low_for_message_stores=#{inspect(low_for_message_stores)}"
          )

          low_for_message_stores

        accumulated_messages?(state) ->
          # When there are messages that the SlotProcessorServer has not flushed yet,
          # we need to fallback on the last low_watermark_wal_cursor (not safe to use
          # the last_commit_lsn, as it has not been flushed or processed by SlotMessageStores yet)
          Logger.info(
            "[SlotProcessorServer] safe_wal_cursor/1: state.low_watermark_wal_cursor=#{inspect(state.low_watermark_wal_cursor)}"
          )

          state.low_watermark_wal_cursor

        true ->
          # The SlotProcessorServer has processed messages beyond what the message stores have.
          # This might be due to health messages or messages for tables that do not belong
          # to any sinks.
          # We want to advance the slot to the last_commit_lsn in that case because:
          # 1. It's safe to do.
          # 2. If the tables in this slot are dormant, the slot will continue to accumulate
          # WAL unless we advance it. (This is the secondary purpose of the health message,
          # to allow us to advance the slot even if tables are dormant.)
          Logger.info("[SlotProcessorServer] safe_wal_cursor/1: state.last_commit_lsn=#{inspect(state.last_commit_lsn)}")
          %{commit_lsn: state.last_commit_lsn, commit_idx: 0}
      end
    else
      {:error, error} ->
        raise error
    end
  end

  defp accumulated_messages?(%State{accumulated_msg_binaries: %{count: count}}) when is_integer(count) do
    count > 0
  end

  defp verify_monitor_refs(%State{} = state) do
    sink_consumer_ids =
      state.replication_slot
      |> Sequin.Repo.preload(:not_disabled_sink_consumers, force: true)
      |> Map.fetch!(:not_disabled_sink_consumers)
      |> Enum.map(& &1.id)
      |> Enum.sort()

    monitored_sink_consumer_ids = Enum.sort(Map.keys(state.message_store_refs))

    %MessageHandler.Context{consumers: message_handler_consumers} = state.message_handler_ctx
    message_handler_sink_consumer_ids = Enum.sort(Enum.map(message_handler_consumers, & &1.id))

    cond do
      sink_consumer_ids != message_handler_sink_consumer_ids ->
        {:error,
         Error.invariant(
           message: """
           Sink consumer IDs do not match message handler sink consumer IDs.
           Sink consumers: #{inspect(sink_consumer_ids)}.
           Message handler: #{inspect(message_handler_sink_consumer_ids)}
           """
         )}

      sink_consumer_ids != monitored_sink_consumer_ids ->
        {:error,
         Error.invariant(
           message: """
           Sink consumer IDs do not match monitored sink consumer IDs.
           Sink consumers: #{inspect(sink_consumer_ids)}.
           Monitored: #{inspect(monitored_sink_consumer_ids)}
           """
         )}

      true ->
        :ok
    end
  end

  defp verify_messages_flushed(%State{} = state) do
    state.message_handler_module.flush_messages(state.message_handler_ctx)
  end

  def data_tuple_to_ids(columns, tuple_data) do
    columns
    |> Enum.zip(Tuple.to_list(tuple_data))
    |> Enum.filter(fn {col, _} -> col.pk? end)
    # Very important - the system expects the PKs to be sorted by attnum
    |> Enum.sort_by(fn {col, _} -> col.attnum end)
    |> Enum.map(fn {_, value} -> value end)
  end

  @spec data_tuple_to_fields(list(), [map()], tuple()) :: [Message.Field.t()]
  def data_tuple_to_fields(id_list, columns, tuple_data) do
    columns
    |> Enum.zip(Tuple.to_list(tuple_data))
    |> Enum.map(fn {%{name: name, attnum: attnum, type: type}, value} ->
      case ValueCaster.cast(type, value) do
        {:ok, casted_value} ->
          %Message.Field{
            column_name: name,
            column_attnum: attnum,
            value: casted_value
          }

        {:error, %ServiceError{code: :invalid_json} = error} ->
          details = Map.put(error.details, :ids, id_list)

          raise %{
            error
            | details: details,
              message: error.message <> " for column `#{name}` in row with ids: #{inspect(id_list)}"
          }
      end
    end)
  end

  @epoch DateTime.to_unix(~U[2000-01-01 00:00:00Z], :microsecond)
  defp current_time, do: System.os_time(:microsecond) - @epoch

  # Add this function to get the last committed timestamp
  def get_last_committed_at(id) do
    case :ets.lookup(ets_table(), {id, :last_committed_at}) do
      [{{^id, :last_committed_at}, ts}] -> ts
      [] -> nil
    end
  end

  defp get_cached_conn(%State{} = state) do
    {:ok, conn} = ConnectionCache.connection(state.postgres_database)
    conn
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

  # Give the system 3 seconds to lower memory
  @check_limit_attempts 30
  @check_limit_interval 100
  defp check_limit(state, raw_bytes_received, attempt \\ 1)

  defp check_limit(_state, _raw_bytes_received, attempt) when attempt > @check_limit_attempts do
    {:error, Error.invariant(message: "Memory limit exceeded", code: :over_system_memory_limit)}
  end

  defp check_limit(%State{} = state, raw_bytes_received, attempt) do
    # Check if it's been a while since we last checked the limit
    if state.bytes_received_since_last_limit_check + raw_bytes_received >= state.bytes_between_limit_checks do
      current_memory = state.check_memory_fn.()

      if current_memory >= state.max_memory_bytes do
        Process.sleep(@check_limit_interval)
        check_limit(state, raw_bytes_received, attempt + 1)
      else
        {:ok, %{state | bytes_received_since_last_limit_check: 0}}
      end
    else
      new_bytes = state.bytes_received_since_last_limit_check + raw_bytes_received
      {:ok, %{state | bytes_received_since_last_limit_check: new_bytes}}
    end
  end

  defp default_check_memory_fn do
    :erlang.memory(:total)
  end

  defp default_max_memory_bytes do
    Application.get_env(:sequin, :max_memory_bytes)
  end

  defp incr_counter(name, amount \\ 1) do
    current = Process.get(name, 0)
    Process.put(name, current + amount)
  end

  defp clear_counter(name) do
    Process.delete(name)
  end

  defp execute_timed(name, fun) do
    {time, result} = :timer.tc(fun, :millisecond)
    incr_counter(:"#{name}_total_ms", time)
    incr_counter(:"#{name}_count")
    result
  end

  # Helper function to determine if we should send an ack
  defp should_send_ack?(%State{last_lsn_acked_at: nil}), do: true

  defp should_send_ack?(%State{last_lsn_acked_at: last_acked_at}) do
    DateTime.diff(Sequin.utc_now(), last_acked_at, :second) > 60
  end

  # Encapsulate commit LSN logic
  defp get_commit_lsn(%State{last_commit_lsn: nil} = state, wal_end) do
    # If we don't have a last_commit_lsn, we're still processing the first xaction
    # we received on boot. This can happen if we're processing a very large xaction.
    # It is therefore safe to send an ack with the last LSN we processed.
    {:ok, low_watermark} = Replication.low_watermark_wal_cursor(state.id)

    if low_watermark.commit_lsn > wal_end do
      Logger.warning("Server LSN #{wal_end} is behind our LSN #{low_watermark.commit_lsn}")
    end

    Logger.info(
      "Acking LSN #{inspect(low_watermark.commit_lsn)} (last_commit_lsn is nil) (current server LSN: #{wal_end})"
    )

    low_watermark.commit_lsn
  end

  defp get_commit_lsn(%State{} = state, wal_end) do
    # With our current LSN increment strategy, we'll always replay the last record on boot. It seems
    # safe to increment the last_commit_lsn by 1 (Commit also contains the next LSN)
    wal_cursor = state.safe_wal_cursor_fn.(state)
    Logger.info("Acking LSN #{inspect(wal_cursor)} (current server LSN: #{wal_end})")

    Replication.put_low_watermark_wal_cursor!(state.id, wal_cursor)

    if wal_cursor.commit_lsn > wal_end do
      Logger.warning("Server LSN #{wal_end} is behind our LSN #{wal_cursor.commit_lsn}")
    end

    wal_cursor.commit_lsn
  end

  defp schedule_observe_ingestion_latency do
    Process.send_after(self(), :observe_ingestion_latency, :timer.seconds(5))
  end

  defp observe_ingestion_latency(%State{} = state, ts) do
    latency_us = DateTime.diff(Sequin.utc_now(), ts, :microsecond)
    Prometheus.observe_ingestion_latency(state.replication_slot.id, state.replication_slot.slot_name, latency_us)
  end
end

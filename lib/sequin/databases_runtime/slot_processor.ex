defmodule Sequin.DatabasesRuntime.SlotProcessor do
  @moduledoc """
  Subscribes to the Postgres replication slot, decodes write ahead log binary messages
  and publishes them to a stream.

  Forked from https://github.com/supabase/realtime/blob/main/lib/extensions/postgres_cdc_stream/replication.ex
  with many modifications.
  """

  # See below, where we set restart: :temporary
  use Sequin.Postgres.ReplicationConnection

  import Sequin.Error.Guards, only: [is_error: 1]

  alias __MODULE__
  alias Ecto.Adapters.SQL.Sandbox
  alias Sequin.Databases.ConnectionCache
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.DatabasesRuntime
  alias Sequin.DatabasesRuntime.PostgresAdapter.Decoder
  alias Sequin.DatabasesRuntime.PostgresAdapter.Decoder.Messages.Begin
  alias Sequin.DatabasesRuntime.PostgresAdapter.Decoder.Messages.Commit
  alias Sequin.DatabasesRuntime.PostgresAdapter.Decoder.Messages.Delete
  alias Sequin.DatabasesRuntime.PostgresAdapter.Decoder.Messages.Insert
  alias Sequin.DatabasesRuntime.PostgresAdapter.Decoder.Messages.LogicalMessage
  alias Sequin.DatabasesRuntime.PostgresAdapter.Decoder.Messages.Relation
  alias Sequin.DatabasesRuntime.PostgresAdapter.Decoder.Messages.Update
  alias Sequin.DatabasesRuntime.SlotMessageStore
  alias Sequin.DatabasesRuntime.SlotProcessor.Message
  alias Sequin.DatabasesRuntime.SlotProcessor.MessageHandler
  alias Sequin.Error
  alias Sequin.Error.ServiceError
  alias Sequin.Health
  alias Sequin.Health.Event
  alias Sequin.Postgres
  alias Sequin.Postgres.ReplicationConnection
  alias Sequin.Replication
  alias Sequin.Tracer.Server, as: TracerServer
  alias Sequin.Workers.CreateReplicationSlotWorker

  require Logger

  # 200MB
  @max_accumulated_bytes 200 * 1024 * 1024

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])
  @stream_schema Application.compile_env(:sequin, [Sequin.Repo, :stream_schema_prefix])

  def ets_table, do: __MODULE__

  defmodule State do
    @moduledoc false
    use TypedStruct

    alias Sequin.Consumers.SinkConsumer
    alias Sequin.Replication.PostgresReplicationSlot

    typedstruct do
      field :current_commit_ts, nil | integer()
      field :current_commit_idx, nil | integer()
      field :current_xaction_lsn, nil | integer()
      field :current_xid, nil | integer()
      field :message_handler_ctx, any()
      field :message_handler_module, atom()
      field :id, String.t()
      field :last_commit_lsn, integer()
      field :last_processed_commit_tuple, {integer(), integer()}
      field :publication, String.t()
      field :slot_name, String.t()
      field :message_store_refs, %{SinkConsumer.id() => reference()}, default: %{}
      field :postgres_database, PostgresDatabase.t()
      field :replication_slot, PostgresReplicationSlot.t()
      field :step, :disconnected | :streaming
      field :test_pid, pid()
      field :connection, map()
      field :schemas, %{}, default: %{}
      field :accumulated_messages, {non_neg_integer(), [Message.t()]}, default: {0, []}
      field :connect_attempts, non_neg_integer(), default: 0
      field :dirty, boolean(), default: false
      field :heartbeat_interval, non_neg_integer()
    end
  end

  def start_link(opts) do
    id = Keyword.fetch!(opts, :id)
    connection = Keyword.fetch!(opts, :connection)
    publication = Keyword.fetch!(opts, :publication)
    slot_name = Keyword.fetch!(opts, :slot_name)
    postgres_database = Keyword.fetch!(opts, :postgres_database)
    replication_slot = Keyword.fetch!(opts, :replication_slot)
    test_pid = Keyword.get(opts, :test_pid)
    message_handler_ctx = Keyword.get(opts, :message_handler_ctx)
    message_handler_module = Keyword.fetch!(opts, :message_handler_module)

    rep_conn_opts =
      [auto_reconnect: true, name: via_tuple(id)]
      |> Keyword.merge(connection)
      # Very important. If we don't add this, ReplicationConnection will block start_link (and the
      # calling process!) while it connects.
      |> Keyword.put(:sync_connect, false)

    init = %State{
      id: id,
      publication: publication,
      slot_name: slot_name,
      postgres_database: postgres_database,
      replication_slot: replication_slot,
      test_pid: test_pid,
      message_handler_ctx: message_handler_ctx,
      message_handler_module: message_handler_module,
      connection: connection,
      last_commit_lsn: nil,
      heartbeat_interval: Keyword.get(opts, :heartbeat_interval, :timer.minutes(1))
    }

    ReplicationConnection.start_link(SlotProcessor, init, rep_conn_opts)
  end

  def child_spec(opts) do
    # Not used by DynamicSupervisor, but used by Supervisor in test
    id = Keyword.fetch!(opts, :id)

    spec = %{
      id: via_tuple(id),
      start: {__MODULE__, :start_link, [opts]}
    }

    # Eventually, we can wrap this in a Supervisor, to get faster retries on restarts before "giving up"
    # The Starter will eventually restart this SlotProcessor GenServer.
    Supervisor.child_spec(spec, restart: :temporary)
  end

  @spec update_message_handler_ctx(id :: String.t(), ctx :: any()) :: :ok | {:error, :not_running}
  def update_message_handler_ctx(id, ctx) do
    GenServer.call(via_tuple(id), {:update_message_handler_ctx, ctx})
  catch
    :exit, {:noproc, {GenServer, :call, _}} ->
      {:error, :not_running}
  end

  def monitor_message_store(id, consumer_id) do
    GenServer.call(via_tuple(id), {:monitor_message_store, consumer_id}, :timer.seconds(120))
  end

  def demonitor_message_store(id, consumer_id) do
    GenServer.call(via_tuple(id), {:demonitor_message_store, consumer_id})
  end

  def via_tuple(id) do
    {:via, :syn, {:replication, {__MODULE__, id}}}
  end

  @impl ReplicationConnection
  def init(%State{} = state) do
    Logger.metadata(account_id: state.postgres_database.account_id, replication_id: state.id)

    Logger.info(
      "[SlotProcessor] Initialized with opts: #{inspect(Keyword.delete(state.connection, :password), pretty: true)}"
    )

    if state.test_pid do
      Mox.allow(Sequin.DatabasesRuntime.MessageHandlerMock, state.test_pid, self())
      Mox.allow(Sequin.TestSupport.DateTimeMock, state.test_pid, self())
      Sandbox.allow(Sequin.Repo, state.test_pid, self())
    end

    Process.send_after(self(), :emit_heartbeat, 0)
    schedule_process_logging()

    {:ok, %{state | step: :disconnected}}
  end

  @impl ReplicationConnection
  def handle_connect(%State{connect_attempts: attempts} = state) when attempts >= 5 do
    Logger.error("[SlotProcessor] Failed to connect to replication slot after 5 attempts")

    conn = get_cached_conn(state)

    error_msg =
      case Postgres.fetch_replication_slot(conn, state.slot_name) do
        {:ok, %{"active" => false}} ->
          "Failed to connect to replication slot after 5 attempts"

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

    # No way to stop by returning a {:stop, reason} tuple from handle_connect
    %Task{ref: ref} =
      Task.Supervisor.async_nolink(Sequin.TaskSupervisor, fn ->
        if env() == :test and not is_nil(state.test_pid) do
          send(state.test_pid, {:stop_replication, state.id})
        else
          DatabasesRuntime.Supervisor.stop_replication(state.id)
        end
      end)

    Process.demonitor(ref, [:flush])

    {:noreply, state}
  end

  def handle_connect(state) do
    Logger.debug("[SlotProcessor] Handling connect (attempt #{state.connect_attempts + 1})")

    {:ok, last_processed_commit_tuple} = Sequin.Replication.last_processed_commit_tuple(state.id)

    query =
      if state.id in ["59d70fc1-e6a2-4c0e-9f4d-c5ced151cec1", "dcfba45f-d503-4fef-bb11-9221b9efa70a"] do
        "START_REPLICATION SLOT #{state.slot_name} LOGICAL 0/0 (proto_version '1', publication_names '#{state.publication}')"
      else
        "START_REPLICATION SLOT #{state.slot_name} LOGICAL 0/0 (proto_version '1', publication_names '#{state.publication}', messages 'true')"
      end

    {:stream, query, [],
     %{
       state
       | step: :streaming,
         connect_attempts: state.connect_attempts + 1,
         last_processed_commit_tuple: last_processed_commit_tuple
     }}
  end

  @impl ReplicationConnection
  def handle_result(result, state) do
    Logger.warning("Unknown result: #{inspect(result)}")
    {:noreply, state}
  end

  @spec stop(pid) :: :ok
  def stop(pid), do: GenServer.stop(pid)

  @impl ReplicationConnection
  def handle_data(<<?w, _header::192, msg::binary>>, %State{} = state) do
    # TODO: Move to better spot after we vendor ReplicationConnection
    Health.put_event(
      state.replication_slot,
      %Event{slug: :replication_connected, status: :success}
    )

    msg = Decoder.decode_message(msg)

    next_state =
      if skip_message?(msg, state) do
        state
      else
        msg
        |> maybe_cast_message(state)
        |> process_message(state)
      end

    {acc_size, _acc_messages} = next_state.accumulated_messages

    cond do
      is_struct(msg, Commit) ->
        if next_state.dirty or state.dirty, do: Logger.warning("Got a commit message while DIRTY")

        next_state = flush_messages(next_state)
        {:noreply, next_state}

      acc_size > @max_accumulated_bytes ->
        next_state = flush_messages(next_state)
        {:noreply, next_state}

      true ->
        {:noreply, next_state}
    end
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
  def handle_data(<<?k, wal_end::64, _clock::64, reply>>, %State{} = state) do
    # Because these are <14 Postgres databases, they will not receive heartbeat messages
    # temporarily mark them as healthy if we receive a keepalive message
    if state.id in ["59d70fc1-e6a2-4c0e-9f4d-c5ced151cec1", "dcfba45f-d503-4fef-bb11-9221b9efa70a"] do
      Health.put_event(
        state.replication_slot,
        %Event{slug: :replication_heartbeat_received, status: :success}
      )
    end

    messages =
      if reply == 1 and not is_nil(state.last_commit_lsn) do
        # With our current LSN increment strategy, we'll always replay the last record on boot. It seems
        # safe to increment the last_commit_lsn by 1 (Commit also contains the next LSN)
        lsn = safe_ack_lsn(state)
        Logger.info("Acking LSN #{lsn} (current server LSN: #{wal_end})")

        Replication.put_last_processed_commit_tuple!(state.id, {lsn, 0})
        ack_message(lsn)
      else
        []
      end

    {:noreply, messages, state}
  end

  def handle_data(data, %State{} = state) do
    Logger.error("Unknown data: #{inspect(data)}")
    {:noreply, state}
  end

  @impl ReplicationConnection
  def handle_call({:update_message_handler_ctx, ctx}, from, state) do
    state = %{state | message_handler_ctx: ctx}
    # Need to manually send reply
    GenServer.reply(from, :ok)
    {:noreply, state}
  end

  @impl ReplicationConnection
  def handle_call({:monitor_message_store, consumer_id}, from, state) do
    if Map.has_key?(state.message_store_refs, consumer_id) do
      GenServer.reply(from, :ok)
      {:noreply, state}
    else
      pid = GenServer.whereis(SlotMessageStore.via_tuple(consumer_id))
      ref = Process.monitor(pid)
      :ok = SlotMessageStore.set_monitor_ref(pid, ref)
      Logger.info("Monitoring message store for consumer #{consumer_id}")
      GenServer.reply(from, :ok)
      {:noreply, %{state | message_store_refs: Map.put(state.message_store_refs, consumer_id, ref)}}
    end
  end

  @impl ReplicationConnection
  def handle_call({:demonitor_message_store, consumer_id}, from, state) do
    case state.message_store_refs[consumer_id] do
      nil ->
        Logger.warning("No monitor found for consumer #{consumer_id}")
        GenServer.reply(from, :ok)
        {:noreply, state}

      ref ->
        res = Process.demonitor(ref)
        Logger.info("Demonitored message store for consumer #{consumer_id}: (res=#{inspect(res)})")
        GenServer.reply(from, :ok)
        {:noreply, %{state | message_store_refs: Map.delete(state.message_store_refs, consumer_id)}}
    end
  end

  @impl ReplicationConnection
  def handle_info({:DOWN, ref, :process, _pid, reason}, %State{} = state) do
    if env() == :test and reason == :shutdown do
      {:noreply, state}
    else
      {consumer_id, ^ref} = Enum.find(state.message_store_refs, fn {_, r} -> r == ref end)

      Logger.error(
        "[SlotProcessor] SlotMessageStore died. Shutting down.",
        consumer_id: consumer_id,
        reason: reason
      )

      raise "SlotMessageStore died (consumer_id=#{consumer_id}, reason=#{inspect(reason)})"
    end
  end

  @impl ReplicationConnection
  def handle_info(:emit_heartbeat, %State{} = state) do
    schedule_heartbeat(state)
    conn = get_cached_conn(state)

    case Postgres.query(conn, "SELECT pg_logical_emit_message(true, 'sequin.heartbeat.0', '')") do
      {:ok, _res} ->
        :ok

      {:error, error} ->
        Logger.error("Error emitting heartbeat: #{inspect(error)}")
    end

    {:noreply, state}
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

    Logger.info("[SlotProcessor] Process metrics",
      memory_mb: Float.round(info[:memory] / 1_024 / 1_024, 2),
      message_queue_len: info[:message_queue_len]
    )

    schedule_process_logging()
    {:noreply, state}
  end

  defp schedule_process_logging do
    Process.send_after(self(), :process_logging, :timer.seconds(30))
  end

  defp schedule_heartbeat(%State{} = state) do
    Process.send_after(self(), :emit_heartbeat, state.heartbeat_interval)
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

  # Used in debugging, worth keeping around.
  # defp lsn_to_tuple(lsn) when is_integer(lsn) do
  #   {lsn >>> 32, lsn &&& 0xFFFFFFFF}
  # end

  defp skip_message?(%struct{} = msg, state) when struct in [Insert, Update, Delete] do
    {_columns, schema, _table} = Map.get(state.schemas, msg.relation_id)
    schema in [@config_schema, @stream_schema]
  end

  defp skip_message?(_, _state), do: false

  @spec process_message(map(), State.t()) :: State.t()
  defp process_message(%Relation{id: id, columns: columns, namespace: schema, name: table}, %State{} = state) do
    conn = get_cached_conn(state)

    # Query to get primary keys
    # We can't trust the `flags` when replica identity is set to full - all columns are indicated
    # as primary keys.
    pk_query = """
    select a.attname
    from pg_index i
    join pg_attribute a on a.attrelid = i.indrelid and a.attnum = any(i.indkey)
    where i.indrelid = $1
    and i.indisprimary;
    """

    {:ok, %{rows: pk_rows}} = Postgres.query(conn, pk_query, [id])
    primary_keys = List.flatten(pk_rows)

    # Query to get attnums
    # The Relation message does not include the attnums, so we need to query the pg_attribute
    # table to get them.
    attnum_query = """
    select attname, attnum
    from pg_attribute
      where attrelid = $1
    and attnum > 0
    and not attisdropped;
    """

    {:ok, %{rows: attnum_rows}} = Postgres.query(conn, attnum_query, [id])
    attnum_map = Map.new(attnum_rows, fn [name, num] -> {name, num} end)

    # Enrich columns with primary key information and attnums
    enriched_columns =
      Enum.map(columns, fn %{name: name} = column ->
        %{column | pk?: name in primary_keys, attnum: Map.get(attnum_map, name)}
      end)

    updated_schemas = Map.put(state.schemas, id, {enriched_columns, schema, table})
    %{state | schemas: updated_schemas}
  end

  defp process_message(
         %Begin{commit_timestamp: ts, final_lsn: lsn, xid: xid},
         %State{accumulated_messages: {0, []}, last_commit_lsn: last_commit_lsn} = state
       ) do
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

    %{state | current_commit_ts: ts, current_commit_idx: 0, current_xaction_lsn: begin_lsn, current_xid: xid}
  end

  # Ensure we do not have an out-of-order bug by asserting equality
  defp process_message(
         %Commit{lsn: lsn, commit_timestamp: ts},
         %State{current_xaction_lsn: current_lsn, current_commit_ts: ts, id: id} = state
       ) do
    lsn = Postgres.lsn_to_int(lsn)

    unless current_lsn == lsn do
      raise "Unexpectedly received a commit LSN that does not match current LSN (#{current_lsn} != #{lsn})"
    end

    :ets.insert(ets_table(), {{id, :last_committed_at}, ts})

    %{
      state
      | last_commit_lsn: lsn,
        current_xaction_lsn: nil,
        current_xid: nil,
        current_commit_ts: nil,
        current_commit_idx: 0,
        dirty: false
    }
  end

  defp process_message(%Message{} = msg, state) do
    TracerServer.message_replicated(state.postgres_database, msg)

    Health.put_event(
      state.replication_slot,
      %Event{slug: :replication_message_processed, status: :success}
    )

    maybe_put_message(state, msg)
  end

  # Ignore type messages, we receive them before type columns:
  # %Sequin.Extensions.PostgresAdapter.Decoder.Messages.Type{id: 551312, namespace: "public", name: "citext"}
  # Custom enum:
  # %Sequin.Extensions.PostgresAdapter.Decoder.Messages.Type{id: 3577319, namespace: "public", name: "character_status"}
  defp process_message(%Decoder.Messages.Type{}, state) do
    state
  end

  defp process_message(%LogicalMessage{prefix: "sequin.heartbeat.0"}, state) do
    Logger.info("[SlotProcessor] Heartbeat received")
    Health.put_event(state.replication_slot, %Event{slug: :replication_heartbeat_received, status: :success})

    if state.test_pid do
      # TODO: Decouple
      send(state.test_pid, {__MODULE__, :heartbeat_received})
    end

    state
  end

  defp process_message(%LogicalMessage{prefix: "sequin." <> _} = msg, state) do
    message_handler_ctx =
      state.message_handler_module.handle_logical_message(state.message_handler_ctx, state.current_xaction_lsn, msg)

    %{state | message_handler_ctx: message_handler_ctx}
  end

  # Ignore other logical messages
  defp process_message(%LogicalMessage{}, state) do
    state
  end

  defp process_message(msg, state) do
    Logger.error("Unknown message: #{inspect(msg)}")
    state
  end

  @spec maybe_cast_message(decoded_message :: map(), State.t()) :: Message.t() | map()
  defp maybe_cast_message(%Insert{} = msg, state) do
    {columns, schema, table} = Map.get(state.schemas, msg.relation_id)
    ids = data_tuple_to_ids(columns, msg.tuple_data)

    %Message{
      action: :insert,
      commit_lsn: state.current_xaction_lsn,
      commit_idx: state.current_commit_idx,
      commit_timestamp: state.current_commit_ts,
      errors: nil,
      ids: ids,
      table_schema: schema,
      table_name: table,
      table_oid: msg.relation_id,
      fields: data_tuple_to_fields(ids, columns, msg.tuple_data),
      trace_id: UUID.uuid4()
    }
  end

  defp maybe_cast_message(%Update{} = msg, state) do
    {columns, schema, table} = Map.get(state.schemas, msg.relation_id)
    ids = data_tuple_to_ids(columns, msg.tuple_data)

    old_fields =
      if msg.old_tuple_data do
        data_tuple_to_fields(ids, columns, msg.old_tuple_data)
      end

    %Message{
      action: :update,
      commit_lsn: state.current_xaction_lsn,
      commit_idx: state.current_commit_idx,
      commit_timestamp: state.current_commit_ts,
      errors: nil,
      ids: ids,
      table_schema: schema,
      table_name: table,
      table_oid: msg.relation_id,
      old_fields: old_fields,
      fields: data_tuple_to_fields(ids, columns, msg.tuple_data),
      trace_id: UUID.uuid4()
    }
  end

  defp maybe_cast_message(%Delete{} = msg, state) do
    {columns, schema, table} = Map.get(state.schemas, msg.relation_id)

    prev_tuple_data =
      if msg.old_tuple_data do
        msg.old_tuple_data
      else
        msg.changed_key_tuple_data
      end

    ids = data_tuple_to_ids(columns, prev_tuple_data)

    %Message{
      action: :delete,
      commit_lsn: state.current_xaction_lsn,
      commit_idx: state.current_commit_idx,
      commit_timestamp: state.current_commit_ts,
      errors: nil,
      ids: ids,
      table_schema: schema,
      table_name: table,
      table_oid: msg.relation_id,
      old_fields: data_tuple_to_fields(ids, columns, prev_tuple_data),
      trace_id: UUID.uuid4()
    }
  end

  defp maybe_cast_message(msg, _state), do: msg

  defp flush_messages(%State{accumulated_messages: {_, []}} = state), do: state

  defp flush_messages(%State{} = state) do
    {_, messages} = state.accumulated_messages
    messages = Enum.reverse(messages)

    next_commit_tuple = messages |> Enum.map(&{&1.commit_lsn, &1.commit_idx}) |> Enum.max()
    # Flush accumulated messages
    {:ok, _count, message_handler_ctx} =
      state.message_handler_module.handle_messages(state.message_handler_ctx, messages)

    if state.test_pid do
      send(state.test_pid, {__MODULE__, :flush_messages})
    end

    %{
      state
      | accumulated_messages: {0, []},
        last_processed_commit_tuple: next_commit_tuple,
        message_handler_ctx: message_handler_ctx
    }
  end

  defp maybe_put_message(%State{} = state, %Message{} = message) do
    if {message.commit_lsn, message.commit_idx} > state.last_processed_commit_tuple do
      {acc_size, acc_messages} = state.accumulated_messages
      new_size = acc_size + :erlang.external_size(message)

      %{
        state
        | accumulated_messages: {new_size, [message | acc_messages]},
          current_commit_idx: state.current_commit_idx + 1
      }
    else
      %{state | current_commit_idx: state.current_commit_idx + 1}
    end
  end

  defp safe_ack_lsn(%State{last_commit_lsn: nil}), do: raise("Unsafe to call safe_ack_lsn when last_commit_lsn is nil")

  defp safe_ack_lsn(%State{} = state) do
    case verify_monitor_refs(state) do
      :ok ->
        state.message_store_refs
        |> Enum.map(fn {consumer_id, ref} ->
          SlotMessageStore.min_unflushed_commit_lsn(consumer_id, ref)
        end)
        |> Enum.filter(& &1)
        |> case do
          [] -> state.last_commit_lsn + 1
          lsns -> Enum.min(lsns)
        end

      {:error, error} ->
        raise error
    end
  end

  defp verify_monitor_refs(%State{} = state) do
    sink_consumer_ids =
      state.replication_slot
      |> Sequin.Repo.preload(:sink_consumers, force: true)
      |> Map.fetch!(:sink_consumers)
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
      case cast_value(type, value) do
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

  defp cast_value("bool", "t"), do: {:ok, true}
  defp cast_value("bool", "f"), do: {:ok, false}

  defp cast_value("_" <> _type, "{}"), do: {:ok, []}

  defp cast_value("_" <> type, array_string) when is_binary(array_string) do
    array_string
    |> String.trim("{")
    |> String.trim("}")
    |> parse_pg_array()
    |> Enum.reduce_while({:ok, []}, fn value, {:ok, acc} ->
      case cast_value(type, value) do
        {:ok, casted_value} -> {:cont, {:ok, [casted_value | acc]}}
        error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, array} -> {:ok, Enum.reverse(array)}
      error -> error
    end
  end

  defp cast_value(type, value) when type in ["json", "jsonb"] and is_binary(value) do
    case Jason.decode(value) do
      {:ok, json} ->
        {:ok, json}

      {:error, %Jason.DecodeError{} = error} ->
        Logger.error("Failed to decode JSON value: #{inspect(error, limit: 10_000)}")

        wrapped_error =
          Error.service(
            service: :postgres_replication_slot,
            message: "Failed to decode JSON value: #{inspect(error)}",
            details: %{error: Exception.message(error)},
            code: :invalid_json
          )

        {:error, wrapped_error}
    end
  end

  defp cast_value(type, value) do
    case Ecto.Type.cast(string_to_ecto_type(type), value) do
      {:ok, casted_value} -> {:ok, casted_value}
      # Fallback to original value if casting fails
      :error -> {:ok, value}
    end
  end

  defp parse_pg_array(str) do
    str
    # split the string on commas, but only when they're not inside quotes.
    |> String.split(~r/,(?=(?:[^"]*"[^"]*")*[^"]*$)/)
    |> Enum.map(&String.trim/1)
    |> Enum.map(&unescape_string/1)
  end

  # When an array element contains a comma and is coming from Postgres, it will be wrapped in double quotes:
  # "\"royal,interest\""
  # We want to remove the double quotes so that we get:
  # "royal,interest"
  defp unescape_string(<<"\"", rest::binary>>) do
    rest
    |> String.slice(0..-2//1)
    |> String.replace("\\\"", "\"")
  end

  defp unescape_string(str), do: str

  @postgres_to_ecto_type_mapping %{
    # Numeric Types
    "int2" => :integer,
    "int4" => :integer,
    "int8" => :integer,
    "float4" => :float,
    "float8" => :float,
    "numeric" => :decimal,
    "money" => :decimal,
    # Character Types
    "char" => :string,
    "varchar" => :string,
    "text" => :string,
    # Binary Data Types
    "bytea" => :binary,
    # Date/Time Types
    "timestamp" => :naive_datetime,
    "timestamptz" => :utc_datetime,
    "date" => :date,
    "time" => :time,
    "timetz" => :time,
    # Ecto doesn't have a direct interval type
    "interval" => :map,
    # Boolean Type
    "bool" => :boolean,
    # Geometric Types
    "point" => {:array, :float},
    "line" => :string,
    "lseg" => :string,
    "box" => :string,
    "path" => :string,
    "polygon" => :string,
    "circle" => :string,
    # Network Address Types
    "inet" => :string,
    "cidr" => :string,
    "macaddr" => :string,
    # Bit String Types
    "bit" => :string,
    "bit_varying" => :string,
    # Text Search Types
    "tsvector" => :string,
    "tsquery" => :string,
    # UUID Type
    "uuid" => Ecto.UUID,
    # XML Type
    "xml" => :string,
    # JSON Types
    "json" => :map,
    "jsonb" => :map,
    # Arrays
    "_text" => {:array, :string},
    # Composite Types
    "composite" => :map,
    # Range Types
    "range" => {:array, :any},
    # Domain Types
    "domain" => :any,
    # Object Identifier Types
    "oid" => :integer,
    # pg_lsn Type
    "pg_lsn" => :string,
    # Pseudotypes
    "any" => :any
  }

  defp string_to_ecto_type(type) do
    Map.get(@postgres_to_ecto_type_mapping, type, :string)
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
    socket_options = state.connection[:socket_options] || []
    # Create a PostgresDatabase struct so we can use ConnectionCache. Doesn't matter
    # that the ID is for the replication_slot. Using ConnectionCache ensures we
    # do not create a bunch of connections to the database, regardless of the lifecycle
    # of this GenServer
    postgres_database = %PostgresDatabase{
      id: state.id,
      database: state.connection[:database],
      hostname: state.connection[:hostname],
      port: state.connection[:port],
      username: state.connection[:username],
      password: state.connection[:password],
      ssl: state.connection[:ssl] || false,
      ipv6: :inet6 in socket_options
    }

    {:ok, conn} = ConnectionCache.connection(postgres_database)
    conn
  end

  defp env do
    Application.get_env(:sequin, :env)
  end
end

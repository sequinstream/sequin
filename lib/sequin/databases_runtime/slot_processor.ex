defmodule Sequin.DatabasesRuntime.SlotProcessor do
  @moduledoc """
  Subscribes to the Postgres replication slot, decodes write ahead log binary messages
  and publishes them to a stream.

  Borrowed heavily from https://github.com/supabase/realtime/blob/main/lib/extensions/postgres_cdc_stream/replication.ex
  """

  # See below, where we set restart: :temporary
  use Postgrex.ReplicationConnection

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
  alias Sequin.DatabasesRuntime.SlotProcessor.Message
  alias Sequin.Error
  alias Sequin.Health2
  alias Sequin.Health2.Event
  alias Sequin.Postgres
  alias Sequin.Tracer.Server, as: TracerServer
  alias Sequin.Workers.CreateReplicationSlotWorker

  require Logger

  # 200MB
  @max_accumulated_bytes 200 * 1024 * 1024

  def ets_table, do: __MODULE__

  defmodule State do
    @moduledoc false
    use TypedStruct

    alias Sequin.Replication.PostgresReplicationSlot

    typedstruct do
      field :current_commit_ts, nil | integer()
      field :current_commit_idx, nil | integer()
      field :current_xaction_lsn, nil | integer()
      field :current_xid, nil | integer()
      field :message_handler_ctx, any()
      field :message_handler_module, atom()
      field :id, String.t()
      field :last_committed_lsn, integer()
      field :last_processed_seq, integer()
      field :publication, String.t()
      field :slot_name, String.t()
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
      last_committed_lsn: nil,
      heartbeat_interval: Keyword.get(opts, :heartbeat_interval, :timer.minutes(1))
    }

    Postgrex.ReplicationConnection.start_link(SlotProcessor, init, rep_conn_opts)
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

  def update_message_handler_ctx(id, ctx) do
    GenServer.call(via_tuple(id), {:update_message_handler_ctx, ctx})
  catch
    :exit, _e ->
      {:error, :not_running}
  end

  def via_tuple(id) do
    {:via, :syn, {:replication, {__MODULE__, id}}}
  end

  @impl Postgrex.ReplicationConnection
  def init(%State{} = state) do
    Logger.metadata(account_id: state.postgres_database.account_id, replication_id: state.id)
    Logger.info("[SlotProcessor] Initialized with opts: #{inspect(state.connection, pretty: true)}")

    if state.test_pid do
      Mox.allow(Sequin.DatabasesRuntime.MessageHandlerMock, state.test_pid, self())
      Mox.allow(Sequin.TestSupport.DateTimeMock, state.test_pid, self())
      Sandbox.allow(Sequin.Repo, state.test_pid, self())
    end

    Process.send_after(self(), :emit_heartbeat, 0)

    {:ok, %{state | step: :disconnected}}
  end

  @impl Postgrex.ReplicationConnection
  def handle_connect(%State{connect_attempts: attempts} = state) when attempts >= 5 do
    Logger.error("[SlotProcessor] Failed to connect to replication slot after 5 attempts")

    conn = get_cached_conn(state)

    error_msg =
      case Postgres.replication_slot_status(conn, state.slot_name) do
        {:ok, :available} ->
          "Failed to connect to replication slot after 5 attempts"

        {:ok, :busy} ->
          "Replication slot '#{state.slot_name}' is currently in use by another connection"

        {:ok, :not_found} ->
          maybe_recreate_slot(state)
          "Replication slot '#{state.slot_name}' does not exist"

        {:error, error} ->
          Exception.message(error)
      end

    Health2.put_event(
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

    {:ok, last_processed_seq} = Sequin.Replication.last_processed_seq(state.id)

    query =
      if state.id in ["59d70fc1-e6a2-4c0e-9f4d-c5ced151cec1", "dcfba45f-d503-4fef-bb11-9221b9efa70a"] do
        "START_REPLICATION SLOT #{state.slot_name} LOGICAL 0/0 (proto_version '1', publication_names '#{state.publication}')"
      else
        "START_REPLICATION SLOT #{state.slot_name} LOGICAL 0/0 (proto_version '1', publication_names '#{state.publication}', messages 'true')"
      end

    {:stream, query, [],
     %{state | step: :streaming, connect_attempts: state.connect_attempts + 1, last_processed_seq: last_processed_seq}}
  end

  @impl Postgrex.ReplicationConnection
  def handle_result(result, state) do
    Logger.warning("Unknown result: #{inspect(result)}")
    {:noreply, state}
  end

  @spec stop(pid) :: :ok
  def stop(pid), do: GenServer.stop(pid)

  @impl Postgrex.ReplicationConnection
  def handle_data(<<?w, _header::192, msg::binary>>, %State{} = state) do
    # TODO: Move to better spot after we vendor ReplicationConnection
    Health2.put_event(
      state.replication_slot,
      %Event{slug: :replication_connected, status: :success}
    )

    msg = Decoder.decode_message(msg)
    next_state = process_message(msg, state)
    {acc_size, _acc_messages} = next_state.accumulated_messages

    unless match?(%LogicalMessage{prefix: "sequin.heartbeat.0"}, msg) do
      # A replication message is *their* message(s), not our message.
      Health2.put_event(
        state.replication_slot,
        %Event{slug: :replication_message_processed, status: :success}
      )
    end

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

      error = Error.service(service: :replication, message: Exception.message(e))

      Health2.put_event(
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
      Health2.put_event(
        state.replication_slot,
        %Event{slug: :replication_heartbeat_received, status: :success}
      )
    end

    messages =
      if reply == 1 and not is_nil(state.last_committed_lsn) do
        # With our current LSN increment strategy, we'll always replay the last record on boot. It seems
        # safe to increment the last_committed_lsn by 1 (Commit also contains the next LSN)
        lsn = state.last_committed_lsn + 1
        Logger.info("Acking LSN #{lsn} (current server LSN: #{wal_end})")
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

  @impl Postgrex.ReplicationConnection
  def handle_call({:update_message_handler_ctx, ctx}, from, state) do
    state = %{state | message_handler_ctx: ctx}
    # Need to manually send reply
    GenServer.reply(from, :ok)
    {:noreply, state}
  end

  @impl Postgrex.ReplicationConnection
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
         %State{accumulated_messages: {0, []}, last_committed_lsn: last_committed_lsn} = state
       ) do
    begin_lsn = Postgres.lsn_to_int(lsn)

    state =
      if not is_nil(last_committed_lsn) and begin_lsn < last_committed_lsn do
        Logger.error(
          "Received a Begin message with an LSN that is less than the last committed LSN (#{begin_lsn} < #{last_committed_lsn})"
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
      | last_committed_lsn: lsn,
        current_xaction_lsn: nil,
        current_xid: nil,
        current_commit_ts: nil,
        current_commit_idx: 0,
        dirty: false
    }
  end

  defp process_message(%Insert{} = msg, state) do
    {columns, schema, table} = Map.get(state.schemas, msg.relation_id)

    record = %Message{
      action: :insert,
      commit_lsn: state.current_xaction_lsn,
      commit_timestamp: state.current_commit_ts,
      commit_idx: state.current_commit_idx,
      seq: state.current_xaction_lsn + state.current_commit_idx,
      errors: nil,
      ids: data_tuple_to_ids(columns, msg.tuple_data),
      table_schema: schema,
      table_name: table,
      table_oid: msg.relation_id,
      fields: data_tuple_to_fields(columns, msg.tuple_data),
      trace_id: UUID.uuid4()
    }

    TracerServer.message_replicated(state.postgres_database, record)

    put_message(state, record)
  end

  defp process_message(%Update{} = msg, %State{} = state) do
    {columns, schema, table} = Map.get(state.schemas, msg.relation_id)

    old_fields =
      if msg.old_tuple_data do
        data_tuple_to_fields(columns, msg.old_tuple_data)
      end

    record = %Message{
      action: :update,
      commit_lsn: state.current_xaction_lsn,
      commit_timestamp: state.current_commit_ts,
      commit_idx: state.current_commit_idx,
      seq: state.current_xaction_lsn + state.current_commit_idx,
      errors: nil,
      ids: data_tuple_to_ids(columns, msg.tuple_data),
      table_schema: schema,
      table_name: table,
      table_oid: msg.relation_id,
      old_fields: old_fields,
      fields: data_tuple_to_fields(columns, msg.tuple_data),
      trace_id: UUID.uuid4()
    }

    TracerServer.message_replicated(state.postgres_database, record)

    put_message(state, record)
  end

  defp process_message(%Delete{} = msg, %State{} = state) do
    {columns, schema, table} = Map.get(state.schemas, msg.relation_id)

    prev_tuple_data =
      if msg.old_tuple_data do
        msg.old_tuple_data
      else
        msg.changed_key_tuple_data
      end

    record = %Message{
      action: :delete,
      commit_lsn: state.current_xaction_lsn,
      commit_timestamp: state.current_commit_ts,
      commit_idx: state.current_commit_idx,
      seq: state.current_xaction_lsn + state.current_commit_idx,
      errors: nil,
      ids: data_tuple_to_ids(columns, prev_tuple_data),
      table_schema: schema,
      table_name: table,
      table_oid: msg.relation_id,
      old_fields: data_tuple_to_fields(columns, prev_tuple_data),
      trace_id: UUID.uuid4()
    }

    TracerServer.message_replicated(state.postgres_database, record)

    put_message(state, record)
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
    Health2.put_event(state.replication_slot, %Event{slug: :replication_heartbeat_received, status: :success})

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

  defp flush_messages(%State{} = state) do
    {_, messages} = state.accumulated_messages

    # Reverse the messages because we accumulate them with list prepending
    {rejected_messages, messages} =
      messages
      |> Enum.reverse()
      |> Enum.split_with(fn message -> message.seq <= state.last_processed_seq end)

    unless rejected_messages == [] do
      max_rejected_seq = rejected_messages |> Enum.map(& &1.seq) |> Enum.max()

      Logger.info(
        "Skipping #{length(rejected_messages)} already-processed messages (last_processed_seq=#{state.last_processed_seq}, max_rejected_seq=#{max_rejected_seq})"
      )
    end

    if Enum.any?(messages) do
      next_seq = messages |> Enum.map(& &1.seq) |> Enum.max()
      # Flush accumulated messages
      {:ok, _count, message_handler_ctx} =
        state.message_handler_module.handle_messages(state.message_handler_ctx, messages)

      if state.test_pid do
        send(state.test_pid, {__MODULE__, :flush_messages})
      end

      %{state | accumulated_messages: {0, []}, last_processed_seq: next_seq, message_handler_ctx: message_handler_ctx}
    else
      %{state | accumulated_messages: {0, []}}
    end
  end

  defp put_message(%State{} = state, message) do
    {acc_size, acc_messages} = state.accumulated_messages
    new_size = acc_size + :erlang.external_size(message)

    %{
      state
      | accumulated_messages: {new_size, [message | acc_messages]},
        current_commit_idx: state.current_commit_idx + 1
    }
  end

  def data_tuple_to_ids(columns, tuple_data) do
    columns
    |> Enum.zip(Tuple.to_list(tuple_data))
    |> Enum.filter(fn {col, _} -> col.pk? end)
    # Very important - the system expects the PKs to be sorted by attnum
    |> Enum.sort_by(fn {col, _} -> col.attnum end)
    |> Enum.map(fn {_, value} -> value end)
  end

  def data_tuple_to_fields(columns, tuple_data) do
    columns
    |> Enum.zip(Tuple.to_list(tuple_data))
    |> Enum.map(fn {%{name: name, attnum: attnum, type: type}, value} ->
      %Message.Field{
        column_name: name,
        column_attnum: attnum,
        value: cast_value(type, value)
      }
    end)
  end

  defp cast_value("bool", "t"), do: true
  defp cast_value("bool", "f"), do: false

  defp cast_value("_" <> _type, "{}") do
    []
  end

  defp cast_value("_" <> type, array_string) when is_binary(array_string) do
    array_string
    |> String.trim("{")
    |> String.trim("}")
    |> parse_pg_array()
    |> Enum.map(&cast_value(type, &1))
  end

  defp cast_value(type, value) when type in ["json", "jsonb"] and is_binary(value) do
    Jason.decode!(value)
  end

  defp cast_value(type, value) do
    case Ecto.Type.cast(string_to_ecto_type(type), value) do
      {:ok, casted_value} -> casted_value
      # Fallback to original value if casting fails
      :error -> value
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

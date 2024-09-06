defmodule Sequin.Extensions.Replication do
  @moduledoc """
  Subscribes to the Postgres replication slot, decodes write ahead log binary messages
  and publishes them to a stream.

  Borrowed heavily from https://github.com/supabase/realtime/blob/main/lib/extensions/postgres_cdc_stream/replication.ex
  """

  # See below, where we set restart: :temporary
  use Postgrex.ReplicationConnection

  import Bitwise

  alias __MODULE__
  alias Ecto.Adapters.SQL.Sandbox
  alias Sequin.Databases.ConnectionCache
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Error
  alias Sequin.Extensions.PostgresAdapter.Decoder
  alias Sequin.Extensions.PostgresAdapter.Decoder.Messages.Begin
  alias Sequin.Extensions.PostgresAdapter.Decoder.Messages.Commit
  alias Sequin.Extensions.PostgresAdapter.Decoder.Messages.Delete
  alias Sequin.Extensions.PostgresAdapter.Decoder.Messages.Insert
  alias Sequin.Extensions.PostgresAdapter.Decoder.Messages.Relation
  alias Sequin.Extensions.PostgresAdapter.Decoder.Messages.Update
  alias Sequin.Health
  alias Sequin.Replication.Message
  alias Sequin.Tracer.Server, as: TracerServer

  require Logger

  def ets_table, do: __MODULE__

  defmodule State do
    @moduledoc false
    use TypedStruct

    typedstruct do
      field :current_commit_ts, nil | integer()
      field :current_xaction_lsn, nil | integer()
      field :current_xid, nil | integer()
      field :message_handler_ctx, any()
      field :message_handler_module, atom()
      field :id, String.t()
      field :last_committed_lsn, integer(), default: 0
      field :publication, String.t()
      field :slot_name, String.t()
      field :postgres_database, PostgresDatabase.t()
      field :step, :disconnected | :streaming
      field :test_pid, pid()
      field :connection, map()
      field :schemas, %{}, default: %{}
      field :accumulated_messages, [Message.t()], default: []
    end
  end

  def start_link(opts) do
    id = Keyword.fetch!(opts, :id)
    connection = Keyword.fetch!(opts, :connection)
    publication = Keyword.fetch!(opts, :publication)
    slot_name = Keyword.fetch!(opts, :slot_name)
    postgres_database = Keyword.fetch!(opts, :postgres_database)
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
      test_pid: test_pid,
      message_handler_ctx: message_handler_ctx,
      message_handler_module: message_handler_module,
      connection: connection
    }

    Logger.debug("[Replication] Starting link with opts: #{inspect(rep_conn_opts, pretty: true)}")

    Postgrex.ReplicationConnection.start_link(Replication, init, rep_conn_opts)
  end

  def child_spec(opts) do
    # Not used by DynamicSupervisor, but used by Supervisor in test
    id = Keyword.fetch!(opts, :id)

    spec = %{
      id: via_tuple(id),
      start: {__MODULE__, :start_link, [opts]}
    }

    # Eventually, we can wrap this in a Supervisor, to get faster retries on restarts before "giving up"
    # The Starter will eventually restart this Replication GenServer.
    Supervisor.child_spec(spec, restart: :temporary)
  end

  def update_message_handler_ctx(id, ctx) do
    GenServer.call(via_tuple(id), {:update_message_handler_ctx, ctx})
  catch
    :exit, _e ->
      {:error, :not_running}
  end

  def via_tuple(id) do
    {:via, Registry, {Sequin.Registry, {Replication, id}}}
  end

  @impl Postgrex.ReplicationConnection
  def init(%State{} = state) do
    Logger.metadata(account_id: state.postgres_database.account_id, replication_id: state.id)
    Logger.info("[Replication] Initialized")

    if state.test_pid do
      Mox.allow(Sequin.Mocks.Extensions.MessageHandlerMock, state.test_pid, self())
      Sandbox.allow(Sequin.Repo, state.test_pid, self())
    end

    {:ok, %{state | step: :disconnected}}
  end

  @impl Postgrex.ReplicationConnection
  def handle_connect(state) do
    Logger.debug("[Replication] Handling connect")
    Health.update(state.postgres_database, :replication_connected, :initializing)

    query =
      "START_REPLICATION SLOT #{state.slot_name} LOGICAL 0/0 (proto_version '1', publication_names '#{state.publication}')"

    {:stream, query, [], %{state | step: :streaming}}
  end

  @impl Postgrex.ReplicationConnection
  def handle_result(result, state) do
    Logger.debug("Unknown result: #{inspect(result)}")
    {:noreply, state}
  end

  @spec stop(pid) :: :ok
  def stop(pid), do: GenServer.stop(pid)

  @impl Postgrex.ReplicationConnection
  def handle_data(<<?w, _header::192, msg::binary>>, %State{} = state) do
    next_state =
      msg
      |> Decoder.decode_message()
      |> process_message(state)

    Health.update(state.postgres_database, :replication_messages, :healthy)

    if next_state.last_committed_lsn == state.last_committed_lsn do
      {:noreply, next_state}
    else
      {:noreply, ack_message(next_state.last_committed_lsn), next_state}
    end
  rescue
    e ->
      Logger.error("Error processing message: #{inspect(e)}")

      error = Error.service(service: :replication, message: Exception.message(e))
      Health.update(state.postgres_database, :replication_messages, :error, error)

      reraise e, __STACKTRACE__
  end

  # keepalive
  # With our current LSN increment strategy, we'll always replay the last record on boot. It seems
  # safe to increment the last_committed_lsn by 1 (Commit also contains the next LSN)
  def handle_data(<<?k, wal_end::64, _clock::64, reply>>, %State{} = state) do
    Health.update(state.postgres_database, :replication_connected, :healthy)

    messages =
      case reply do
        1 ->
          # wal_end is already an int
          last_lsn = state.last_committed_lsn || wal_end
          ack_message(last_lsn)

        0 ->
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

  defp ack_message(lsn) when is_integer(lsn) do
    lsn = lsn + 1
    [<<?r, lsn::64, lsn::64, lsn::64, current_time()::64, 0>>]
  end

  # In Postgres, an LSN is typically represented as a 64-bit integer, but it's sometimes split
  # into two 32-bit parts for easier reading or processing. We'll receive tuples like `{401, 1032909664}`
  # and we'll need to combine them to get the 64-bit LSN.
  defp lsn_to_int({high, low}) do
    high <<< 32 ||| low
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
    SELECT a.attname
    FROM pg_index i
    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
    WHERE i.indrelid = ($1::text || '.' || $2::text)::regclass
    AND i.indisprimary;
    """

    {:ok, %{rows: pk_rows}} = Postgrex.query(conn, pk_query, [schema, table])
    primary_keys = List.flatten(pk_rows)

    # Query to get attnums
    # The Relation message does not include the attnums, so we need to query the pg_attribute
    # table to get them.
    attnum_query = """
    SELECT attname, attnum
    FROM pg_attribute
    WHERE attrelid = ($1::text || '.' || $2::text)::regclass
    AND attnum > 0
    AND NOT attisdropped;
    """

    {:ok, %{rows: attnum_rows}} = Postgrex.query(conn, attnum_query, [schema, table])
    attnum_map = Map.new(attnum_rows, fn [name, num] -> {name, num} end)

    # Enrich columns with primary key information and attnums
    enriched_columns =
      Enum.map(columns, fn %{name: name} = column ->
        %{column | pk?: name in primary_keys, attnum: Map.get(attnum_map, name)}
      end)

    updated_schemas = Map.put(state.schemas, id, {enriched_columns, schema, table})
    %{state | schemas: updated_schemas}
  end

  defp process_message(%Begin{commit_timestamp: ts, final_lsn: lsn, xid: xid}, %State{accumulated_messages: []} = state) do
    %{state | current_commit_ts: ts, current_xaction_lsn: lsn_to_int(lsn), current_xid: xid}
  end

  # Ensure we do not have an out-of-order bug by asserting equality
  defp process_message(
         %Commit{lsn: lsn, commit_timestamp: ts},
         %State{current_xaction_lsn: current_lsn, current_commit_ts: ts, id: id} = state
       ) do
    lsn = lsn_to_int(lsn)

    unless current_lsn == lsn do
      raise "Unexpectedly received a commit LSN that does not match current LSN #{current_lsn} != #{lsn}"
    end

    :ets.insert(ets_table(), {{id, :last_committed_at}, ts})

    # Flush accumulated messages
    state.message_handler_module.handle_messages(
      state.message_handler_ctx,
      Enum.reverse(state.accumulated_messages)
    )

    if state.test_pid do
      send(state.test_pid, {__MODULE__, :flush_messages})
    end

    %{
      state
      | last_committed_lsn: lsn,
        current_xaction_lsn: nil,
        current_xid: nil,
        current_commit_ts: nil,
        accumulated_messages: []
    }
  end

  defp process_message(%Insert{} = msg, state) do
    {columns, schema, table} = Map.get(state.schemas, msg.relation_id)

    record = %Message{
      action: :insert,
      commit_timestamp: state.current_commit_ts,
      errors: nil,
      ids: data_tuple_to_ids(columns, msg.tuple_data),
      table_schema: schema,
      table_name: table,
      table_oid: msg.relation_id,
      fields: data_tuple_to_fields(columns, msg.tuple_data),
      trace_id: UUID.uuid4()
    }

    TracerServer.message_replicated(state.postgres_database, record)

    %{state | accumulated_messages: [record | state.accumulated_messages]}
  end

  defp process_message(%Update{} = msg, %State{} = state) do
    {columns, schema, table} = Map.get(state.schemas, msg.relation_id)

    old_fields =
      if msg.old_tuple_data do
        data_tuple_to_fields(columns, msg.old_tuple_data)
      end

    record = %Message{
      action: :update,
      commit_timestamp: state.current_commit_ts,
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

    %{state | accumulated_messages: [record | state.accumulated_messages]}
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
      commit_timestamp: state.current_commit_ts,
      errors: nil,
      ids: data_tuple_to_ids(columns, prev_tuple_data),
      table_schema: schema,
      table_name: table,
      table_oid: msg.relation_id,
      old_fields: data_tuple_to_fields(columns, prev_tuple_data),
      trace_id: UUID.uuid4()
    }

    TracerServer.message_replicated(state.postgres_database, record)

    %{state | accumulated_messages: [record | state.accumulated_messages]}
  end

  defp process_message(msg, state) do
    Logger.error("Unknown message: #{inspect(msg)}")
    state
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

  defp cast_value("_" <> type, array_string) when is_binary(array_string) do
    array_string
    |> String.trim("{")
    |> String.trim("}")
    |> parse_pg_array()
    |> Enum.map(&cast_value(type, &1))
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
end

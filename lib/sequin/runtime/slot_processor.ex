defmodule Sequin.Runtime.SlotProcessor do
  @moduledoc false
  alias Sequin.Databases.ConnectionCache
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Error
  alias Sequin.Postgres
  alias Sequin.Replication
  alias Sequin.Runtime.PostgresAdapter.Decoder
  alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.Relation

  require Logger

  @type reply :: String.t()

  defmodule State do
    @moduledoc false
    use TypedStruct

    alias Sequin.Consumers.SinkConsumer
    alias Sequin.Replication.PostgresReplicationSlot
    alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.LogicalMessage

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
      field :connect_attempts, non_neg_integer(), default: 0
      field :current_commit_idx, nil | integer()
      field :current_commit_ts, nil | integer()
      field :current_xaction_lsn, nil | integer()
      field :current_xid, nil | integer()
      field :last_commit_lsn, integer()
      field :step, :disconnected | :streaming

      # Wal cursors
      field :low_watermark_wal_cursor, Replication.wal_cursor()
      field :safe_wal_cursor_fn, (State.t() -> Replication.wal_cursor())

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
      field :heartbeat_timer, nil | reference()

      # Settings
      field :bytes_between_limit_checks, non_neg_integer()
      field :heartbeat_interval, non_neg_integer()
      field :max_memory_bytes, non_neg_integer()
    end
  end

  @eager_decode_msg_kinds [
    # Relation
    ?R
  ]

  @spec handle_data(binary() | Decoder.message(), State.t()) :: {:ok, [reply()], State.t()} | {:error, Error.t()}
  def handle_data(<<?w, _header::192, msg_kind::8, _::binary>> = bin, %State{} = state)
      when msg_kind in @eager_decode_msg_kinds do
    <<?w, _header::192, msg::binary>> = bin
    msg = Decoder.decode_message(msg)
    handle_data(msg, state)
  end

  def handle_data(<<?w, _header::192, msg::binary>>, %State{} = state) do
    raw_bytes_received = byte_size(msg)
    incr_counter(:raw_bytes_received, raw_bytes_received)
    incr_counter(:raw_bytes_received_since_last_log, raw_bytes_received)

    state =
      Map.update!(state, :accumulated_msg_binaries, fn acc ->
        %{acc | count: acc.count + 1, bytes: acc.bytes + raw_bytes_received, binaries: [msg | acc.binaries]}
      end)

    # Update bytes processed and check limits
    with {:ok, state} <- check_limit(state, raw_bytes_received) do
      {:ok, [], state}
    end
  end

  # Primary keepalive message from server:
  # https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-PRIMARY-KEEPALIVE-MESSAGE
  #
  # Byte1('k')      - Identifies message as a sender keepalive
  # Int64           - Current end of WAL on the server
  # Int64           - Server's system clock (microseconds since 2000-01-01 midnight)
  # Byte1           - 1 if reply requested immediately to avoid timeout, 0 otherwise
  def handle_data(<<?k, _wal_end::64, _clock::64, 0>>, %State{} = state) do
    {:ok, [], state}
  end

  def handle_data(<<?k, wal_end::64, _clock::64, 1>>, %State{last_commit_lsn: nil} = state) do
    # If we don't have a last_commit_lsn, we're still processing the first xaction
    # we received on boot. This can happen if we're processing a very large xaction.
    # It is therefore safe to send an ack with the last LSN we processed.
    {:ok, low_watermark} = Replication.low_watermark_wal_cursor(state.id)

    if low_watermark.commit_lsn > wal_end do
      Logger.warning("Server LSN #{wal_end} is behind our LSN #{low_watermark.commit_lsn}")
    end

    reply = [ack_message(low_watermark.commit_lsn)]
    {:ok, reply, state}
  end

  def handle_data(<<?k, wal_end::64, _clock::64, 1>>, %State{} = state) do
    wal_cursor = state.safe_wal_cursor_fn.(state)
    Logger.info("Acking LSN #{inspect(wal_cursor)} (current server LSN: #{wal_end})")

    Replication.put_low_watermark_wal_cursor!(state.id, wal_cursor)

    if wal_cursor.commit_lsn > wal_end do
      Logger.warning("Server LSN #{wal_end} is behind our LSN #{wal_cursor.commit_lsn}")
    end

    reply = [ack_message(wal_cursor.commit_lsn)]
    {:ok, reply, state}
  end

  def handle_data(data, %State{} = state) when is_binary(data) do
    Logger.error("Unknown data: #{inspect(data)}")
    {:ok, [], state}
  end

  def handle_data(%Relation{} = relation, %State{} = state) do
    state = put_relation_message(relation, state)
    {:ok, [], state}
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
    <<?r, lsn::64, lsn::64, lsn::64, current_time()::64, 0>>
  end

  @spec put_relation_message(Relation.t(), State.t()) :: State.t()
  defp put_relation_message(%Relation{id: id, columns: columns, namespace: schema, name: table}, %State{} = state) do
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

  defp check_limit(%State{} = state, raw_bytes_received) do
    # Check if it's been a while since we last checked the limit
    if state.bytes_received_since_last_limit_check + raw_bytes_received >= state.bytes_between_limit_checks do
      current_memory = state.check_memory_fn.()

      if current_memory >= state.max_memory_bytes do
        {:error, Error.invariant(message: "Memory limit exceeded", code: :over_system_memory_limit)}
      else
        {:ok, %{state | bytes_received_since_last_limit_check: 0}}
      end
    else
      new_bytes = state.bytes_received_since_last_limit_check + raw_bytes_received
      {:ok, %{state | bytes_received_since_last_limit_check: new_bytes}}
    end
  end

  defp get_cached_conn(%State{} = state) do
    {:ok, conn} = ConnectionCache.connection(state.postgres_database)
    conn
  end

  defp incr_counter(name, amount) do
    current = Process.get(name, 0)
    Process.put(name, current + amount)
  end

  @epoch DateTime.to_unix(~U[2000-01-01 00:00:00Z], :microsecond)
  defp current_time, do: System.os_time(:microsecond) - @epoch
end

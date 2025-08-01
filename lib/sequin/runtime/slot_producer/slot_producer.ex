defmodule Sequin.Runtime.SlotProducer do
  @moduledoc """
  A GenStage producer that streams PostgreSQL replication messages.

  This module uses `Postgrex.Protocol` to establish a replication
  connection and emits WAL messages downstream on demand.
  """

  use GenStage

  use Sequin.ProcessMetrics,
    metric_prefix: "sequin.slot_producer"

  use Sequin.GenerateBehaviour

  alias Postgrex.Protocol
  alias Sequin.Error
  alias Sequin.Error.NotFoundError
  alias Sequin.Health
  alias Sequin.Health.Event
  alias Sequin.Postgres
  alias Sequin.ProcessMetrics
  alias Sequin.Replication
  alias Sequin.Runtime.PostgresAdapter.Decoder
  alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.Begin
  alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.Commit
  alias Sequin.Runtime.SlotProducer.BatchMarker
  alias Sequin.Runtime.SlotProducer.PipelineDefaults
  alias Sequin.Runtime.SlotProducer.Relation

  require Logger

  defguardp is_socket_message(message)
            when elem(message, 0) in [:tcp, :tcp_closed, :tcp_error, :ssl, :ssl_closed, :ssl_error]

  defguardp below_restart_wal_cursor?(state)
            when {state.commit_lsn, state.next_commit_idx} <
                   {state.restart_wal_cursor.commit_lsn, state.restart_wal_cursor.commit_idx}

  @max_messages_per_protocol_read 500

  @type batch_flush_interval :: non_neg_integer()

  @type status :: :active | :buffering | :disconnected

  def batch_flush_interval do
    get_config!(:batch_flush_interval)
  end

  def merge_batch_flush_interval(partial) do
    config = batch_flush_interval()
    put_config(:batch_flush_interval, Keyword.merge(config, partial))
  end

  defp get_config!(key) do
    config = Application.fetch_env!(:sequin, __MODULE__)
    Keyword.fetch!(config, key)
  end

  defp put_config(key, value) do
    current_config = Application.get_env(:sequin, __MODULE__, [])
    updated_config = Keyword.put(current_config, key, value)
    Application.put_env(:sequin, __MODULE__, updated_config)
  end

  def status(id) do
    GenStage.call(via_tuple(id), :status)
  end

  defmodule Message do
    @moduledoc false
    use TypedStruct

    alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.LogicalMessage
    alias Sequin.Runtime.SlotProcessor.Message

    @type kind :: :insert | :update | :delete | :logical | :relation

    typedstruct enforce: true do
      field :byte_size, integer()
      field :commit_idx, integer()
      field :commit_lsn, integer()
      field :commit_ts, DateTime.t()
      field :kind, kind()
      field :payload, binary()
      # Temp: This wraps the current Message/LogicalMessage payload for compatibility
      field :message, Message.t() | LogicalMessage.t()
      field :transaction_annotations, String.t()
      field :batch_idx, non_neg_integer(), enforce: false
    end
  end

  defmodule State do
    @moduledoc false
    use TypedStruct

    alias Sequin.Postgres
    alias Sequin.Replication.PostgresReplicationSlot
    alias Sequin.Runtime.SlotProducer
    alias Sequin.Runtime.SlotProducer.BatchMarker
    alias Sequin.Runtime.SlotProducer.Relation

    typedstruct do
      field :id, String.t()
      field :database_id, String.t()
      field :slot_name, String.t()
      field :publication_name, String.t()
      field :pg_major_version, integer()
      field :conn, (-> Postgres.db_conn())
      # Postgres replication connection
      field :protocol, Postgrex.Protocol.state()
      field :connect_opts, keyword()
      field :on_connect_fail, (any() -> any())
      field :on_disconnect, (-> :ok)
      field :setting_reconnect_interval, non_neg_integer()
      field :status, SlotProducer.status()

      # Cursors & aliveness
      field :restart_wal_cursor, Replication.wal_cursor()
      field :last_sent_restart_wal_cursor, Replication.wal_cursor() | nil
      field :restart_wal_cursor_fn, (PostgresReplicationSlot.id(), Postgres.wal_cursor() -> Postgres.wal_cursor())
      field :setting_restart_wal_cursor_update_interval, non_neg_integer()
      field :restart_wal_cursor_update_timer, reference()
      field :setting_ack_interval, non_neg_integer()
      field :ack_timer, reference()
      field :last_dispatched_wal_cursor, Replication.wal_cursor()

      # Batches
      field :batch_idx, BatchMarker.idx(), default: 0

      field :batch_flush_timer, reference()
      field :setting_batch_flush_interval, SlotProducer.batch_flush_interval()

      # Current xaction state
      field :commit_ts, DateTime.t()
      field :commit_lsn, integer()
      field :next_commit_idx, integer()
      field :commit_xid, integer()
      field :transaction_annotations, nil | String.t()
      field :last_commit_lsn, nil | integer()
      field :last_commit_idx, nil | integer()

      field :demand, integer(), default: 0
      field :consumers, [pid()], default: []
      field :consumer_mod, module()

      # For consumers
      field :relations, %{required(table_oid :: String.t()) => Relation.t()}, default: %{}
      # Last batch marker for new subscribers
      field :last_batch_marker, BatchMarker.t() | nil

      # Buffers
      field :accumulated_messages, %{count: non_neg_integer(), bytes: non_neg_integer(), messages: [Message.t()]},
        default: %{count: 0, bytes: 0, messages: []}

      field :buffered_sock_msg, nil | String.t()
    end
  end

  @callback restart_wal_cursor(id :: PostgresReplicationSlot.id(), current_cursor :: Postgres.wal_cursor()) ::
              Postgres.wal_cursor()

  @callback on_connect_fail(state :: State.t(), error :: any()) :: :ok

  def start_link(opts) do
    id = Keyword.fetch!(opts, :id)
    GenStage.start_link(__MODULE__, opts, name: via_tuple(id))
  end

  def via_tuple(id) do
    {:via, :syn, {:replication, {__MODULE__, id}}}
  end

  @impl GenStage
  def init(opts) do
    id = Keyword.fetch!(opts, :id)
    database_id = Keyword.fetch!(opts, :database_id)
    account_id = Keyword.fetch!(opts, :account_id)

    Sequin.name_process({__MODULE__, id})

    Logger.metadata(
      account_id: account_id,
      replication_id: id,
      database_id: database_id
    )

    connect_opts = Keyword.fetch!(opts, :connect_opts)

    connect_opts = Keyword.put(connect_opts, :parameters, replication: "database")

    state = %State{
      id: Keyword.fetch!(opts, :id),
      database_id: Keyword.fetch!(opts, :database_id),
      slot_name: Keyword.fetch!(opts, :slot_name),
      publication_name: Keyword.fetch!(opts, :publication_name),
      connect_opts: connect_opts,
      pg_major_version: Keyword.fetch!(opts, :pg_major_version),
      status: :disconnected,
      on_connect_fail: Keyword.get(opts, :on_connect_fail_fn, &PipelineDefaults.on_connect_fail/2),
      # on_disconnect: Keyword.get(opts, :on_disconnect),
      restart_wal_cursor_fn: Keyword.get(opts, :restart_wal_cursor_fn, &PipelineDefaults.restart_wal_cursor/2),
      setting_reconnect_interval: Keyword.get(opts, :reconnect_interval, to_timeout(second: 10)),
      setting_ack_interval: Keyword.get(opts, :ack_interval, to_timeout(second: 5)),
      setting_restart_wal_cursor_update_interval:
        Keyword.get(opts, :restart_wal_cursor_update_interval, to_timeout(second: 10)),
      consumer_mod: Keyword.get_lazy(opts, :consumer_mod, fn -> PipelineDefaults.processor_mod() end),
      conn: Keyword.fetch!(opts, :conn),
      setting_batch_flush_interval: Keyword.get(opts, :batch_flush_interval)
    }

    if test_pid = opts[:test_pid] do
      Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
    end

    Process.send_after(self(), :connect, 0)
    {:producer, state, buffer_size: :infinity}
  end

  @impl GenStage
  def handle_demand(incoming, %{demand: demand} = state) do
    {messages, state} = maybe_produce_and_flush(%{state | demand: demand + incoming})
    state = maybe_toggle_buffering(state)
    {:noreply, messages, state}
  end

  @impl GenStage
  def handle_subscribe(:consumer, _opts, {pid, _ref}, state) do
    # Send all stored relations to the new consumer
    Enum.each(state.relations, fn {_table_oid, relation} ->
      state.consumer_mod.handle_relation(pid, relation)
    end)

    # Send last batch marker to the new consumer if we have one
    if state.last_batch_marker do
      state.consumer_mod.handle_batch_marker(pid, state.last_batch_marker)
    end

    {:automatic, %{state | consumers: [pid | state.consumers]}}
  end

  @impl GenStage
  def handle_info(:connect, %State{} = state) do
    with {:ok, protocol} <- Protocol.connect(state.connect_opts),
         Logger.info("[SlotProducer] Connected"),
         :ok <- put_connected_health(state.id),
         {:ok, %State{} = state, protocol} <- init_restart_wal_cursor(state, protocol),
         {:ok, protocol} <- Protocol.handle_streaming(start_replication_query(state), protocol),
         {:ok, protocol} <- Protocol.checkin(protocol) do
      state = %{state | protocol: protocol, status: :active}
      state = schedule_timers(state)
      {:noreply, [], state}
    else
      error ->
        reason =
          case error do
            {:error, msg} -> msg
            {:error, msg, %Protocol{}} -> msg
          end

        error_msg = if is_exception(reason), do: Exception.message(reason), else: inspect(reason)
        Logger.error("[SlotProducer] replication connect failed: #{error_msg}")
        if fun = state.on_connect_fail, do: fun.(state, reason)
        Process.send_after(self(), :connect, state.setting_reconnect_interval)
        {:noreply, [], state}
    end
  end

  def handle_info(msg, %State{status: :buffering, buffered_sock_msg: nil} = s) when is_socket_message(msg) do
    {:noreply, [], %{s | buffered_sock_msg: msg}}
  end

  def handle_info(msg, %State{status: :buffering}) when is_socket_message(msg) do
    raise "Unexpectedly received a second socket message while buffering sock messages"
  end

  def handle_info(msg, %State{protocol: protocol} = state) when is_socket_message(msg) do
    maybe_log_message(state)

    with {:ok, copies, protocol} <- Protocol.handle_copy_recv(msg, @max_messages_per_protocol_read, protocol),
         {:ok, state} <- handle_copies(copies, %{state | protocol: protocol}) do
      {messages, state} = maybe_produce_and_flush(state)
      state = maybe_toggle_buffering(state)
      {:noreply, messages, state}
    else
      {error, reason, protocol} ->
        handle_disconnect(error, reason, %{state | protocol: protocol})
    end
  end

  def handle_info(:send_ack, %State{status: :disconnected} = state) do
    state = schedule_ack(%{state | ack_timer: nil})
    {:noreply, [], state}
  end

  def handle_info(:send_ack, %State{} = state) do
    state = schedule_ack(%{state | ack_timer: nil})
    send_ack(state)
  end

  def handle_info(:update_restart_wal_cursor, %State{} = state) do
    state = update_restart_wal_cursor(state)

    state = schedule_update_cursor(%{state | restart_wal_cursor_update_timer: nil})
    {:noreply, [], state}
  end

  def handle_info(:flush_batch, %State{last_dispatched_wal_cursor: nil} = state) do
    Logger.info("[SlotProducer] Skipping flush_batch, no messages dispatched yet.")

    {:noreply, [], %{state | batch_flush_timer: nil}}
  end

  def handle_info(:flush_batch, %State{} = state) do
    batch_marker = %BatchMarker{
      high_watermark_wal_cursor: state.last_dispatched_wal_cursor,
      idx: state.batch_idx
    }

    Enum.each(state.consumers, fn consumer ->
      state.consumer_mod.handle_batch_marker(consumer, batch_marker)
    end)

    state = %{
      state
      | batch_idx: state.batch_idx + 1,
        last_batch_marker: batch_marker,
        batch_flush_timer: nil
    }

    {:noreply, [], state}
  end

  @impl GenStage
  def handle_call(:status, _from, %State{} = state) do
    {:reply, state.status, [], state}
  end

  defp maybe_log_message(%State{} = state) do
    if is_nil(state.last_commit_lsn) and state.next_commit_idx == 0 and state.accumulated_messages.count == 0 do
      Logger.info("Received first message from slot (`last_commit_lsn` was nil)")
    end
  end

  @advance_idx_types [?I, ?U, ?D, ?T, ?M]
  # Ignore ?T for processing, but still count it for idx advancement to future-proof
  @ignoreable_messages [?Y, ?T, ?O]
  defp handle_copies(copies, %State{} = state) do
    Enum.reduce_while(copies, {:ok, state}, fn copy, {:ok, state} ->
      {type, msg} = parse_copy(copy)

      case handle_data(type, msg, state) do
        {:ok, next_state} ->
          advance_next_commit_idx? = not is_nil(state.commit_lsn) and type in @advance_idx_types

          if advance_next_commit_idx? do
            {:cont, {:ok, %{next_state | next_commit_idx: state.next_commit_idx + 1}}}
          else
            {:cont, {:ok, next_state}}
          end

        error ->
          {:halt, error}
      end
    end)
  end

  defp parse_copy(<<?w, _header::192, msg::binary>>) do
    <<type, _::binary>> = msg
    {type, msg}
  end

  defp parse_copy(<<?k, _rest::binary>> = msg) do
    {?k, msg}
  end

  defp handle_data(type, _msg, %State{} = state) when type in @ignoreable_messages do
    {:ok, state}
  end

  defp handle_data(?B, msg, %State{} = state) do
    %State{last_commit_lsn: last_commit_lsn} = state
    %Begin{commit_timestamp: ts, final_lsn: lsn, xid: xid} = Decoder.decode_message(msg)
    begin_lsn = Postgres.lsn_to_int(lsn)

    if not is_nil(last_commit_lsn) and begin_lsn < last_commit_lsn do
      raise "Received a Begin message with an LSN that is less than the last commit LSN (#{begin_lsn} < #{last_commit_lsn})"
    end

    state = %{state | commit_ts: ts, next_commit_idx: 0, commit_lsn: begin_lsn, commit_xid: xid}
    {:ok, state}
  end

  defp handle_data(?C, msg, %State{} = state) do
    %State{commit_lsn: commit_lsn, commit_ts: commit_ts} = state
    %Commit{} = commit = Decoder.decode_message(msg)
    recv_lsn = Postgres.lsn_to_int(commit.lsn)

    if commit_lsn != recv_lsn or commit_ts != commit.commit_timestamp do
      error = """
      Unexpectedly received a commit that does not match current commit or ts
      lsn: #{commit_lsn} != #{recv_lsn}
      ts: #{inspect(commit_ts)} != #{inspect(commit.commit_timestamp)})
      """

      raise error
    end

    state = %{
      state
      | last_commit_lsn: commit_lsn,
        last_commit_idx: state.next_commit_idx - 1,
        commit_lsn: nil,
        commit_ts: nil,
        commit_xid: nil,
        next_commit_idx: 0,
        transaction_annotations: nil
    }

    {:ok, state}
  end

  defp handle_data(?R, msg, %State{} = state) do
    relation = Relation.parse_relation(msg, state.database_id, state.conn.())

    state = %{state | relations: Map.put(state.relations, relation.id, relation)}

    Enum.each(state.consumers, fn consumer ->
      state.consumer_mod.handle_relation(consumer, relation)
    end)

    {:ok, state}
  end

  defp handle_data(
         ?M,
         <<?M, _transactional::binary-1, _lsn::binary-8, "sequin:transaction_annotations."::binary, rest::binary>>,
         %State{} = state
       ) do
    [op, <<_length::integer-32, content::binary>>] = String.split(rest, <<0>>, parts: 2)

    case op do
      "set" ->
        state = %{state | transaction_annotations: content}
        {:ok, state}

      "clear" ->
        state = %{state | transaction_annotations: nil}
        {:ok, state}

      unknown ->
        Logger.warning("Unknown transaction annotation operation: #{unknown}")
        {:ok, state}
    end
  end

  defp handle_data(?M, <<?M, 0, _lsn::binary-8, _rest::binary>>, %State{} = state) do
    # Result of calling `select pg_logical_emit_message` with first argument as `false` (non-transactional)
    # This will appear outside of a transaction. We could parse the LSN from the logical message and emit, but
    # we don't use these so we can just ignore.

    {:ok, state}
  end

  @change_types [?I, ?U, ?D, ?M]
  defp handle_data(type, msg, %State{} = state) when below_restart_wal_cursor?(state) and type in @change_types do
    raw_bytes_received = byte_size(msg)
    ProcessMetrics.increment_throughput("raw_bytes_received", raw_bytes_received)

    {:ok, state}
  end

  defp handle_data(type, msg, %State{} = state) when type in @change_types do
    raw_bytes_received = byte_size(msg)
    ProcessMetrics.increment_throughput("raw_bytes_received", raw_bytes_received)

    msg = message_from_binary(state, msg)

    state =
      Map.update!(state, :accumulated_messages, fn acc ->
        %{acc | count: acc.count + 1, bytes: acc.bytes + raw_bytes_received, messages: [msg | acc.messages]}
      end)

    ProcessMetrics.gauge("accumulated_messages_count", state.accumulated_messages.count)
    ProcessMetrics.gauge("accumulated_messages_bytes", state.accumulated_messages.bytes)

    {:ok, state}
  end

  # Primary keepalive message from server:
  # https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-PRIMARY-KEEPALIVE-MESSAGE
  #
  # Byte1('k')      - Identifies message as a sender keepalive
  # Int64           - Current end of WAL on the server
  # Int64           - Server's system clock (microseconds since 2000-01-01 midnight)
  # Byte1           - 1 if reply requested immediately to avoid timeout, 0 otherwise
  # The server is not asking for a reply
  defp handle_data(?k, <<?k, wal_end::64, clock::64, reply_requested>>, %State{} = state) do
    diff_ms = Sequin.Time.microseconds_since_2000_to_ms_since_now(clock)
    log = "Received keepalive message for slot (reply_requested=#{reply_requested}) (clock_diff=#{diff_ms}ms)"
    log_meta = [clock: clock, wal_end: wal_end, diff_ms: diff_ms]

    if reply_requested == 1 do
      Logger.info(log, log_meta)
    else
      Logger.debug(log, log_meta)
    end

    {:ok, state}
  end

  defp handle_data(type, _msg, %State{} = state) do
    Logger.warning("Unsupported message type: #{type}")

    {:ok, state}
  end

  defp update_restart_wal_cursor(%State{} = state) do
    restart_wal_cursor =
      case state.restart_wal_cursor_fn.(state.id, state.restart_wal_cursor) do
        nil -> state.restart_wal_cursor
        next_cursor -> next_cursor
      end

    if not is_nil(state.restart_wal_cursor) and
         Postgres.compare_wal_cursors(restart_wal_cursor, state.restart_wal_cursor) == :lt do
      raise "New restart cursor is behind new restart cursor: #{inspect(restart_wal_cursor)} < #{inspect(state.restart_wal_cursor)}"
    end

    if is_nil(restart_wal_cursor) or is_nil(restart_wal_cursor.commit_lsn) or is_nil(restart_wal_cursor.commit_idx) do
      raise "[SlotProducer] restart_wal_cursor is empty"
    end

    Replication.put_restart_wal_cursor!(state.id, restart_wal_cursor)

    %{state | restart_wal_cursor: restart_wal_cursor}
  end

  defp message_from_binary(%State{commit_lsn: nil}, msg) do
    Logger.error("Got a change message outside of a transaction: #{inspect(msg)}")

    raise "Got a change message outside of a transaction"
  end

  defp message_from_binary(%State{} = state, binary) do
    kind =
      case binary do
        <<?I, _rest::binary>> -> :insert
        <<?U, _rest::binary>> -> :update
        <<?D, _rest::binary>> -> :delete
        <<?M, _rest::binary>> -> :logical
        <<?R, _rest::binary>> -> :relation
      end

    %Message{
      byte_size: byte_size(binary),
      commit_idx: state.next_commit_idx,
      commit_lsn: state.commit_lsn,
      commit_ts: state.commit_ts,
      kind: kind,
      payload: binary,
      transaction_annotations: state.transaction_annotations,
      message: nil
    }
  end

  defp maybe_produce_and_flush(%State{demand: demand} = state) when demand > 0 do
    {acc_msgs, remaining_msgs} = state.accumulated_messages.messages |> Enum.reverse() |> Enum.split(demand)
    acc_msgs = Enum.map(acc_msgs, fn %Message{} = msg -> %{msg | batch_idx: state.batch_idx} end)
    remaining_msgs = Enum.reverse(remaining_msgs)
    dropped_bytes = acc_msgs |> Enum.map(& &1.byte_size) |> Enum.sum()

    # Update last_dispatched_wal_cursor to the highest cursor from dispatched messages
    last_dispatched_wal_cursor =
      case acc_msgs do
        [] ->
          state.last_dispatched_wal_cursor

        msgs ->
          last_msg = List.last(msgs)
          %{commit_lsn: last_msg.commit_lsn, commit_idx: last_msg.commit_idx}
      end

    state = %{
      state
      | demand: state.demand - length(acc_msgs),
        last_dispatched_wal_cursor: last_dispatched_wal_cursor,
        accumulated_messages: %{
          state.accumulated_messages
          | count: state.accumulated_messages.count - length(acc_msgs),
            bytes: state.accumulated_messages.bytes - dropped_bytes,
            messages: remaining_msgs
        }
    }

    if acc_msgs == [] do
      {[], state}
    else
      {acc_msgs, maybe_schedule_flush(state)}
    end
  end

  defp maybe_produce_and_flush(state) do
    {[], state}
  end

  defp maybe_toggle_buffering(%State{status: :disconnected} = state), do: state

  defp maybe_toggle_buffering(%State{status: :buffering, demand: demand} = state) when demand > 0 do
    if state.buffered_sock_msg do
      send(self(), state.buffered_sock_msg)
    end

    %{state | status: :active, buffered_sock_msg: nil}
  end

  defp maybe_toggle_buffering(%State{status: :buffering} = state), do: state

  defp maybe_toggle_buffering(%State{status: :active, demand: 0} = state) do
    %{state | status: :buffering}
  end

  defp maybe_toggle_buffering(%State{status: :active} = state), do: state

  defp init_restart_wal_cursor(%State{} = state, protocol) do
    query = "select restart_lsn from pg_replication_slots where slot_name = '#{state.slot_name}'"

    case Replication.restart_wal_cursor(state.id) do
      {:error, %NotFoundError{}} ->
        case Protocol.handle_simple(query, [], protocol) do
          {:ok, [%Postgrex.Result{rows: [[lsn]]}], protocol} when not is_nil(lsn) ->
            cursor = %{commit_lsn: Postgres.lsn_to_int(lsn), commit_idx: 0}
            {:ok, %{state | restart_wal_cursor: cursor}, protocol}

          {:ok, _res} ->
            {:error,
             Error.not_found(
               entity: :source_replication_slot,
               message: "Error fetching metadata about the replication slot from Postgres"
             )}

          error ->
            {:error,
             Error.service(
               service: :source_postgres,
               message: "Error fetching metadata about the replication slot from Postgres (#{inspect(error)})"
             )}
        end

      {:ok, cursor} ->
        {:ok, %{state | restart_wal_cursor: cursor}, protocol}
    end
  end

  ## Helpers

  defp handle_disconnect(error, reason, %State{} = state) when error in [:error, :disconnect] do
    Logger.error("[SlotProducer] Replication disconnected: #{inspect(reason)}")
    Protocol.disconnect(%RuntimeError{}, state.protocol)
    Process.send_after(self(), :connect, state.setting_reconnect_interval)

    if is_function(state.on_disconnect) do
      state.on_disconnect.()
    end

    state =
      close_commit(%{
        state
        | status: :disconnected,
          last_commit_lsn: nil,
          last_commit_idx: nil,
          accumulated_messages: %{count: 0, bytes: 0, messages: []},
          buffered_sock_msg: nil
      })

    {:noreply, [], state}
  end

  defp close_commit(%State{} = state) do
    %{state | commit_ts: nil, commit_lsn: nil, next_commit_idx: nil, commit_xid: nil, transaction_annotations: nil}
  end

  defp schedule_timers(state) do
    state
    |> schedule_ack()
    |> schedule_update_cursor()
  end

  defp schedule_ack(%State{ack_timer: nil, setting_ack_interval: int} = state) do
    ref = Process.send_after(self(), :send_ack, int)
    %{state | ack_timer: ref}
  end

  defp schedule_ack(state), do: state

  defp schedule_update_cursor(
         %State{restart_wal_cursor_update_timer: nil, setting_restart_wal_cursor_update_interval: int} = state
       ) do
    ref = Process.send_after(self(), :update_restart_wal_cursor, int)
    %{state | restart_wal_cursor_update_timer: ref}
  end

  defp schedule_update_cursor(state), do: state

  # We always trigger a flush check after producing
  defp maybe_schedule_flush(%State{batch_flush_timer: nil} = state) do
    delay = batch_flush_interval(state)
    ref = Process.send_after(self(), :flush_batch, delay)
    %{state | batch_flush_timer: ref}
  end

  defp maybe_schedule_flush(state), do: state

  defp batch_flush_interval(%State{} = state) do
    state.setting_batch_flush_interval || batch_flush_interval()
  end

  defp ack_message(lsn) when is_integer(lsn) do
    [<<?r, lsn::64, lsn::64, lsn::64, current_time()::64, 0>>]
  end

  defp put_connected_health(id) do
    Health.put_event(
      :postgres_replication_slot,
      id,
      %Event{slug: :replication_connected, status: :success}
    )
  end

  defp send_ack(%State{} = state) do
    delta =
      case state.last_sent_restart_wal_cursor do
        nil -> 0
        last_sent -> state.restart_wal_cursor.commit_lsn - last_sent.commit_lsn
      end

    if delta < 0 do
      raise """
        Was about to send lower restart_wal_cursor:
          restart_wal_cursor=#{inspect(state.restart_wal_cursor)}
          last_sent_restart_wal_cursor=#{inspect(state.last_sent_restart_wal_cursor)}
      """
    end

    Logger.info("[SlotProducer] Sending ack for LSN #{state.restart_wal_cursor.commit_lsn} (+#{delta})",
      delta_bytes: delta
    )

    msg = ack_message(state.restart_wal_cursor.commit_lsn)

    case Protocol.handle_copy_send(msg, state.protocol) do
      :ok ->
        {:noreply, [], %{state | last_sent_restart_wal_cursor: state.restart_wal_cursor}}

      {error, reason, protocol} ->
        handle_disconnect(error, reason, %{state | protocol: protocol})
    end
  end

  defp start_replication_query(%State{} = state) do
    if state.pg_major_version >= 14 do
      "START_REPLICATION SLOT #{state.slot_name} LOGICAL 0/0 (proto_version '1', publication_names '#{state.publication_name}', messages 'true')"
    else
      "START_REPLICATION SLOT #{state.slot_name} LOGICAL 0/0 (proto_version '1', publication_names '#{state.publication_name}')"
    end
  end

  @epoch DateTime.to_unix(~U[2000-01-01 00:00:00Z], :microsecond)
  defp current_time, do: System.os_time(:microsecond) - @epoch
end

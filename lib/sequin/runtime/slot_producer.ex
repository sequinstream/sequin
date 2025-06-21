defmodule Sequin.Runtime.SlotProducer do
  @moduledoc """
  A GenStage producer that streams PostgreSQL replication messages.

  This module uses `Postgrex.Protocol` to establish a replication
  connection and emits WAL messages downstream on demand.
  """

  use GenStage

  use Sequin.ProcessMetrics,
    metric_prefix: "sequin.slot_producer"

  alias Postgrex.Protocol
  alias Sequin.Postgres
  alias Sequin.ProcessMetrics
  alias Sequin.Replication
  alias Sequin.Runtime.PostgresAdapter.Decoder
  alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.Begin
  alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.Commit
  alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.LogicalMessage

  require Logger

  defguardp is_socket_message(message)
            when elem(message, 0) in [:tcp, :tcp_closed, :tcp_error, :ssl, :ssl_closed, :ssl_error]

  @max_messages_per_protocol_read 500

  # 100 MB
  # @max_accumulated_bytes 100 * 1024 * 1024
  # @max_accumulated_messages 2500
  # @backfill_batch_high_watermark Constants.backfill_batch_high_watermark()

  # @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])
  # @stream_schema Application.compile_env(:sequin, [Sequin.Repo, :stream_schema_prefix])

  # @logical_message_table_name Constants.logical_messages_table_name()

  # def max_accumulated_bytes do
  #   get_config(:max_accumulated_bytes) || @max_accumulated_bytes
  # end

  # def max_accumulated_messages do
  #   get_config(:max_accumulated_messages) || @max_accumulated_messages
  # end

  # def set_max_accumulated_messages(value) do
  #   put_config(:max_accumulated_messages, value)
  # end

  # def set_max_accumulated_bytes(value) do
  #   put_config(:max_accumulated_bytes, value)
  # end

  # def set_max_accumulated_messages_time_ms(value) do
  #   put_config(:max_accumulated_messages_time_ms, value)
  # end

  # def set_retry_flush_after_ms(value) do
  #   put_config(:retry_flush_after_ms, value)
  # end

  # defp get_config(key) do
  #   config = Application.get_env(:sequin, __MODULE__, [])
  #   Keyword.get(config, key)
  # end

  # defp put_config(key, value) do
  #   current_config = Application.get_env(:sequin, __MODULE__, [])
  #   updated_config = Keyword.put(current_config, key, value)
  #   Application.put_env(:sequin, __MODULE__, updated_config)
  # end
  #
  #
  defmodule Message do
    @moduledoc false
    use TypedStruct

    typedstruct enforce: true do
      field :byte_size, integer()
      field :commit_idx, integer()
      field :commit_lsn, integer()
      field :commit_ts, DateTime.t()
      field :kind, :insert | :update | :delete
      field :payload, binary()
      field :transaction_annotations, String.t()
    end
  end

  defmodule State do
    @moduledoc false
    use TypedStruct

    typedstruct do
      field :id, String.t()
      field :slot_name, String.t()
      # Postgres replication connection
      field :protocol, Protocol.t()
      field :connect_opts, keyword()
      field :start_replication_query, String.t()
      field :handle_connect_fail, (any() -> any())
      field :handle_disconnected, (any() -> any())
      field :ingest_enabled_fn, (-> boolean())
      field :setting_reconnect_interval, non_neg_integer()
      field :status, :active | :disconnected
      field :last_commit_lsn, nil | integer()

      # Cursors & aliveness
      field :safe_wal_cursor, Replication.wal_cursor()
      field :safe_wal_cursor_fn, (State.t() -> Replication.wal_cursor())
      field :setting_update_cursor_interval, non_neg_integer()
      field :setting_ack_interval, non_neg_integer()
      field :ack_timer, reference()
      field :update_cursor_timer, reference()

      # Current xaction state
      field :commit_ts, DateTime.t()
      field :commit_lsn, integer()
      field :commit_idx, integer()
      field :commit_xid, integer()
      field :transaction_annotations, nil | String.t()

      field :demand, integer(), default: 0
      field :socket_state, :active | :buffer_once

      # Buffers
      field :accumulated_messages, %{count: non_neg_integer(), bytes: non_neg_integer(), messages: [Message.t()]},
        default: %{count: 0, bytes: 0, messages: []}

      field :buffered_sock_msg, nil | String.t()
      field :buffering_sock_msgs?, boolean(), default: false
    end
  end

  def start_link(opts) do
    id = Keyword.fetch!(opts, :id)
    GenStage.start_link(__MODULE__, opts, name: via_tuple(id))
  end

  def via_tuple(id) do
    {:via, :syn, {:replication, {__MODULE__, id}}}
  end

  @impl GenStage
  def init(opts) do
    connect_opts = Keyword.fetch!(opts, :connect_opts)
    start_query = Keyword.fetch!(opts, :start_replication_query)

    connect_opts = Keyword.put(connect_opts, :parameters, replication: "database")

    state = %State{
      id: Keyword.fetch!(opts, :id),
      slot_name: Keyword.fetch!(opts, :slot_name),
      connect_opts: connect_opts,
      start_replication_query: start_query,
      status: :disconnected,
      ingest_enabled_fn: Keyword.get(opts, :ingest_enabled_fn, fn -> true end),
      handle_connect_fail: Keyword.get(opts, :handle_connect_fail),
      handle_disconnected: Keyword.get(opts, :handle_disconnected),
      safe_wal_cursor_fn: Keyword.fetch!(opts, :safe_wal_cursor_fn),
      setting_reconnect_interval: Keyword.get(opts, :reconnect_interval, :timer.seconds(10)),
      setting_ack_interval: Keyword.get(opts, :ack_interval, :timer.seconds(10)),
      setting_update_cursor_interval: Keyword.get(opts, :update_cursor_interval, :timer.seconds(30))
    }

    Process.send_after(self(), :connect, 0)
    {:producer, state}
  end

  @impl GenStage
  def handle_demand(incoming, %{demand: demand} = state) do
    {messages, state} = maybe_produce(%{state | demand: demand + incoming})
    {:noreply, messages, state}
  end

  @impl GenStage
  def handle_info(:connect, state) do
    with {:ok, protocol} <- Protocol.connect(state.connect_opts),
         Logger.info("[SlotProducer] Connected"),
         {:ok, state, protocol} <- init_safe_wal_cursor(state, protocol),
         {:ok, protocol} <- Protocol.handle_streaming(state.start_replication_query, protocol),
         {:ok, protocol} <- Protocol.checkin(protocol) do
      state = %{state | protocol: protocol, status: :active}
      state = schedule_timers(state)
      {:noreply, [], state}
    else
      {:error, reason} ->
        Logger.error("[SlotProducer] replication connect failed: #{inspect(reason)}")
        if fun = state.handle_connect_fail, do: fun.(reason)
        Process.send_after(self(), :connect, state.setting_reconnect_interval)
        {:noreply, [], state}
    end
  end

  @impl GenStage
  def handle_info(msg, %State{buffering_sock_msgs?: true, buffered_sock_msg: nil} = s) when is_socket_message(msg) do
    {:keep_state, %{s | buffered_sock_msg: msg}}
  end

  def handle_info(msg, %State{buffering_sock_msgs?: true}) when is_socket_message(msg) do
    raise "Unexpectedly received a second socket message while buffering sock messages"
  end

  def handle_info(msg, %State{protocol: protocol} = s) when is_socket_message(msg) do
    with {:ok, copies, protocol} <- Protocol.handle_copy_recv(msg, @max_messages_per_protocol_read, protocol),
         {:ok, state} <- handle_copies(copies, %{s | protocol: protocol}) do
      {messages, state} = maybe_produce(state)
      {:noreply, messages, state}
    else
      {error, reason, protocol} ->
        handle_disconnect(error, reason, %{s | protocol: protocol})
    end
  end

  def handle_info(:send_ack, %State{status: :disconnected} = state) do
    state = schedule_ack(%{state | ack_timer: nil})
    {:noreply, [], state}
  end

  def handle_info(:send_ack, %State{} = state) do
    Logger.info("[SlotProcessorServer] Sending ack for LSN #{state.safe_wal_cursor.commit_lsn}")

    # TODO: safe_wal_cursor.commit_lsn can be 0 -- bad. Do not want to send in that case
    msg = ack_message(state.safe_wal_cursor.commit_lsn)
    state = schedule_ack(%{state | ack_timer: nil})

    case Protocol.handle_copy_send(msg, state.protocol) do
      :ok ->
        {:noreply, [], state}

      {error, reason, protocol} ->
        handle_disconnect(error, reason, %{state | protocol: protocol})
    end
  end

  def handle_info(:update_safe_wal_cursor, %State{} = state) do
    state = update_safe_wal_cursor(state)

    state = schedule_update_cursor(%{state | update_cursor_timer: nil})
    {:noreply, [], state}
  end

  defp handle_copies(copies, state) do
    Enum.reduce_while(copies, {:ok, state}, fn copy, {:ok, state} ->
      case handle_data(copy, state) do
        {:ok, state} -> {:cont, {:ok, state}}
        error -> {:halt, error}
      end
    end)
  end

  defp handle_data(<<?w, _header::192, ?B, rest::binary>>, %State{} = state) do
    %State{last_commit_lsn: last_commit_lsn} = state
    %Begin{commit_timestamp: ts, final_lsn: lsn, xid: xid} = Decoder.decode_message(<<?B, rest::binary>>)
    begin_lsn = Postgres.lsn_to_int(lsn)

    if not is_nil(last_commit_lsn) and begin_lsn < last_commit_lsn do
      raise "Received a Begin message with an LSN that is less than the last commit LSN (#{begin_lsn} < #{last_commit_lsn})"
    end

    state = %State{state | commit_ts: ts, commit_idx: 0, commit_lsn: begin_lsn, commit_xid: xid}
    {:ok, state}
  end

  defp handle_data(<<?w, _header::192, ?C, msg::binary>>, %State{} = state) do
    %State{commit_lsn: commit_lsn, commit_ts: commit_ts} = state
    %Commit{} = commit = Decoder.decode_message(<<?C, msg::binary>>)
    recv_lsn = Postgres.lsn_to_int(commit.lsn)

    if commit_lsn != recv_lsn or commit_ts != commit.commit_timestamp do
      error = """
      Unexpectedly received a commit that does not match current commit or ts
      lsn: #{commit_lsn} != #{recv_lsn}
      ts: #{inspect(commit_ts)} != #{inspect(commit.commit_timestamp)})
      """

      raise error
    end

    state = %State{
      state
      | last_commit_lsn: commit_lsn,
        commit_lsn: nil,
        commit_ts: nil,
        commit_xid: nil,
        commit_idx: 0,
        transaction_annotations: nil
    }

    {:ok, state}
  end

  defp handle_data(<<?w, _header::192, ?R, _msg::binary>>, %State{} = state) do
    {:ok, state}
  end

  defp handle_data(<<?w, _header::192, ?M, rest::binary>>, %State{} = state) do
    %LogicalMessage{} = message = Decoder.decode_message(<<?M, rest::binary>>)
    handle_data(message, state)
  end

  defp handle_data(%LogicalMessage{prefix: "sequin:transaction_annotations.set", content: content}, state) do
    state = %{state | transaction_annotations: content}
    {:ok, state}
  end

  defp handle_data(%LogicalMessage{prefix: "sequin:transaction_annotations.clear"}, state) do
    state = %{state | transaction_annotations: nil}
    {:ok, state}
  end

  # defp process_message(%State{} = state, %Message{} = msg) do
  #   state =
  #     if logical_message_table_upsert?(state, msg) do
  #       content = Enum.find(msg.fields, fn field -> field.column_name == "content" end)
  #       handle_logical_message_content(state, content.value)
  #     else
  #       state
  #     end

  #   msg = %Message{
  #     msg
  #     | commit_lsn: state.current_xaction_lsn,
  #       commit_idx: state.current_commit_idx,
  #       commit_timestamp: state.current_commit_ts,
  #       transaction_annotations: state.transaction_annotations
  #   }

  #   {%State{state | current_commit_idx: state.current_commit_idx + 1}, msg}
  # end

  # defp process_message(%State{} = state, %LogicalMessage{prefix: "sequin.heartbeat.1", content: content}) do
  #   handle_logical_message_content(state, content)
  # end

  # defp process_message(%State{} = state, %LogicalMessage{prefix: "sequin.heartbeat.0", content: emitted_at}) do
  #   Logger.info("[SlotProcessorServer] Legacy heartbeat received", emitted_at: emitted_at)
  #   state
  # end

  # defp process_message(%State{} = state, %LogicalMessage{prefix: @backfill_batch_high_watermark} = msg) do
  #   %State{state | backfill_watermark_messages: [msg | state.backfill_watermark_messages]}
  # end

  defp handle_data(<<?w, _header::192, msg::binary>>, %State{} = state) do
    if is_nil(state.last_commit_lsn) and state.accumulated_messages.count == 0 do
      Logger.info("Received first message from slot (`last_commit_lsn` was nil)")
    end

    raw_bytes_received = byte_size(msg)
    ProcessMetrics.increment_throughput("raw_bytes_received", raw_bytes_received)

    msg = message_from_binary(state, msg)

    state =
      Map.update!(state, :accumulated_messages, fn acc ->
        %{acc | count: acc.count + 1, bytes: acc.bytes + raw_bytes_received, messages: [msg | acc.messages]}
      end)

    # state = maybe_schedule_flush(state)

    ProcessMetrics.gauge("accumulated_messages_count", state.accumulated_messages.count)
    ProcessMetrics.gauge("accumulated_messages_bytes", state.accumulated_messages.bytes)

    {:ok, %{state | commit_idx: state.commit_idx + 1}}
  end

  # Primary keepalive message from server:
  # https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-PRIMARY-KEEPALIVE-MESSAGE
  #
  # Byte1('k')      - Identifies message as a sender keepalive
  # Int64           - Current end of WAL on the server
  # Int64           - Server's system clock (microseconds since 2000-01-01 midnight)
  # Byte1           - 1 if reply requested immediately to avoid timeout, 0 otherwise
  # The server is not asking for a reply
  defp handle_data(<<?k, wal_end::64, clock::64, reply_requested>>, %State{} = state) do
    diff_ms = Sequin.Time.microseconds_since_2000_to_ms_since_now(clock)

    Logger.info("Received keepalive message for slot (reply_requested=#{reply_requested}) (clock_diff=#{diff_ms}ms)",
      clock: clock,
      wal_end: wal_end,
      diff_ms: diff_ms
    )

    {:ok, state}
  end

  defp update_safe_wal_cursor(%State{} = state) do
    safe_wal_cursor =
      if is_nil(state.last_commit_lsn) do
        # If we don't have a last_commit_lsn, we're still processing the first xaction
        # we received on boot. This can happen if we're processing a very large xaction.
        # It is therefore safe to send an ack with the last LSN we processed.
        state.safe_wal_cursor
      else
        state.safe_wal_cursor_fn.(state)
      end

    if is_nil(safe_wal_cursor) do
      Logger.info("[SlotProcessorServer] safe_wal_cursor=nil, skipping put_restart_wal_cursor!")
    else
      Replication.put_restart_wal_cursor!(state.id, safe_wal_cursor)
    end

    %{state | safe_wal_cursor: safe_wal_cursor}
  end

  defp message_from_binary(%State{} = state, binary) do
    kind =
      case binary do
        <<?I, _rest::binary>> -> :insert
        <<?U, _rest::binary>> -> :update
        <<?D, _rest::binary>> -> :delete
      end

    %Message{
      byte_size: byte_size(binary),
      commit_idx: state.commit_idx,
      commit_lsn: state.commit_lsn,
      commit_ts: state.commit_ts,
      kind: kind,
      payload: binary,
      transaction_annotations: state.transaction_annotations
    }
  end

  defp maybe_produce(%State{demand: demand} = state) when demand > 0 do
    {acc_msgs, remaining_msgs} = state.accumulated_messages.messages |> Enum.reverse() |> Enum.split(demand)
    remaining_msgs = Enum.reverse(remaining_msgs)
    dropped_bytes = acc_msgs |> Enum.map(& &1.byte_size) |> Enum.sum()

    state = %{
      state
      | demand: state.demand - length(acc_msgs),
        accumulated_messages: %{
          state.accumulated_messages
          | count: state.accumulated_messages.count - length(acc_msgs),
            bytes: state.accumulated_messages.bytes - dropped_bytes,
            messages: remaining_msgs
        }
    }

    {acc_msgs, state}
  end

  defp maybe_produce(state) do
    {[], state}
  end

  defp init_safe_wal_cursor(%State{} = state, protocol) do
    query = "select restart_lsn from pg_replication_slots where slot_name = '#{state.slot_name}'"

    case Replication.restart_wal_cursor(state.id) do
      # TODO: Refactor to return not found
      {:ok, %{commit_lsn: 0}} ->
        case Protocol.handle_simple(query, [], protocol) do
          {:ok, [%Postgrex.Result{rows: [[lsn]]}], protocol} ->
            cursor = %{commit_lsn: Postgres.lsn_to_int(lsn), commit_idx: 0}
            {:ok, %State{state | safe_wal_cursor: cursor}, protocol}

          error ->
            error
        end

      {:ok, cursor} ->
        {:ok, %State{state | safe_wal_cursor: cursor}}
    end
  end

  # def handle_info(:send_ack, state) do
  #   state = %{state | ack_timer: nil}

  #   if state.protocol do
  #     reply = ack_message(state.safe_wal_cursor.commit_lsn)

  #     case Protocol.handle_copy_send(reply, state.protocol) do
  #       :ok -> :ok
  #       {_, reason, _} -> Logger.error("ack failed: #{inspect(reason)}")
  #     end
  #   end

  #   {:noreply, [], schedule_ack(state)}
  # end

  # def handle_info(:update_safe_wal_cursor, state) do
  #   state = %{state | update_cursor_timer: nil}
  #   cursor = state.safe_wal_cursor_fn.(state)
  #   state = %{state | safe_wal_cursor: cursor}
  #   {:noreply, [], schedule_update_cursor(state)}
  # end

  ## Helpers

  defp handle_disconnect(error, reason, %State{} = state) when error in [:error, :disconnect] do
    Logger.error("[SlotProducer] Replication disconnected: #{inspect(reason)}")
    Protocol.disconnect(%RuntimeError{}, state.protocol)
    Process.send_after(self(), :connect, state.setting_reconnect_interval)
    # todo: reset state
    state
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

  defp schedule_update_cursor(%State{update_cursor_timer: nil, setting_update_cursor_interval: int} = state) do
    ref = Process.send_after(self(), :update_safe_wal_cursor, int)
    %{state | update_cursor_timer: ref}
  end

  defp schedule_update_cursor(state), do: state

  defp ack_message(lsn) when is_integer(lsn) do
    [<<?r, lsn::64, lsn::64, lsn::64, current_time()::64, 0>>]
  end

  @epoch DateTime.to_unix(~U[2000-01-01 00:00:00Z], :microsecond)
  defp current_time, do: System.os_time(:microsecond) - @epoch
end

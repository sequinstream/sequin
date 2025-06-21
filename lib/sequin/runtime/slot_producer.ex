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
  alias Sequin.ProcessMetrics

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

  defmodule State do
    @moduledoc false
    use TypedStruct

    typedstruct do
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
      field :restart_cursor, Replication.wal_cursor()
      field :safe_wal_cursor, Replication.wal_cursor()
      field :safe_wal_cursor_fn, (State.t() -> Replication.wal_cursor())
      field :setting_update_cursor_interval, non_neg_integer()
      field :setting_heartbeat_interval, non_neg_integer()
      field :heartbeat_timer, reference()
      field :setting_ack_interval, non_neg_integer()
      field :ack_timer, reference()
      field :setting_update_cursor_timer, reference()
      field :message_received_since_last_heartbeat, boolean(), default: false

      # Current xaction state
      field :commit_lsn, integer()
      field :commit_idx, integer()

      field :demand, integer(), default: 0
      field :socket_state, :active | :buffer_once

      # Buffers
      field :accumulated_msg_binaries, %{count: non_neg_integer(), bytes: non_neg_integer(), binaries: [binary()]},
        default: %{count: 0, bytes: 0, binaries: []}

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
      connect_opts: connect_opts,
      start_replication_query: start_query,
      status: :disconnected,
      restart_cursor: Keyword.get(opts, :restart_cursor),
      ingest_enabled_fn: Keyword.get(opts, :ingest_enabled_fn, fn -> true end),
      handle_connect_fail: Keyword.get(opts, :handle_connect_fail),
      handle_disconnected: Keyword.get(opts, :handle_disconnected),
      safe_wal_cursor_fn: Keyword.fetch!(opts, :safe_wal_cursor_fn),
      setting_reconnect_interval: Keyword.get(opts, :reconnect_interval, :timer.seconds(10)),
      setting_heartbeat_interval: Keyword.get(opts, :heartbeat_interval, :timer.seconds(15)),
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
         {:ok, protocol} <- Protocol.handle_streaming(state.start_replication_query, protocol),
         {:ok, protocol} <- Protocol.checkin(protocol) do
      state = %{state | protocol: protocol, status: :active}
      # state = schedule_timers(state)
      # state = maybe_activate_socket(state)
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

  @impl GenStage

  def handle_info(msg, %State{buffering_sock_msgs?: true}) when is_socket_message(msg) do
    raise "Unexpectedly received a second socket message while buffering sock messages"
  end

  @impl GenStage
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

  defp handle_copies(copies, state) do
    Enum.reduce_while(copies, {:ok, state}, fn copy, {:ok, state} ->
      case handle_data(copy, state) do
        {:ok, state} -> {:cont, {:ok, state}}
        error -> {:halt, error}
      end
    end)
  end

  defp handle_data(<<?w, _header::192, msg::binary>>, %State{} = state) do
    if is_nil(state.last_commit_lsn) and state.accumulated_msg_binaries.count == 0 do
      Logger.info("Received first message from slot (`last_commit_lsn` was nil)")
    end

    raw_bytes_received = byte_size(msg)
    ProcessMetrics.increment_throughput("raw_bytes_received", raw_bytes_received)

    state = %{state | message_received_since_last_heartbeat: true}

    state =
      Map.update!(state, :accumulated_msg_binaries, fn acc ->
        %{acc | count: acc.count + 1, bytes: acc.bytes + raw_bytes_received, binaries: [msg | acc.binaries]}
      end)

    # state = maybe_schedule_flush(state)

    ProcessMetrics.gauge("accumulated_msg_binaries_count", state.accumulated_msg_binaries.count)
    ProcessMetrics.gauge("accumulated_msg_binaries_bytes", state.accumulated_msg_binaries.bytes)

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

  defp handle_data(<<?k, _wal_end::64, _clock::64, _reply_requested>>, %State{} = state) do
    # Because these are <14 Postgres databases, they will not receive heartbeat messages
    # temporarily mark them as healthy if we receive a keepalive message

    # send_ack? =
    #   cond do
    #     reply_requested == 1 ->
    #       true

    #     is_nil(state.last_lsn_acked_at) ->
    #       true

    #     DateTime.diff(Sequin.utc_now(), state.last_lsn_acked_at, :second) > 60 ->
    #       true

    #     true ->
    #       false
    #   end

    # # Check if we should send an ack even though not requested
    # if send_ack? do
    #   safe_wal_cursor = state.safe_wal_cursor
    #   diff_ms = Time.microseconds_since_2000_to_ms_since_now(clock)

    #   Logger.info("Received keepalive message for slot (reply_requested=#{reply_requested}) (clock_diff=#{diff_ms}ms)",
    #     clock: clock,
    #     wal_end: wal_end,
    #     diff_ms: diff_ms
    #   )

    #   if safe_wal_cursor.commit_lsn > wal_end do
    #     Logger.warning("Server LSN #{wal_end} is behind our LSN #{safe_wal_cursor.commit_lsn}")
    #   end

    #   Logger.info(
    #     "Acking LSN #{inspect(safe_wal_cursor.commit_lsn)} (current server LSN: #{wal_end}) (last_commit_lsn: #{state.last_commit_lsn})"
    #   )

    #   reply = ack_message(safe_wal_cursor.commit_lsn)
    #   state = %{state | last_lsn_acked_at: Sequin.utc_now()}
    #   {:keep_state_and_ack, reply, state}
    # else
    #   {:keep_state, state}
    # end
    {:ok, state}
  end

  defp maybe_produce(%State{demand: demand} = state) when demand > 0 do
    {acc_binaries, remaining_binaries} = state.accumulated_msg_binaries.binaries |> Enum.reverse() |> Enum.split(demand)
    remaining_binaries = Enum.reverse(remaining_binaries)
    dropped_bytes = Enum.reduce(acc_binaries, 0, fn binary, acc -> acc + byte_size(binary) end)

    state = %{
      state
      | demand: state.demand - length(acc_binaries),
        accumulated_msg_binaries: %{
          state.accumulated_msg_binaries
          | count: state.accumulated_msg_binaries.count - length(acc_binaries),
            bytes: state.accumulated_msg_binaries.bytes - dropped_bytes,
            binaries: remaining_binaries
        }
    }

    {acc_binaries, state}
  end

  defp maybe_produce(state) do
    {[], state}
  end

  # def handle_info(:emit_heartbeat, state) do
  #   state = %{state | heartbeat_timer: nil}

  #   case send_heartbeat(state) do
  #     {:ok, lsn} ->
  #       {:noreply, [], schedule_heartbeat(%{state | commit_lsn: lsn})}

  #     {:error, reason} ->
  #       Logger.error("[SlotProducer] heartbeat error: #{inspect(reason)}")
  #       {:noreply, [], schedule_heartbeat(state)}
  #   end
  # end

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
    # state
  end

  # defp schedule_timers(state) do
  #   state
  #   |> schedule_heartbeat()
  #   |> schedule_ack()
  #   |> schedule_update_cursor()
  # end

  # defp schedule_heartbeat(%{heartbeat_timer: nil, heartbeat_interval: int} = state) do
  #   ref = Process.send_after(self(), :emit_heartbeat, int)
  #   %{state | heartbeat_timer: ref}
  # end

  # defp schedule_heartbeat(state), do: state

  # defp schedule_ack(%{ack_timer: nil, ack_interval: int} = state) do
  #   ref = Process.send_after(self(), :send_ack, int)
  #   %{state | ack_timer: ref}
  # end

  # defp schedule_ack(state), do: state

  # defp schedule_update_cursor(%{update_cursor_timer: nil, update_cursor_interval: int} = state) do
  #   ref = Process.send_after(self(), :update_safe_wal_cursor, int)
  #   %{state | update_cursor_timer: ref}
  # end

  # defp schedule_update_cursor(state), do: state

  # defp send_heartbeat(state) do
  #   payload = "{}"
  #   conn = state.protocol

  #   case Protocol.handle_simple("SELECT pg_logical_emit_message(true, 'sequin.heartbeat.1', '#{payload}')", [], conn) do
  #     {:ok, [%Postgrex.Result{rows: [[lsn]]}], protocol} ->
  #       {:ok, %{state | protocol: protocol}, lsn}

  #     {:error, reason, protocol} ->
  #       {:error, reason}
  #   end
  # end

  # defp ack_message(lsn) when is_integer(lsn) do
  #   [<<?r, lsn::64, lsn::64, lsn::64, current_time()::64, 0>>]
  # end

  # @epoch DateTime.to_unix(~U[2000-01-01 00:00:00Z], :microsecond)
  # defp current_time, do: System.os_time(:microsecond) - @epoch
end

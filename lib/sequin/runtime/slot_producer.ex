defmodule Sequin.Runtime.SlotProducer do
  @moduledoc """
  A GenStage producer that streams PostgreSQL replication messages.

  This module uses `Postgrex.Protocol` to establish a replication
  connection and emits WAL messages downstream on demand.
  """

  use GenStage

  alias Postgrex.Protocol
  alias Sequin.Postgres.ProtocolUtils

  require Logger

  defmodule State do
    @moduledoc false
    use TypedStruct

    typedstruct do
      # Postgres replication connection
      field :postgrex_protocol, Protocol.t()
      field :connect_opts, keyword()
      field :start_replication_query, String.t()
      field :handle_connect_fail, (any() -> any())
      field :handle_disconnected, (any() -> any())
      field :ingest_enabled_fn, (-> boolean())
      field :setting_reconnect_interval, non_neg_integer()
      field :status, :active | :disconnected

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

      # Current xaction state
      field :commit_lsn, integer()
      field :commit_idx, integer()

      field :demand, integer(), default: 0
      field :socket_state, :active | :buffer_once
      field :buffered_msg, nil | String.t()
    end
  end

  @impl GenStage
  def init(opts) do
    connect_opts = Keyword.fetch!(opts, :connect_opts)
    start_query = Keyword.fetch!(opts, :start_replication_query)

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
      heartbeat_interval: Keyword.get(opts, :heartbeat_interval, :timer.seconds(15)),
      ack_interval: Keyword.get(opts, :ack_interval, :timer.seconds(10)),
      update_cursor_interval: Keyword.get(opts, :update_cursor_interval, :timer.seconds(30))
    }

    Process.send_after(self(), :connect, 0)
    {:producer, state}
  end

  @impl GenStage
  def handle_demand(incoming, %{demand: demand} = state) do
    maybe_activate_socket(%{state | demand: demand + incoming})
  end

  @impl GenStage
  def handle_info(:connect, state) do
    with {:ok, protocol} <- Protocol.connect(state.connect_opts),
         Logger.info("[SlotProducer] Connected"),
         {:ok, protocol} <- Protocol.handle_streaming(state.start_replication_query, protocol),
         {:ok, protocol} <- Protocol.checkin(protocol) do
      state = %{state | protocol: protocol, status: :active}
      state = schedule_timers(state)
      state = maybe_activate_socket(state)
      {:noreply, [], state}
    else
      {:error, reason} ->
        Logger.error("[SlotProducer] replication connect failed: #{inspect(reason)}")
        if fun = state.handle_connect_fail, do: fun.(reason)
        Process.send_after(self(), :connect, state.setting_reconnect_interval)
        {:noreply, [], state}
    end
  end

  def handle_info(:emit_heartbeat, state) do
    state = %{state | heartbeat_timer: nil}

    case send_heartbeat(state) do
      {:ok, lsn} ->
        {:noreply, [], schedule_heartbeat(%{state | commit_lsn: lsn})}

      {:error, reason} ->
        Logger.error("[SlotProducer] heartbeat error: #{inspect(reason)}")
        {:noreply, [], schedule_heartbeat(state)}
    end
  end

  def handle_info(:send_ack, state) do
    state = %{state | ack_timer: nil}

    if state.protocol do
      reply = ack_message(state.safe_wal_cursor.commit_lsn)

      case Protocol.handle_copy_send(reply, state.protocol) do
        :ok -> :ok
        {_, reason, _} -> Logger.error("ack failed: #{inspect(reason)}")
      end
    end

    {:noreply, [], schedule_ack(state)}
  end

  def handle_info(:update_safe_wal_cursor, state) do
    state = %{state | update_cursor_timer: nil}
    cursor = state.safe_wal_cursor_fn.(state)
    state = %{state | safe_wal_cursor: cursor}
    {:noreply, [], schedule_update_cursor(state)}
  end

  def handle_info({tag, sock, data}, %{protocol: %{sock: {mod, sock}}} = state) when tag in [:tcp, :ssl] do
    state = %{state | socket_active: false}

    case Sequin.Postgres.ProtocolUtils.recv_replication_data({tag, sock, data}, state.protocol) do
      {:ok, messages, protocol} ->
        state = %{state | protocol: protocol}
        {events, state} = decode_messages(messages, state)
        state = maybe_activate_socket(state)
        {:noreply, events, state}

      {kind, reason, protocol} ->
        handle_disconnect(kind, reason, %{state | protocol: protocol})
    end
  end

  def handle_info({tag, sock}, %{protocol: %{sock: {mod, sock}}} = state) when tag in [:tcp_closed, :ssl_closed] do
    handle_disconnect(:disconnect, :closed, state)
  end

  def handle_info({tag, sock, reason}, %{protocol: %{sock: {mod, sock}}} = state) when tag in [:tcp_error, :ssl_error] do
    handle_disconnect(:disconnect, reason, state)
  end

  def handle_info(_msg, state), do: {:noreply, [], state}

  ## Helpers

  defp handle_disconnect(kind, reason, state) do
    Logger.error("replication disconnected: #{inspect(reason)}")
    if fun = state.handle_disconnected, do: fun.(reason)
    Protocol.disconnect(%RuntimeError{}, state.protocol)
    Process.send_after(self(), :connect, 1_000)
    {:noreply, [], %{state | protocol: nil, socket_active: false}}
  end

  defp maybe_activate_socket(%{protocol: nil} = state), do: {:noreply, [], state}

  defp maybe_activate_socket(%{socket_active: true} = state), do: {:noreply, [], state}

  defp maybe_activate_socket(%{protocol: protocol, demand: demand, read_messages?: read?} = state) do
    if demand > 0 and read?.() do
      case Protocol.checkout(protocol) do
        {:ok, protocol} ->
          :ok = :inet.setopts(elem(protocol.sock, 1), active: :once)
          {:noreply, [], %{state | protocol: protocol, socket_active: true, demand: demand}}

        other ->
          {:noreply, [], %{state | protocol: protocol}}
      end
    else
      {:noreply, [], state}
    end
  end

  defp schedule_timers(state) do
    state
    |> schedule_heartbeat()
    |> schedule_ack()
    |> schedule_update_cursor()
  end

  defp schedule_heartbeat(%{heartbeat_timer: nil, heartbeat_interval: int} = state) do
    ref = Process.send_after(self(), :emit_heartbeat, int)
    %{state | heartbeat_timer: ref}
  end

  defp schedule_heartbeat(state), do: state

  defp schedule_ack(%{ack_timer: nil, ack_interval: int} = state) do
    ref = Process.send_after(self(), :send_ack, int)
    %{state | ack_timer: ref}
  end

  defp schedule_ack(state), do: state

  defp schedule_update_cursor(%{update_cursor_timer: nil, update_cursor_interval: int} = state) do
    ref = Process.send_after(self(), :update_safe_wal_cursor, int)
    %{state | update_cursor_timer: ref}
  end

  defp schedule_update_cursor(state), do: state

  defp send_heartbeat(state) do
    payload = "{}"
    conn = state.protocol

    case Protocol.handle_simple("SELECT pg_logical_emit_message(true, 'sequin.heartbeat.1', '#{payload}')", [], conn) do
      {:ok, [%Postgrex.Result{rows: [[lsn]]}], protocol} ->
        {:ok, %{state | protocol: protocol}, lsn}

      {:error, reason, protocol} ->
        {:error, reason}
    end
  end

  defp decode_messages([], state), do: {[], state}

  defp decode_messages(messages, state) do
    {events, state} =
      Enum.map_reduce(messages, state, fn bin, state ->
        decode_message(bin, state)
      end)

    {events, state}
  end

  defp decode_message(<<?k, wal_end::64, clock::64, reply>>, state) do
    # keepalive
    send_ack? = reply == 1
    state = %{state | commit_lsn: wal_end, commit_idx: state.commit_idx}
    if send_ack?, do: send(self(), :send_ack)
    {{:keepalive, wal_end, clock}, state}
  end

  defp decode_message(<<?w, _header::192, msg::binary>>, state) do
    {:ok, decoded} = decode_logical(msg)

    state =
      case decoded do
        %{__struct__: Sequin.Runtime.PostgresAdapter.Decoder.Messages.Begin, final_lsn: {file, offset}} ->
          %{state | commit_lsn: (file <<< 32) + offset, commit_idx: 0}

        %{__struct__: Sequin.Runtime.PostgresAdapter.Decoder.Messages.Commit} ->
          %{state | commit_idx: state.commit_idx + 1}

        _ ->
          state
      end

    {{:message, state.commit_lsn, state.commit_idx, decoded}, state}
  end

  defp decode_message(other, state) do
    {{:unknown, other}, state}
  end

  defp decode_logical(msg) do
    {:ok, Sequin.Runtime.PostgresAdapter.Decoder.decode_message(msg)}
  end

  defp ack_message(lsn) when is_integer(lsn) do
    [<<?r, lsn::64, lsn::64, lsn::64, current_time()::64, 0>>]
  end

  @epoch DateTime.to_unix(~U[2000-01-01 00:00:00Z], :microsecond)
  defp current_time, do: System.os_time(:microsecond) - @epoch
end

defmodule Sequin.Libcluster.PostgresStrategy do
  @moduledoc """
  A libcluster strategy that uses Postgres LISTEN/NOTIFY to determine the cluster topology.

  Adopted from https://github.com/supabase/libcluster_postgres, but uses Ecto connection.

  This strategy uses the existing Sequin.Repo connection rather than creating its own.

  When a node comes online, it begins to broadcast its name in a "heartbeat" message to the channel. All other nodes that receive this message attempt to connect to it.

  This strategy does not check connectivity between nodes and does not disconnect them

  ## Options

  * `heartbeat_interval` - The interval at which to send heartbeat messages in milliseconds (optional; default: 5_000)
  * `channel_name` - The name of the channel to which nodes will listen and notify (optional; defaults to the result of `Node.get_cookie/0`)
  """
  use GenServer

  alias Cluster.Logger
  alias Cluster.Strategy
  alias Sequin.Repo

  def start_link(args), do: GenServer.start_link(__MODULE__, args)

  @spec init([%{:config => any(), :meta => any(), optional(any()) => any()}, ...]) ::
          {:ok, %{:config => list(), :meta => map(), optional(any()) => any()}, {:continue, :connect}}
  def init([state]) do
    channel_name = Keyword.get(state.config, :channel_name, clean_cookie(Node.get_cookie()))

    config =
      state.config
      |> Keyword.put_new(:channel_name, channel_name)
      |> Keyword.put_new(:heartbeat_interval, 5_000)

    meta = %{
      conn_notif: nil,
      heartbeat_ref: make_ref()
    }

    {:ok, %{state | config: config, meta: meta}, {:continue, :connect}}
  end

  def handle_continue(:connect, state) do
    with {:ok, conn_notif} <- Postgrex.Notifications.start_link(config()),
         {_, _} <- Postgrex.Notifications.listen(conn_notif, state.config[:channel_name]) do
      Logger.info(state.topology, "Listening for notifications on Postgres database")

      meta = %{
        state.meta
        | conn_notif: conn_notif,
          heartbeat_ref: heartbeat(0)
      }

      {:noreply, put_in(state.meta, meta)}
    else
      reason ->
        Logger.error(state.topology, "Failed to start listening for Postgres notifications: #{inspect(reason)}")
        {:noreply, state}
    end
  end

  def handle_info(:heartbeat, state) do
    Process.cancel_timer(state.meta.heartbeat_ref)
    Repo.query("notify #{state.config[:channel_name]}, '#{node()}'", [])
    ref = heartbeat(state.config[:heartbeat_interval])
    {:noreply, put_in(state.meta.heartbeat_ref, ref)}
  end

  def handle_info({:notification, _, _, _, node}, state) do
    node = String.to_atom(node)

    if node != node() do
      topology = state.topology
      Logger.debug(topology, "Trying to connect to node: #{node}")

      case Strategy.connect_nodes(topology, state.connect, state.list_nodes, [node]) do
        :ok -> Logger.debug(topology, "Connected to node: #{node}")
        {:error, _} -> Logger.error(topology, "Failed to connect to node: #{node}")
      end
    end

    {:noreply, state}
  end

  def handle_info(msg, state) do
    Logger.error(state.topology, "Undefined message #{inspect(msg, pretty: true)}")
    {:noreply, state}
  end

  ### Internal functions
  @spec heartbeat(non_neg_integer()) :: reference()
  defp heartbeat(interval) when interval >= 0 do
    Process.send_after(self(), :heartbeat, interval)
  end

  # Replaces all non alphanumeric values into underescore
  defp clean_cookie(cookie) when is_atom(cookie), do: cookie |> Atom.to_string() |> clean_cookie()

  defp clean_cookie(str) when is_binary(str) do
    String.replace(str, ~r/\W/, "_")
  end

  defp config do
    Keyword.take(Repo.config(), [
      :hostname,
      :endpoints,
      :socket_dir,
      :socket,
      :port,
      :database,
      :username,
      :password,
      :parameters,
      :timeout,
      :connect_timeout,
      :handshake_timeout,
      :ping_timeout,
      :ssl,
      :socket_options,
      :prepare,
      :transactions,
      :types,
      :search_path,
      :disconnect_on_error_codes,
      :pool_size,
      :idle_interval,
      :queue_target,
      :queue_interval,
      :ownership_timeout,
      :show_sensitive_data_on_connection_error
    ])
  end
end

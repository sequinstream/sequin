defmodule Sequin.Postgres.PostgrexBackend do
  @moduledoc """
  Real Postgres backend using Postgrex.Protocol.

  This is the default backend used by SlotProducer for actual
  Postgres replication connections.
  """

  @behaviour Sequin.Postgres.Backend

  @impl true
  defdelegate connect(opts), to: Postgrex.Protocol

  @impl true
  defdelegate handle_streaming(query, state), to: Postgrex.Protocol

  @impl true
  defdelegate checkin(state), to: Postgrex.Protocol

  @impl true
  defdelegate handle_copy_recv(msg, max, state), to: Postgrex.Protocol

  @impl true
  defdelegate handle_copy_send(msg, state), to: Postgrex.Protocol

  @impl true
  defdelegate handle_simple(query, params, state), to: Postgrex.Protocol

  @impl true
  defdelegate disconnect(reason, state), to: Postgrex.Protocol
end

defmodule Sequin.Postgres.FakegresBackend do
  @moduledoc """
  Fake Postgres backend for benchmarking.

  Delegates to a Fakegres GenServer that generates WAL messages
  and tracks acks for verification.

  TODO: Implement when building benchmark harness.
  """

  @behaviour Sequin.Postgres.Backend

  @impl true
  def connect(_opts), do: raise("Not implemented")

  @impl true
  def handle_streaming(_query, _state), do: raise("Not implemented")

  @impl true
  def checkin(_state), do: raise("Not implemented")

  @impl true
  def handle_copy_recv(_msg, _max, _state), do: raise("Not implemented")

  @impl true
  def handle_copy_send(_msg, _state), do: raise("Not implemented")

  @impl true
  def handle_simple(_query, _params, _state), do: raise("Not implemented")

  @impl true
  def disconnect(_reason, _state), do: raise("Not implemented")
end

defmodule Sequin.Test.Support.ReplicationSlots do
  @moduledoc """
  The Ecto sandbox interferes with replication slots. Even running replication slot tests in
  async: false mode doesn't solve it. Neither does setting up a different logical database for
  replication slot tests -- it appears creating a replication slot requires a lock that affects
  the whole db instance!

  Replication slot tests can carefully create the replication slot before touching the
  database/Ecto, but it's a footgun.

  To navigate around this, we reset and create the slot once before all tests here.
  """
  alias Sequin.Repo

  @doc """
  To add a replication slot used in a test, you must register it here.
  """
  def replication_slots do
    %{
      Sequin.PostgresReplicationTest => "__postgres_replication_test_slot__",
      SequinWeb.PostgresReplicationControllerTest => "__postgres_rep_controller_test_slot__",
      SequinWeb.DatabaseControllerTest => "__database_controller_test_slot__"
    }
  end

  def slot_name(mod), do: Map.fetch!(replication_slots(), mod)

  @doc """
  Run this before ExUnit.start/0. Because replication slots and sandboxes don't play nicely, we
  want to create the slots just once before we run the test suite.
  """
  def setup_all do
    replication_slots()
    |> Map.values()
    |> Enum.each(fn slot_name ->
      case Repo.query("select pg_drop_replication_slot($1)", [slot_name]) do
        {:ok, _} -> :ok
        {:error, %Postgrex.Error{postgres: %{code: :undefined_object}}} -> :ok
      end

      Repo.query!("select pg_create_logical_replication_slot($1, 'pgoutput')::text", [slot_name])
    end)
  end
end

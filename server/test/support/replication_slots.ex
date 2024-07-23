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
      case Repo.query("SELECT pg_drop_replication_slot($1)", [slot_name]) do
        {:ok, _} -> :ok
        {:error, %Postgrex.Error{postgres: %{code: :undefined_object}}} -> :ok
      end

      Repo.query!("SELECT pg_create_logical_replication_slot($1, 'pgoutput')::text", [slot_name])
    end)
  end

  @doc """
  For any test that needs to use a replication slot, call this function in the `setup` callback.

  This will ensure the "source" database, tables, and publication are in a pristine state.

  IMPORTANT: The order of these operations is important. Re-order at your own risk.
  """
  def setup_each(schema, tables, publication, replication_slot, create_table_ddls) do
    {:ok, conn} = Postgrex.start_link(config())

    # Create schema and tables
    query!(conn, "drop schema if exists #{schema} cascade")
    query!(conn, "create schema if not exists #{schema}")

    Enum.each(create_table_ddls, fn ddl ->
      query!(conn, ddl)
    end)

    # Need to cast the result of the function to `text`, as Postgrex does not support the pg_lsn type
    {:ok, %Postgrex.Result{rows: [[current_lsn]]}} = query(conn, "SELECT pg_current_wal_lsn()::text", [])

    # See above -- we need to cast `current_lsn` to `pg_lsn` over in the database due to Postgrex limitation
    {:ok, %Postgrex.Result{}} =
      query(
        conn,
        "SELECT pg_replication_slot_advance($1, '#{current_lsn}'::pg_lsn)::text",
        [replication_slot]
      )

    # drop and recreate publication
    query(conn, "drop publication if exists #{publication}")

    tables_string = Enum.map_join(tables, ", ", fn table -> "#{schema}.#{table}" end)
    query!(conn, "create publication #{publication} for table #{tables_string}")

    # set replica identity to full for all tables. This means we'll get `old` rows with changes.
    # Enum.each(tables, fn table ->
    #   query!(conn, "alter table #{schema}.#{table} replica identity full")
    # end)

    ExUnit.Callbacks.on_exit(fn ->
      # Original `conn` will be dead
      {:ok, conn} = Postgrex.start_link(config())
      # cleanup after all tests
      query(conn, "drop schema if exists #{schema} cascade")
      query(conn, "drop publication if exists #{publication}")
    end)
  end

  defp config do
    :sequin
    |> Application.get_env(Sequin.Repo)
    |> Keyword.take([:username, :password, :hostname, :database, :port])
  end

  defp query!(conn, query, params \\ [], opts \\ []) do
    case query(conn, query, params, opts) do
      {:ok, res} -> res
      error -> raise "Unexpected Postgrex response: #{inspect(error)}"
    end
  end

  defp query(conn, query, params \\ [], opts \\ []) do
    {ignore_error_code, opts} = Keyword.pop(opts, :ignore_error_code, [])

    case Postgrex.query(conn, query, params, opts) do
      {:ok, res} -> {:ok, res}
      {:error, %{postgres: %{code: ^ignore_error_code}} = res} -> {:ok, res}
      error -> error
    end
  end

  def reset_replication_slots(conn) do
  after
    GenServer.stop(conn)
  end
end

defmodule Sequin.Extensions.ReplicationTest do
  use Sequin.DataCase, async: true

  alias Sequin.Extensions.CreateReplicationSlot
  alias Sequin.Extensions.PostgresAdapter.Changes.DeletedRecord
  alias Sequin.Extensions.PostgresAdapter.Changes.NewRecord
  alias Sequin.Extensions.PostgresAdapter.Changes.UpdatedRecord
  alias Sequin.Extensions.Replication
  alias Sequin.Mocks.Extensions.ReplicationMessageHandlerMock
  alias Sequin.Repo

  @test_schema "__test_cdc__"
  @test_table "test_table"
  @publication "test_publication"
  @slot_name "pg_cdc_test"

  setup_all do
    # These tests need to happen outside of the sandbox
    {:ok, conn} = Postgrex.start_link(config())

    # Create schema and table
    query!(conn, "CREATE SCHEMA IF NOT EXISTS #{@test_schema}")

    query!(conn, """
    CREATE TABLE IF NOT EXISTS #{@test_schema}.#{@test_table} (
      id serial primary key,
      first_name text,
      last_name text
    )
    """)

    # Set replica identity to full
    query!(conn, "ALTER TABLE #{@test_schema}.#{@test_table} REPLICA IDENTITY FULL")

    on_exit(fn ->
      # Cleanup after all tests
      query(conn, "DROP SCHEMA IF EXISTS #{@test_schema} CASCADE")
      query(conn, "SELECT pg_drop_replication_slot('#{@slot_name}')", [], ignore_error_code: :undefined_object)
      query(conn, "DROP PUBLICATION IF EXISTS #{@publication}")
    end)

    [conn: conn]
  end

  setup %{conn: conn} do
    # Reset environment before each test
    reset_replication_environment(conn)

    :ok
  end

  @server_id __MODULE__
  @server_via Replication.via_tuple(@server_id)

  describe "creating a replication slot" do
    test "creates a replication slot if one doesn't exist", %{conn: conn} do
      test_pid = self()

      stub(ReplicationMessageHandlerMock, :handle_message, fn _ctx, msg ->
        send(test_pid, {:change, msg})
      end)

      start_replication!(message_handler: ReplicationMessageHandlerMock)

      assert_receive {Replication, :slot_created}

      query!(conn, "INSERT INTO #{@test_schema}.#{@test_table} (first_name, last_name) VALUES ('Paul', 'Atreides')")

      assert_receive {:change, _change}, :timer.seconds(1)
    end
  end

  describe "handling changes from replication slot" do
    setup do
      create_replication_slot(config(), @slot_name)
      :ok
    end

    test "changes are buffered, even if the listener is not up", %{conn: conn} do
      query!(conn, "INSERT INTO #{@test_schema}.#{@test_table} (first_name, last_name) VALUES ('Paul', 'Atreides')")

      test_pid = self()

      stub(ReplicationMessageHandlerMock, :handle_message, fn _ctx, msg ->
        send(test_pid, {:change, msg})
      end)

      start_replication!(message_handler: ReplicationMessageHandlerMock)

      assert_receive {:change, change}, :timer.seconds(1)

      assert is_struct(change, NewRecord)

      assert Map.equal?(change.record, %{
               "id" => "1",
               "first_name" => "Paul",
               "last_name" => "Atreides"
             })

      assert change.table == @test_table
      assert change.schema == @test_schema
    end

    @tag capture_log: true
    test "changes are delivered at least once", %{conn: conn} do
      test_pid = self()

      # simulate a message mis-handle/crash
      stub(ReplicationMessageHandlerMock, :handle_message, fn _ctx, msg ->
        send(test_pid, {:change, msg})
        raise "Simulated crash"
      end)

      start_replication!(message_handler: ReplicationMessageHandlerMock)

      query!(conn, "INSERT INTO #{@test_schema}.#{@test_table} (first_name, last_name) VALUES ('Paul', 'Atreides')")

      assert_receive {:change, _}, :timer.seconds(1)

      stop_replication!()

      stub(ReplicationMessageHandlerMock, :handle_message, fn _ctx, msg ->
        send(test_pid, {:change, msg})
      end)

      start_replication!(message_handler: ReplicationMessageHandlerMock)

      assert_receive {:change, change}, :timer.seconds(1)
      assert is_struct(change, NewRecord)

      # Should have received the record (it was re-delivered)
      assert Map.equal?(change.record, %{
               "id" => "1",
               "first_name" => "Paul",
               "last_name" => "Atreides"
             })
    end

    test "creates, updates, and deletes are captured", %{conn: conn} do
      test_pid = self()

      stub(ReplicationMessageHandlerMock, :handle_message, fn _ctx, msg ->
        send(test_pid, {:change, msg})
      end)

      start_replication!(message_handler: ReplicationMessageHandlerMock)

      # Test create
      query!(conn, "INSERT INTO #{@test_schema}.#{@test_table} (first_name, last_name) VALUES ('Paul', 'Atreidez')")

      assert_receive {:change, create_change}, :timer.seconds(1)
      assert is_struct(create_change, NewRecord)

      assert Map.equal?(create_change.record, %{
               "id" => "1",
               "first_name" => "Paul",
               "last_name" => "Atreidez"
             })

      assert create_change.type == "insert"

      # Test update
      query!(conn, "UPDATE #{@test_schema}.#{@test_table} SET last_name = 'Muad''Dib' WHERE id = 1")

      assert_receive {:change, update_change}, :timer.seconds(1)
      assert is_struct(update_change, UpdatedRecord)

      assert Map.equal?(update_change.record, %{
               "id" => "1",
               "first_name" => "Paul",
               "last_name" => "Muad'Dib"
             })

      assert Map.equal?(update_change.old_record, %{
               "id" => "1",
               "first_name" => "Paul",
               "last_name" => "Atreidez"
             })

      assert update_change.type == "update"

      # Test delete
      query!(conn, "DELETE FROM #{@test_schema}.#{@test_table} WHERE id = 1")

      assert_receive {:change, delete_change}, :timer.seconds(1)
      assert is_struct(delete_change, DeletedRecord)

      assert Map.equal?(delete_change.old_record, %{
               "id" => "1",
               "first_name" => "Paul",
               "last_name" => "Muad'Dib"
             })

      assert delete_change.type == "delete"
    end
  end

  # TODO: Test what happens in the integration test when replication identity is off

  # describe "changes propagating to a stream" do
  # setup - an actual replication object. Boot the replication supervisor?
  # - creates are inserted
  # - updates are upserted
  # - deletes propagate as upserts
  # end

  # Helper functions

  defp reset_replication_environment(conn) do
    # Drop and recreate replication slot
    query(conn, "SELECT pg_drop_replication_slot('#{@slot_name}')", [], ignore_error_code: :undefined_object)

    # Drop and recreate publication
    query(conn, "DROP PUBLICATION IF EXISTS #{@publication}")
    query!(conn, "CREATE PUBLICATION #{@publication} FOR TABLE #{@test_schema}.#{@test_table}")

    # Truncate the test table
    query!(conn, "TRUNCATE TABLE #{@test_schema}.#{@test_table} RESTART IDENTITY CASCADE")
  end

  defp create_replication_slot(conn_opts, slot_name) do
    test_pid = self()

    on_finish = fn ->
      send(test_pid, :done)
    end

    args = [slot_name: slot_name, on_finish: on_finish]
    opts = args ++ conn_opts
    # Will die after this
    start_supervised!({CreateReplicationSlot, opts})

    receive do
      :done -> :ok
    end
  end

  defp start_replication!(opts) do
    opts =
      Keyword.merge(
        [
          publication: @publication,
          connection: Repo.config(),
          slot_name: @slot_name,
          test_pid: self(),
          id: @server_id,
          message_handler: ReplicationMessageHandlerMock,
          message_handler_ctx: nil
        ],
        opts
      )

    start_supervised!(Replication.child_spec(opts))
  end

  defp stop_replication! do
    stop_supervised!(@server_via)
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

  defp config do
    :sequin
    |> Application.get_env(Sequin.Repo)
    |> Keyword.take([:username, :password, :hostname, :database, :port])
  end
end

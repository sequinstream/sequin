defmodule Sequin.Extensions.ReplicationTest do
  use Sequin.DataCase, async: true

  alias Sequin.Extensions.CreateReplicationSlot
  alias Sequin.Extensions.PostgresAdapter.Changes.DeletedRecord
  alias Sequin.Extensions.PostgresAdapter.Changes.NewRecord
  alias Sequin.Extensions.PostgresAdapter.Changes.UpdatedRecord
  alias Sequin.Extensions.Replication
  alias Sequin.Repo

  @test_schema "__test_cdc__"
  @test_table "test_table"
  @publication "test_publication"
  @slot_name "pg_cdc_test"

  setup_all do
    # These tests need to happen outside of the sandbox
    {:ok, conn} = Postgrex.start_link(config())

    query(conn, """
    select pg_drop_replication_slot('#{@slot_name}');
    """)

    query!(conn, """
    create schema if not exists #{@test_schema};
    """)

    query!(conn, """
    drop table if exists #{@test_schema}.#{@test_table};
    """)

    query!(conn, """
    create table #{@test_schema}.#{@test_table} (
      id serial primary key,
      first_name text,
      last_name text
    );
    """)

    # this gives us old row tuples for updates and deletes - users need to opt in to this
    query!(conn, """
    alter table #{@test_schema}.#{@test_table} replica identity full;
    """)

    query!(conn, """
    SELECT relreplident FROM pg_class WHERE relname = '#{@test_table}';
    """)

    query!(
      conn,
      """
      CREATE PUBLICATION #{@publication} FOR TABLE #{@test_schema}.#{@test_table};
      """,
      [],
      ignore_error_code: :duplicate_object
    )

    on_exit(fn ->
      query(conn, """
      drop publication if exists #{@publication};
      """)
    end)

    [conn: conn]
  end

  @server_id __MODULE__
  @server_via Replication.via_tuple(@server_id)
  describe "creating a replication slot" do
    test "creates a replication slot if one doesn't exist", %{conn: conn} do
      test_pid = self()
      start_replication!(handle_message_fun: fn msg -> send(test_pid, {:change, msg}) end)

      assert_receive {Replication, :slot_created}

      query!(conn, "insert into #{@test_schema}.#{@test_table} (first_name, last_name) values ('Paul', 'Atreides')")

      assert_receive {:change, _change}, :timer.seconds(1)
    end
  end

  describe "handling changes from replication slot" do
    setup do
      create_replication_slot(config(), @slot_name)
    end

    test "changes are buffered, even if the listener is not up", %{conn: conn} do
      query!(conn, "insert into #{@test_schema}.#{@test_table} (first_name, last_name) values ('Paul', 'Atreides')")

      test_pid = self()
      start_replication!(handle_message_fun: fn msg -> send(test_pid, {:change, msg}) end)

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
      start_replication!(
        handle_message_fun: fn msg ->
          send(test_pid, {:change, msg})
          raise "Simulated crash"
        end
      )

      query!(conn, "insert into #{@test_schema}.#{@test_table} (first_name, last_name) values ('Paul', 'Atreides')")

      assert_receive {:change, _}, :timer.seconds(1)

      stop_replication!()

      start_replication!(handle_message_fun: fn msg -> send(test_pid, {:change, msg}) end)

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
      start_replication!(handle_message_fun: fn msg -> send(test_pid, {:change, msg}) end)

      # Test create
      query!(conn, "insert into #{@test_schema}.#{@test_table} (first_name, last_name) values ('Paul', 'Atreides')")

      assert_receive {:change, create_change}, :timer.seconds(1)
      assert is_struct(create_change, NewRecord), "Expected NewRecord, got #{inspect(create_change)}"

      assert Map.equal?(create_change.record, %{
               "id" => "1",
               "first_name" => "Paul",
               "last_name" => "Atreides"
             })

      assert create_change.type == "insert"

      # Test update
      query!(conn, "update #{@test_schema}.#{@test_table} set last_name = 'Muad''Dib' where id = 1")

      assert_receive {:change, update_change}, :timer.seconds(1)
      assert is_struct(update_change, UpdatedRecord), "Expected UpdatedRecord, got #{inspect(update_change)}"

      assert Map.equal?(update_change.record, %{
               "id" => "1",
               "first_name" => "Paul",
               "last_name" => "Muad'Dib"
             })

      assert Map.equal?(update_change.old_record, %{
               "id" => "1",
               "first_name" => "Paul",
               "last_name" => "Atreides"
             })

      assert update_change.type == "update"

      # Test delete
      query!(conn, "delete from #{@test_schema}.#{@test_table} where id = 1")

      assert_receive {:change, delete_change}, :timer.seconds(1)
      assert is_struct(delete_change, DeletedRecord), "Expected DeletedRecord, got #{inspect(delete_change)}"

      assert Map.equal?(delete_change.old_record, %{
               "id" => "1",
               "first_name" => "Paul",
               "last_name" => "Muad'Dib"
             })

      assert delete_change.type == "delete"
    end
  end

  # TODO: Test what happens in the integration test when replication identity is off

  # The best way to create a replication slot with Postgrex seems to be to start this GenServer
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
        [publication: @publication, connection: Repo.config(), slot_name: @slot_name, test_pid: self(), id: @server_id],
        opts
      )

    start_supervised!(Replication.child_spec(opts))
  end

  defp stop_replication! do
    stop_supervised!(@server_via)
  end

  # describe "changes propagating to a stream" do
  # setup - an actual replication object. Boot the replication supervisor?
  # - creates are inserted
  # - updates are upserted
  # - deletes propagate as upserts
  # end

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
      {:error, %{postgres: %{code: ^ignore_error_code}}} -> :ok
      error -> error
    end
  end

  defp config do
    :sequin
    |> Application.get_env(Sequin.Repo)
    |> Keyword.take([:username, :password, :hostname, :database, :port])
  end
end

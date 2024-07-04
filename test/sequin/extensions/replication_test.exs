defmodule Sequin.Extensions.ReplicationTest do
  use Sequin.DataCase, async: true

  alias Sequin.Extensions.PostgresAdapter.Changes.DeletedRecord
  alias Sequin.Extensions.PostgresAdapter.Changes.NewRecord
  alias Sequin.Extensions.PostgresAdapter.Changes.UpdatedRecord
  alias Sequin.Extensions.Replication
  alias Sequin.Mocks.Extensions.ReplicationMessageHandlerMock
  alias Sequin.Test.Support.ReplicationSlots

  @moduletag :replication

  @test_schema "__replication_test_schema__"
  @test_table "__replication_test_table__"
  @publication "__replication_test_publication__"

  def replication_slot, do: ReplicationSlots.slot_name(__MODULE__)

  setup do
    # These queries need to happen outside of the sandbox
    {:ok, conn} = Postgrex.start_link(config())

    create_table_ddl = """
    create table if not exists #{@test_schema}.#{@test_table} (
      id serial primary key,
      first_name text,
      last_name text
    )
    """

    ReplicationSlots.setup_each(@test_schema, [@test_table], @publication, replication_slot(), [create_table_ddl])

    [conn: conn]
  end

  @server_id __MODULE__
  @server_via Replication.via_tuple(@server_id)

  describe "handling changes from replication slot" do
    test "changes are buffered, even if the listener is not up", %{conn: conn} do
      query!(conn, "INSERT INTO #{@test_schema}.#{@test_table} (first_name, last_name) VALUES ('Paul', 'Atreides')")

      test_pid = self()

      stub(ReplicationMessageHandlerMock, :handle_message, fn _ctx, msg ->
        send(test_pid, {:change, msg})
      end)

      start_replication!(message_handler: ReplicationMessageHandlerMock)
      assert_receive {:change, change}, :timer.seconds(5)

      assert is_struct(change, NewRecord), "Expected change to be a NewRecord, got: #{inspect(change)}"

      assert Map.equal?(change.record, %{
               "id" => 1,
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
               "id" => 1,
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
               "id" => 1,
               "first_name" => "Paul",
               "last_name" => "Atreidez"
             })

      assert create_change.type == "insert"

      # Test update
      query!(conn, "UPDATE #{@test_schema}.#{@test_table} SET last_name = 'Muad''Dib' WHERE id = 1")

      assert_receive {:change, update_change}, :timer.seconds(1)
      assert is_struct(update_change, UpdatedRecord)

      assert Map.equal?(update_change.record, %{
               "id" => 1,
               "first_name" => "Paul",
               "last_name" => "Muad'Dib"
             })

      assert Map.equal?(update_change.old_record, %{
               "id" => 1,
               "first_name" => "Paul",
               "last_name" => "Atreidez"
             })

      assert update_change.type == "update"

      # Test delete
      query!(conn, "DELETE FROM #{@test_schema}.#{@test_table} WHERE id = 1")

      assert_receive {:change, delete_change}, :timer.seconds(1)
      assert is_struct(delete_change, DeletedRecord)

      assert Map.equal?(delete_change.old_record, %{
               "id" => 1,
               "first_name" => "Paul",
               "last_name" => "Muad'Dib"
             })

      assert delete_change.type == "delete"
    end
  end

  defp start_replication!(opts) do
    opts =
      Keyword.merge(
        [
          publication: @publication,
          connection: config(),
          slot_name: replication_slot(),
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
    Postgrex.query!(conn, query, params, opts)
  end

  defp config do
    :sequin
    |> Application.get_env(Sequin.Repo)
    |> Keyword.take([:username, :password, :hostname, :database, :port])
  end
end

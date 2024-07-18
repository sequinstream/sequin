defmodule Sequin.PostgresReplicationTest do
  @moduledoc """
  This test file contains both unit tests for the Replication extension as well as integration
  tests with PostgresReplication. The reason for combining the two is interference: when the two
  test suites are run side-by-side, they interfere with each other. We didn't get to the bottom of
  the interference (issues starting the replication connection?), but it definitely seems to occur
  when the two tests are running and connecting to slots at the same time.

  To keep them async, we're just co-locating them in this one test file. That ensures they run
  serially, avoiding interference, while remaining concurrent with the rest of the test suite.
  """
  use Sequin.DataCase, async: true

  alias Sequin.Extensions.PostgresAdapter.Changes.DeletedRecord
  alias Sequin.Extensions.PostgresAdapter.Changes.NewRecord
  alias Sequin.Extensions.PostgresAdapter.Changes.UpdatedRecord
  alias Sequin.Extensions.Replication
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.SourcesFactory
  alias Sequin.Factory.StreamsFactory
  alias Sequin.Mocks.Extensions.ReplicationMessageHandlerMock
  alias Sequin.Sources
  alias Sequin.Sources.PostgresReplication
  alias Sequin.SourcesRuntime
  alias Sequin.Streams
  alias Sequin.Test.Support.ReplicationSlots

  @test_schema "__postgres_replication_test_schema__"
  @test_table "__postgres_replication_test_table__"
  @test_table_2pk "__postgres_replication_test_table_2pk__"
  @publication "__postgres_replication_test_publication__"

  def replication_slot, do: ReplicationSlots.slot_name(__MODULE__)

  setup do
    # This setup needs to happen outside of the sandbox
    {:ok, conn} = Postgrex.start_link(config())

    create_table_ddls = [
      """
      create table if not exists #{@test_schema}.#{@test_table} (
        id serial primary key,
        name text,
        house text,
        planet text
      )
      """,
      """
      create table if not exists #{@test_schema}.#{@test_table_2pk} (
        id1 serial,
        id2 serial,
        name text,
        house text,
        planet text,
        primary key (id1, id2)
      )
      """
    ]

    ReplicationSlots.setup_each(
      @test_schema,
      [@test_table, @test_table_2pk],
      @publication,
      replication_slot(),
      create_table_ddls
    )

    %{conn: conn}
  end

  describe "PostgresReplication end-to-end" do
    setup do
      # Create source database
      account_id = AccountsFactory.insert_account!().id
      source_db = DatabasesFactory.insert_configured_postgres_database!(account_id: account_id)
      stream = StreamsFactory.insert_stream!(account_id: account_id)

      # Create PostgresReplication entity
      pg_replication =
        SourcesFactory.insert_postgres_replication!(
          status: :active,
          postgres_database_id: source_db.id,
          stream_id: stream.id,
          slot_name: replication_slot(),
          publication_name: @publication,
          account_id: stream.account_id,
          key_format: :basic
        )

      # Start replication
      sup = Module.concat(__MODULE__, SourcesRuntime.Supervisor)
      start_supervised!(Sequin.DynamicSupervisor.child_spec(name: sup))

      {:ok, _pid} =
        SourcesRuntime.Supervisor.start_for_pg_replication(sup, pg_replication, test_pid: self())

      %{stream: stream, pg_replication: pg_replication, source_db: source_db, sup: sup}
    end

    test "inserts are replicated to the stream", %{conn: conn, stream: stream} do
      query!(
        conn,
        "INSERT INTO #{@test_schema}.#{@test_table} (name, house, planet) VALUES ('Paul Atreides', 'Atreides', 'Arrakis')"
      )

      assert_receive {Replication, :message_handled}, 500

      [message] = Streams.list_messages_for_stream(stream.id)
      assert message.subject =~ "#{@test_schema}.#{@test_table}"

      decoded_data = Jason.decode!(message.data)
      assert decoded_data["data"] == %{"id" => 1, "name" => "Paul Atreides", "house" => "Atreides", "planet" => "Arrakis"}
      refute decoded_data["deleted"]
    end

    test "updates are replicated to the stream", %{conn: conn, stream: stream} do
      query!(
        conn,
        "INSERT INTO #{@test_schema}.#{@test_table} (name, house, planet) VALUES ('Leto Atreides', 'Atreides', 'Caladan')"
      )

      assert_receive {Replication, :message_handled}, 1_000

      query!(conn, "UPDATE #{@test_schema}.#{@test_table} SET planet = 'Arrakis' WHERE id = 1")
      assert_receive {Replication, :message_handled}, 1_000

      [message] = Streams.list_messages_for_stream(stream.id)
      assert message.subject =~ "#{@test_schema}.#{@test_table}"

      decoded_data = Jason.decode!(message.data)
      assert decoded_data["data"] == %{"id" => 1, "name" => "Leto Atreides", "house" => "Atreides", "planet" => "Arrakis"}
      refute decoded_data["deleted"]
    end

    test "deletes are replicated to the stream", %{conn: conn, stream: stream} do
      query!(
        conn,
        "INSERT INTO #{@test_schema}.#{@test_table} (name, house, planet) VALUES ('Duncan Idaho', 'Atreides', 'Caladan')"
      )

      assert_receive {Replication, :message_handled}, 1_000

      query!(conn, "DELETE FROM #{@test_schema}.#{@test_table} WHERE id = 1")
      assert_receive {Replication, :message_handled}, 1_000

      [message] = Streams.list_messages_for_stream(stream.id)
      assert message.subject =~ "#{@test_schema}.#{@test_table}"

      decoded_data = Jason.decode!(message.data)
      assert decoded_data["data"] == %{"id" => 1, "name" => "Duncan Idaho", "house" => "Atreides", "planet" => "Caladan"}
      assert decoded_data["deleted"]
    end

    test "replication with default replica identity", %{conn: conn, stream: stream} do
      # Set replica identity to default
      query!(conn, "ALTER TABLE #{@test_schema}.#{@test_table} REPLICA IDENTITY DEFAULT")

      query!(
        conn,
        "INSERT INTO #{@test_schema}.#{@test_table} (name, house, planet) VALUES ('Chani', 'Fremen', 'Arrakis')"
      )

      assert_receive {Replication, :message_handled}, 1_000

      [insert_message] = Streams.list_messages_for_stream(stream.id)
      assert insert_message.subject =~ "#{@test_schema}.#{@test_table}"
      decoded_insert_data = Jason.decode!(insert_message.data)
      assert decoded_insert_data["data"] == %{"id" => 1, "name" => "Chani", "house" => "Fremen", "planet" => "Arrakis"}
      refute decoded_insert_data["deleted"]

      query!(conn, "UPDATE #{@test_schema}.#{@test_table} SET house = 'Atreides' WHERE id = 1")
      assert_receive {Replication, :message_handled}, 1_000

      [update_message] = Streams.list_messages_for_stream(stream.id)
      assert update_message.seq > insert_message.seq
      assert update_message.subject == insert_message.subject
      assert DateTime.compare(update_message.inserted_at, insert_message.inserted_at) == :eq

      decoded_update_data = Jason.decode!(update_message.data)
      assert decoded_update_data["data"] == %{"id" => 1, "name" => "Chani", "house" => "Atreides", "planet" => "Arrakis"}
      refute decoded_update_data["deleted"]

      query!(conn, "DELETE FROM #{@test_schema}.#{@test_table} WHERE id = 1")
      assert_receive {Replication, :message_handled}, 1_000

      [delete_message] = Streams.list_messages_for_stream(stream.id)
      assert delete_message.seq > update_message.seq
      assert delete_message.subject == update_message.subject
      assert DateTime.compare(delete_message.inserted_at, update_message.inserted_at) == :eq

      decoded_delete_data = Jason.decode!(delete_message.data)
      assert decoded_delete_data["data"]["id"] == 1
      assert decoded_delete_data["deleted"]
    end

    test "replication with two primary key columns", %{conn: conn, stream: stream} do
      # Set replica identity to default - make sure even the delete comes through with both PKs
      query!(conn, "ALTER TABLE #{@test_schema}.#{@test_table} REPLICA IDENTITY DEFAULT")
      # Insert
      query!(
        conn,
        "INSERT INTO #{@test_schema}.#{@test_table_2pk} (id1, id2, name, house, planet) VALUES (1, 2, 'Paul Atreides', 'Atreides', 'Arrakis')"
      )

      assert_receive {Replication, :message_handled}, 1_000

      [insert_message] = Streams.list_messages_for_stream(stream.id)
      decoded_insert_data = Jason.decode!(insert_message.data)
      assert %{"id1" => 1, "id2" => 2} = decoded_insert_data["data"]
      refute decoded_insert_data["deleted"]

      # Delete
      query!(conn, "DELETE FROM #{@test_schema}.#{@test_table_2pk} WHERE id1 = 1 AND id2 = 2")
      assert_receive {Replication, :message_handled}, 1_000

      [delete_message] = Streams.list_messages_for_stream(stream.id)
      decoded_delete_data = Jason.decode!(delete_message.data)
      assert %{"id1" => 1, "id2" => 2} = decoded_delete_data["data"]
      assert decoded_delete_data["deleted"]
    end

    test "add_info returns correct information", %{conn: conn, pg_replication: pg_replication} do
      # Insert some data to ensure there's a last committed timestamp
      query!(
        conn,
        "INSERT INTO #{@test_schema}.#{@test_table} (name, house, planet) VALUES ('Paul Atreides', 'Atreides', 'Arrakis')"
      )

      assert_receive {Replication, :message_handled}, 1000

      # Call add_info
      pg_replication_with_info = Sources.add_info(pg_replication)

      # Assert that the info field is populated
      assert %PostgresReplication.Info{} = pg_replication_with_info.info

      # Check last_committed_at
      assert pg_replication_with_info.info.last_committed_at != nil
      assert DateTime.before?(pg_replication_with_info.info.last_committed_at, DateTime.utc_now())
    end

    test "replication with 'with_operation' key format", %{
      conn: conn,
      stream: stream,
      pg_replication: pg_replication,
      sup: sup
    } do
      {:ok, pg_replication} = Sources.update_pg_replication(pg_replication, %{key_format: :with_operation})

      # Restart the server
      SourcesRuntime.Supervisor.stop_for_pg_replication(sup, pg_replication)

      {:ok, _pid} = SourcesRuntime.Supervisor.start_for_pg_replication(sup, pg_replication, test_pid: self())

      query!(
        conn,
        "INSERT INTO #{@test_schema}.#{@test_table} (name, house, planet) VALUES ('Paul Atreides', 'Atreides', 'Arrakis')"
      )

      assert_receive {Replication, :message_handled}, 500

      [message] = Streams.list_messages_for_stream(stream.id)
      assert message.subject =~ "#{@test_schema}.#{@test_table}.insert"
    end
  end

  describe "create_pg_replication_for_account_with_lifecycle" do
    setup do
      account = AccountsFactory.insert_account!()
      database = DatabasesFactory.insert_configured_postgres_database!(account_id: account.id)
      stream = StreamsFactory.insert_stream!(account_id: account.id)

      %{account: account, database: database, stream: stream}
    end

    test "creates a pg_replication in backfilling state and enqueues backfill jobs", %{
      account: account,
      database: database,
      stream: stream
    } do
      attrs = %{
        postgres_database_id: database.id,
        stream_id: stream.id,
        slot_name: replication_slot(),
        publication_name: @publication,
        status: :backfilling
      }

      {:ok, pg_replication} = Sources.create_pg_replication_for_account_with_lifecycle(account.id, attrs)

      assert pg_replication.status == :backfilling
      assert is_nil(pg_replication.backfill_completed_at)

      # Check that backfill jobs are enqueued
      assert_enqueued(worker: Sequin.Sources.BackfillPostgresTableWorker)
    end

    test "fails to create pg_replication with invalid attributes", %{account: account} do
      invalid_attrs = %{
        postgres_database_id: Ecto.UUID.generate(),
        stream_id: Ecto.UUID.generate(),
        slot_name: "",
        publication_name: ""
      }

      assert {:error, _changeset} =
               Sources.create_pg_replication_for_account_with_lifecycle(account.id, invalid_attrs)
    end
  end

  @server_id __MODULE__
  @server_via Replication.via_tuple(@server_id)

  describe "replication in isolation" do
    test "changes are buffered, even if the listener is not up", %{conn: conn} do
      query!(
        conn,
        "INSERT INTO #{@test_schema}.#{@test_table} (name, house, planet) VALUES ('Paul Atreides', 'Atreides', 'Arrakis')"
      )

      test_pid = self()

      stub(ReplicationMessageHandlerMock, :handle_message, fn _ctx, msg ->
        send(test_pid, {:change, msg})
      end)

      start_replication!(message_handler_module: ReplicationMessageHandlerMock)
      assert_receive {:change, change}, :timer.seconds(5)

      assert is_struct(change, NewRecord), "Expected change to be a NewRecord, got: #{inspect(change)}"

      assert Map.equal?(change.record, %{
               "id" => 1,
               "name" => "Paul Atreides",
               "house" => "Atreides",
               "planet" => "Arrakis"
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

      start_replication!(message_handler_module: ReplicationMessageHandlerMock)

      query!(
        conn,
        "INSERT INTO #{@test_schema}.#{@test_table} (name, house, planet) VALUES ('Paul Atreides', 'Atreides', 'Arrakis')"
      )

      assert_receive {:change, _}, :timer.seconds(1)

      stop_replication!()

      stub(ReplicationMessageHandlerMock, :handle_message, fn _ctx, msg ->
        send(test_pid, {:change, msg})
      end)

      start_replication!(message_handler_module: ReplicationMessageHandlerMock)

      assert_receive {:change, change}, :timer.seconds(1)
      assert is_struct(change, NewRecord)

      # Should have received the record (it was re-delivered)
      assert Map.equal?(change.record, %{
               "id" => 1,
               "name" => "Paul Atreides",
               "house" => "Atreides",
               "planet" => "Arrakis"
             })
    end

    test "creates, updates, and deletes are captured", %{conn: conn} do
      test_pid = self()

      stub(ReplicationMessageHandlerMock, :handle_message, fn _ctx, msg ->
        send(test_pid, {:change, msg})
      end)

      start_replication!(message_handler_module: ReplicationMessageHandlerMock)

      # Test create
      query!(
        conn,
        "INSERT INTO #{@test_schema}.#{@test_table} (name, house, planet) VALUES ('Paul Atreides', 'Atreides', 'Caladan')"
      )

      assert_receive {:change, create_change}, :timer.seconds(1)
      assert is_struct(create_change, NewRecord)

      assert Map.equal?(create_change.record, %{
               "id" => 1,
               "name" => "Paul Atreides",
               "house" => "Atreides",
               "planet" => "Caladan"
             })

      assert create_change.type == "insert"

      # Test update
      query!(conn, "UPDATE #{@test_schema}.#{@test_table} SET planet = 'Arrakis' WHERE id = 1")

      assert_receive {:change, update_change}, :timer.seconds(1)
      assert is_struct(update_change, UpdatedRecord)

      assert Map.equal?(update_change.record, %{
               "id" => 1,
               "name" => "Paul Atreides",
               "house" => "Atreides",
               "planet" => "Arrakis"
             })

      assert Map.equal?(update_change.old_record, %{
               "id" => 1,
               "name" => "Paul Atreides",
               "house" => "Atreides",
               "planet" => "Caladan"
             })

      assert update_change.type == "update"

      # Test delete
      query!(conn, "DELETE FROM #{@test_schema}.#{@test_table} WHERE id = 1")

      assert_receive {:change, delete_change}, :timer.seconds(1)
      assert is_struct(delete_change, DeletedRecord)

      assert Map.equal?(delete_change.old_record, %{
               "id" => 1,
               "name" => "Paul Atreides",
               "house" => "Atreides",
               "planet" => "Arrakis"
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
          message_handler_module: ReplicationMessageHandlerMock,
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

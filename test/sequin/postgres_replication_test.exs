defmodule Sequin.PostgresReplicationTest do
  @moduledoc false
  use Sequin.DataCase, async: true

  alias Sequin.Extensions.Replication
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.SourcesFactory
  alias Sequin.Factory.StreamsFactory
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

    # Create source database
    account_id = AccountsFactory.insert_account!().id
    source_db = DatabasesFactory.insert_configured_postgres_database!(account_id: account_id)
    stream = StreamsFactory.insert_stream!(account_id: account_id)

    # Create PostgresReplication entity
    pg_replication =
      SourcesFactory.insert_postgres_replication!(
        postgres_database_id: source_db.id,
        stream_id: stream.id,
        slot_name: replication_slot(),
        publication_name: @publication,
        account_id: account_id
      )

    # Start replication
    sup = Module.concat(__MODULE__, SourcesRuntime.Supervisor)
    start_supervised!(Sequin.DynamicSupervisor.child_spec(name: sup))

    {:ok, _pid} =
      SourcesRuntime.Supervisor.start_for_pg_replication(sup, pg_replication, test_pid: self())

    %{conn: conn, stream: stream, pg_replication: pg_replication}
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

  defp query!(conn, query, params \\ [], opts \\ []) do
    Postgrex.query!(conn, query, params, opts)
  end

  defp config do
    :sequin
    |> Application.get_env(Sequin.Repo)
    |> Keyword.take([:username, :password, :hostname, :database, :port])
  end
end

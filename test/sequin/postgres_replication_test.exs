defmodule Sequin.PostgresReplicationTest do
  @moduledoc """
  This test file contains both unit tests for the Replication extension as well as integration
  tests with PostgresReplicationSlot. The reason for combining the two is interference: when the two
  test suites are run side-by-side, they interfere with each other. We didn't get to the bottom of
  the interference (issues starting the replication connection?), but it definitely seems to occur
  when the two tests are running and connecting to slots at the same time.

  To keep them async, we're just co-locating them in this one test file. That ensures they run
  serially, avoiding interference, while remaining concurrent with the rest of the test suite.
  """
  use Sequin.DataCase, async: true

  alias Sequin.Consumers
  alias Sequin.Extensions.Replication, as: ReplicationExt
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.CharacterFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Mocks.Extensions.MessageHandlerMock
  alias Sequin.Replication.Message
  # alias Sequin.Replication
  # alias Sequin.Replication.PostgresReplicationSlot
  alias Sequin.ReplicationRuntime
  alias Sequin.Test.Support.Models.Character
  alias Sequin.Test.Support.Models.Character2PK
  alias Sequin.Test.Support.Models.CharacterIdentFull
  alias Sequin.Test.Support.ReplicationSlots
  alias Sequin.Test.UnboxedRepo

  @moduletag :unboxed

  @publication "characters_publication"

  def replication_slot, do: ReplicationSlots.slot_name(__MODULE__)

  setup do
    # Fast-forward the replication slot to the current WAL position
    {:ok, _} =
      UnboxedRepo.query("SELECT pg_replication_slot_advance($1, pg_current_wal_lsn())::text", [replication_slot()])

    :ok
  end

  describe "PostgresReplicationSlot end-to-end" do
    setup do
      # Create source database
      account_id = AccountsFactory.insert_account!().id
      source_db = DatabasesFactory.insert_configured_postgres_database!(account_id: account_id)

      # Create PostgresReplicationSlot entity
      pg_replication =
        ReplicationFactory.insert_postgres_replication!(
          postgres_database_id: source_db.id,
          slot_name: replication_slot(),
          publication_name: @publication,
          account_id: account_id
        )

      # Create a consumer for this replication slot
      consumer =
        ConsumersFactory.insert_http_push_consumer!(
          message_kind: :event,
          replication_slot_id: pg_replication.id,
          account_id: account_id,
          source_tables: [
            ConsumersFactory.source_table(
              oid: Character.table_oid(),
              column_filters: []
            ),
            ConsumersFactory.source_table(
              oid: CharacterIdentFull.table_oid(),
              column_filters: []
            ),
            ConsumersFactory.source_table(
              oid: Character2PK.table_oid(),
              column_filters: []
            )
          ]
        )

      # Start replication
      sup = Module.concat(__MODULE__, ReplicationRuntime.Supervisor)
      start_supervised!(Sequin.DynamicSupervisor.child_spec(name: sup))

      {:ok, _pid} =
        ReplicationRuntime.Supervisor.start_for_pg_replication(sup, pg_replication, test_pid: self())

      %{pg_replication: pg_replication, source_db: source_db, sup: sup, consumer: consumer}
    end

    test "inserts are replicated to consumer events", %{consumer: consumer} do
      # Insert a character
      character = CharacterFactory.insert_character!()

      # Wait for the message to be handled
      assert_receive {ReplicationExt, :flush_messages}, 500

      # Fetch consumer events
      [consumer_event] = Consumers.list_consumer_events_for_consumer(consumer.id)

      # Assert the consumer event details
      assert consumer_event.consumer_id == consumer.id
      assert consumer_event.table_oid == Character.table_oid()
      assert consumer_event.record_pks == [to_string(character.id)]
      %{data: data} = consumer_event

      assert data.record == %{
               "id" => character.id,
               "name" => character.name,
               "house" => character.house,
               "planet" => character.planet,
               "is_active" => character.is_active,
               "tags" => character.tags
             }

      assert is_nil(data.changes)
      assert data.action == :insert
      assert_maps_equal(data.metadata, %{table: "characters", schema: "public"}, [:table, :schema])
      assert is_struct(data.metadata.commit_timestamp, DateTime)
    end

    test "updates are replicated to consumer events when replica identity default", %{consumer: consumer} do
      # Insert a character
      character = CharacterFactory.insert_character!(%{name: "Leto Atreides", house: "Atreides", planet: "Caladan"})

      # Wait for the message to be handled
      assert_receive {ReplicationExt, :flush_messages}, 500

      # Update the character
      UnboxedRepo.update!(Ecto.Changeset.change(character, planet: "Arrakis"))

      # Wait for the update message to be handled
      assert_receive {ReplicationExt, :flush_messages}, 500

      # Fetch consumer events
      [_insert_event, update_event] =
        Consumers.list_consumer_events_for_consumer(consumer.id, order_by: [asc: :inserted_at])

      # Assert the consumer event details
      assert update_event.consumer_id == consumer.id
      assert update_event.table_oid == Character.table_oid()
      assert update_event.record_pks == [to_string(character.id)]
      %{data: data} = update_event

      assert data.record == %{
               "id" => character.id,
               "name" => character.name,
               "house" => character.house,
               "planet" => "Arrakis",
               "is_active" => character.is_active,
               "tags" => character.tags
             }

      assert data.changes == %{}
      assert data.action == :update
      assert_maps_equal(data.metadata, %{table: "characters", schema: "public"}, [:table, :schema])
      assert is_struct(data.metadata.commit_timestamp, DateTime)
    end

    test "updates are replicated to consumer events when replica identity full", %{consumer: consumer} do
      # Insert a character with full replica identity
      character =
        CharacterFactory.insert_character_ident_full!(%{
          name: "Paul Atreides",
          house: "Atreides",
          planet: "Caladan",
          is_active: true,
          tags: ["heir", "kwisatz haderach"]
        })

      # Wait for the message to be handled
      assert_receive {ReplicationExt, :flush_messages}, 500

      # Update the character
      UnboxedRepo.update!(
        Ecto.Changeset.change(character, %{
          house: "Emperor",
          planet: "Arrakis",
          is_active: false,
          tags: ["emperor", "kwisatz haderach"]
        })
      )

      # Wait for the update message to be handled
      assert_receive {ReplicationExt, :flush_messages}, 500

      # Fetch consumer events
      [_insert_event, update_event] =
        Consumers.list_consumer_events_for_consumer(consumer.id, order_by: [asc: :inserted_at])

      # Assert the consumer event details
      %{data: data} = update_event

      assert data.changes == %{
               "house" => "Atreides",
               "planet" => "Caladan",
               "is_active" => true,
               "tags" => ["heir", "kwisatz haderach"]
             }

      assert data.action == :update
    end

    test "deletes are replicated to consumer events when replica identity default", %{consumer: consumer} do
      character = CharacterFactory.insert_character!()

      assert_receive {ReplicationExt, :flush_messages}, 500

      UnboxedRepo.delete!(character)
      assert_receive {ReplicationExt, :flush_messages}, 500

      [_insert_event, delete_event] = Consumers.list_consumer_events_for_consumer(consumer.id)

      %{data: data} = delete_event

      assert data.record == %{
               "house" => nil,
               "id" => character.id,
               "is_active" => nil,
               "name" => nil,
               "planet" => nil,
               "tags" => nil
             }

      assert data.changes == nil
      assert data.action == :delete
      assert_maps_equal(data.metadata, %{table: "characters", schema: "public"}, [:table, :schema])
      assert is_struct(data.metadata.commit_timestamp, DateTime)
    end

    test "deletes are replicated to consumer events when replica identity full", %{consumer: consumer} do
      character = CharacterFactory.insert_character_ident_full!()

      assert_receive {ReplicationExt, :flush_messages}, 500

      UnboxedRepo.delete!(character)
      assert_receive {ReplicationExt, :flush_messages}, 500

      [_insert_event, delete_event] = Consumers.list_consumer_events_for_consumer(consumer.id)

      %{data: data} = delete_event

      assert data.record == %{
               "id" => character.id,
               "name" => character.name,
               "house" => character.house,
               "planet" => character.planet,
               "is_active" => character.is_active,
               "tags" => character.tags
             }

      assert data.changes == nil
      assert data.action == :delete
      assert_maps_equal(data.metadata, %{table: "characters_ident_full", schema: "public"}, [:table, :schema])
      assert is_struct(data.metadata.commit_timestamp, DateTime)
    end

    test "replication with two primary key columns", %{consumer: consumer} do
      # Insert
      character = CharacterFactory.insert_character_2pk!()

      assert_receive {ReplicationExt, :flush_messages}, 500

      [insert_event] = Consumers.list_consumer_events_for_consumer(consumer.id)
      %{data: data} = insert_event

      assert data.record == %{
               "id1" => character.id1,
               "id2" => character.id2,
               "name" => character.name,
               "house" => character.house,
               "planet" => character.planet
             }

      assert is_nil(data.changes)
      assert data.action == :insert
      assert_maps_equal(data.metadata, %{table: "characters_2pk", schema: "public"}, [:table, :schema])
      assert is_struct(data.metadata.commit_timestamp, DateTime)

      # Delete
      UnboxedRepo.delete!(character)

      assert_receive {ReplicationExt, :flush_messages}, 500

      [_insert_event, delete_event] = Consumers.list_consumer_events_for_consumer(consumer.id)
      %{data: data} = delete_event

      assert data.record == %{
               "id1" => character.id1,
               "id2" => character.id2,
               "name" => nil,
               "house" => nil,
               "planet" => nil
             }

      assert is_nil(data.changes)
      assert data.action == :delete
      assert_maps_equal(data.metadata, %{table: "characters_2pk", schema: "public"}, [:table, :schema])
      assert is_struct(data.metadata.commit_timestamp, DateTime)
    end
  end

  @server_id __MODULE__
  @server_via ReplicationExt.via_tuple(@server_id)

  describe "replication in isolation" do
    test "changes are buffered in the WAL, even if the listener is not up" do
      record = CharacterFactory.insert_character!() |> Sequin.Map.from_ecto() |> Sequin.Map.stringify_keys()

      test_pid = self()

      stub(MessageHandlerMock, :handle_messages, fn _ctx, msgs ->
        send(test_pid, {:change, msgs})
      end)

      start_replication!(message_handler_module: MessageHandlerMock)
      assert_receive {:change, [change]}, :timer.seconds(5)

      assert is_action(change, :insert), "Expected change to be an insert, got: #{inspect(change)}"

      assert fields_equal?(change.fields, record)

      assert change.table_name == "characters"
      assert change.table_schema == "public"
    end

    test "changes in a transaction are buffered then delivered to message handler in order" do
      test_pid = self()

      stub(MessageHandlerMock, :handle_messages, fn _ctx, msgs ->
        send(test_pid, {:changes, msgs})
      end)

      start_replication!(message_handler_module: MessageHandlerMock)

      # Create three characters in sequence
      UnboxedRepo.transaction(fn ->
        CharacterFactory.insert_character!(name: "Paul Atreides")
        CharacterFactory.insert_character!(name: "Leto Atreides")
        CharacterFactory.insert_character!(name: "Chani")
      end)

      assert_receive {:changes, changes}, :timer.seconds(1)
      # Assert the order of changes
      assert length(changes) == 3
      [insert1, insert2, insert3] = changes

      assert is_action(insert1, :insert)
      assert get_field_value(insert1.fields, "name") == "Paul Atreides"

      assert is_action(insert2, :insert)
      assert get_field_value(insert2.fields, "name") == "Leto Atreides"

      assert is_action(insert3, :insert)
      assert get_field_value(insert3.fields, "name") == "Chani"
    end

    @tag capture_log: true
    test "changes are delivered at least once" do
      test_pid = self()

      # simulate a message mis-handle/crash
      stub(MessageHandlerMock, :handle_messages, fn _ctx, msgs ->
        send(test_pid, {:change, msgs})
        raise "Simulated crash"
      end)

      start_replication!(message_handler_module: MessageHandlerMock)

      record = CharacterFactory.insert_character!() |> Sequin.Map.from_ecto() |> Sequin.Map.stringify_keys()

      assert_receive {:change, _}, :timer.seconds(1)

      stop_replication!()

      stub(MessageHandlerMock, :handle_messages, fn _ctx, msgs ->
        send(test_pid, {:change, msgs})
      end)

      start_replication!(message_handler_module: MessageHandlerMock)

      assert_receive {:change, [change]}, :timer.seconds(1)
      assert is_action(change, :insert)

      # Should have received the record (it was re-delivered)
      assert fields_equal?(change.fields, record)
    end

    test "creates, updates, and deletes are captured" do
      test_pid = self()

      stub(MessageHandlerMock, :handle_messages, fn _ctx, msgs ->
        send(test_pid, {:change, msgs})
      end)

      start_replication!(message_handler_module: MessageHandlerMock)

      # Test create
      character = CharacterFactory.insert_character_ident_full!(planet: "Caladan")
      record = character |> Sequin.Map.from_ecto() |> Sequin.Map.stringify_keys()

      assert_receive {:change, [create_change]}, :timer.seconds(1)
      assert is_action(create_change, :insert)

      assert fields_equal?(create_change.fields, record)

      assert create_change.action == :insert

      # Test update
      UnboxedRepo.update!(Ecto.Changeset.change(character, planet: "Arrakis"))
      record = Map.put(record, "planet", "Arrakis")

      assert_receive {:change, [update_change]}, :timer.seconds(1)
      assert is_action(update_change, :update)

      assert fields_equal?(update_change.fields, record)

      refute is_nil(update_change.old_fields)

      assert Enum.find(update_change.old_fields, &(&1.column_name == "planet")).value == "Caladan"

      assert update_change.action == :update

      # Test delete
      UnboxedRepo.delete!(character)

      assert_receive {:change, [delete_change]}, :timer.seconds(1)
      assert is_action(delete_change, :delete)

      assert fields_equal?(delete_change.old_fields, record)

      assert delete_change.action == :delete
    end

    test "messages are processed exactly once, even after crash and reboot" do
      test_pid = self()

      stub(MessageHandlerMock, :handle_messages, fn _ctx, msgs ->
        send(test_pid, {:changes, msgs})
      end)

      start_replication!(message_handler_module: MessageHandlerMock)

      # Insert a record
      character1 = CharacterFactory.insert_character!()

      # Wait for the message to be handled
      assert_receive {:changes, [change]}, :timer.seconds(1)
      assert is_action(change, :insert)
      assert get_field_value(change.fields, "id") == character1.id

      # Give the ack_message time to be sent
      Process.sleep(20)

      # Stop the replication
      stop_replication!()

      # Restart the replication
      start_replication!(message_handler_module: MessageHandlerMock)

      # Insert another record to verify replication is working
      character2 = CharacterFactory.insert_character!()

      # Wait for the new message to be handled
      assert_receive {:changes, [change]}, :timer.seconds(1)

      # Verify we only get the new record
      assert is_action(change, :insert)
      assert get_field_value(change.fields, "id") == character2.id
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
          message_handler_module: MessageHandlerMock,
          message_handler_ctx: nil
        ],
        opts
      )

    start_supervised!(ReplicationExt.child_spec(opts))
  end

  defp is_action(change, action) do
    is_struct(change, Message) and change.action == action
  end

  defp stop_replication! do
    stop_supervised!(@server_via)
  end

  defp config do
    :sequin
    |> Application.get_env(Sequin.Repo)
    |> Keyword.take([:username, :password, :hostname, :database, :port])
  end

  # Helper functions

  defp get_field_value(fields, column_name) do
    Enum.find_value(fields, fn %{column_name: name, value: value} ->
      if name == column_name, do: value
    end)
  end

  defp fields_equal?(fields, record) do
    record_fields = Enum.map(record, fn {key, value} -> %{column_name: key, value: value} end)

    assert_lists_equal(fields, record_fields, fn field1, field2 ->
      field1.column_name == field2.column_name && field1.value == field2.value
    end)
  end
end

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
  # alias Sequin.Test.Support.Models.Character2PK
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

    # test "updates are replicated to the stream", %{stream: stream} do
    #   character = CharacterFactory.insert_character!(%{name: "Leto Atreides", house: "Atreides", planet: "Caladan"})

    #   assert_receive {ReplicationExt, :flush_messages}, 1_000

    #   UnboxedRepo.update!(Ecto.Changeset.change(character, planet: "Arrakis"))
    #   assert_receive {ReplicationExt, :flush_messages}, 1_000

    #   [message] = Streams.list_messages_for_stream(stream.id)
    #   assert message.key =~ "public.characters"

    #   decoded_data = Jason.decode!(message.data)

    #   assert decoded_data["data"] == %{
    #            "id" => 1,
    #            "name" => "Leto Atreides",
    #            "house" => "Atreides",
    #            "planet" => "Arrakis",
    #            "is_active" => true,
    #            "tags" => []
    #          }

    #   refute decoded_data["deleted"]
    #   assert decoded_data["changes"] == %{"planet" => "Caladan"}
    #   assert decoded_data["action"] == "update"
    # end

    # test "deletes are replicated to the stream", %{stream: stream} do
    #   character = CharacterFactory.insert_character!(%{name: "Duncan Idaho", house: "Atreides", planet: "Caladan"})

    #   assert_receive {ReplicationExt, :flush_messages}, 1_000

    #   UnboxedRepo.delete!(character)
    #   assert_receive {ReplicationExt, :flush_messages}, 1_000

    #   [message] = Streams.list_messages_for_stream(stream.id)
    #   assert message.key =~ "public.characters"

    #   decoded_data = Jason.decode!(message.data)

    #   assert decoded_data["data"] == %{
    #            "id" => 1,
    #            "name" => "Duncan Idaho",
    #            "house" => "Atreides",
    #            "planet" => "Caladan",
    #            "is_active" => true,
    #            "tags" => []
    #          }

    #   assert decoded_data["deleted"]
    #   assert decoded_data["changes"] == %{}
    #   assert decoded_data["action"] == "delete"
    # end

    # test "replication with default replica identity", %{stream: stream} do
    #   CharacterFactory.insert_character!(%{name: "Chani", house: "Fremen", planet: "Arrakis"})

    #   assert_receive {ReplicationExt, :flush_messages}, 1_000

    #   [insert_message] = Streams.list_messages_for_stream(stream.id)
    #   decoded_insert_data = Jason.decode!(insert_message.data)
    #   assert decoded_insert_data["data"] == %{"id" => 1, "name" => "Chani", "house" => "Fremen", "planet" => "Arrakis"}
    #   refute decoded_insert_data["deleted"]
    #   assert decoded_insert_data["changes"] == %{}
    #   assert decoded_insert_data["action"] == "insert"

    #   1
    #   |> Character.where_id()
    #   |> UnboxedRepo.update_all(house: "Atreides")

    #   assert_receive {ReplicationExt, :flush_messages}, 1_000

    #   [update_message] = Streams.list_messages_for_stream(stream.id)
    #   assert update_message.seq > insert_message.seq
    #   assert update_message.key == insert_message.key
    #   assert DateTime.compare(update_message.inserted_at, insert_message.inserted_at) == :eq

    #   decoded_update_data = Jason.decode!(update_message.data)
    #   assert decoded_update_data["data"] == %{"id" => 1, "name" => "Chani", "house" => "Atreides", "planet" => "Arrakis"}
    #   refute decoded_update_data["deleted"]
    #   assert decoded_update_data["changes"] == %{}
    #   assert decoded_update_data["action"] == "update"

    #   1
    #   |> Character.where_id()
    #   |> UnboxedRepo.delete_all()

    #   assert_receive {ReplicationExt, :flush_messages}, 1_000

    #   [delete_message] = Streams.list_messages_for_stream(stream.id)
    #   assert delete_message.seq > update_message.seq
    #   assert delete_message.key == update_message.key
    #   assert DateTime.compare(delete_message.inserted_at, update_message.inserted_at) == :eq

    #   decoded_delete_data = Jason.decode!(delete_message.data)
    #   assert decoded_delete_data["data"]["id"] == 1
    #   assert decoded_delete_data["deleted"]
    #   assert decoded_delete_data["changes"] == %{}
    #   assert decoded_delete_data["action"] == "delete"
    # end

    # test "replication with two primary key columns", %{stream: stream} do
    #   # Insert
    #   CharacterFactory.insert_character_2pk!(%{
    #     id1: 1,
    #     id2: 2,
    #     name: "Paul Atreides",
    #     house: "Atreides",
    #     planet: "Arrakis"
    #   })

    #   assert_receive {ReplicationExt, :flush_messages}, 1_000

    #   [insert_message] = Streams.list_messages_for_stream(stream.id)
    #   decoded_insert_data = Jason.decode!(insert_message.data)
    #   assert %{"id1" => 1, "id2" => 2} = decoded_insert_data["data"]
    #   refute decoded_insert_data["deleted"]
    #   assert decoded_insert_data["changes"] == %{}
    #   assert decoded_insert_data["action"] == "insert"

    #   # Delete
    #   1
    #   |> Character2PK.where_id(2)
    #   |> UnboxedRepo.delete_all()

    #   assert_receive {ReplicationExt, :flush_messages}, 1_000

    #   [delete_message] = Streams.list_messages_for_stream(stream.id)
    #   decoded_delete_data = Jason.decode!(delete_message.data)
    #   assert %{"id1" => 1, "id2" => 2} = decoded_delete_data["data"]
    #   assert decoded_delete_data["deleted"]
    #   assert decoded_delete_data["changes"] == %{}
    #   assert decoded_delete_data["action"] == "delete"
    # end

    # test "add_info returns correct information", %{pg_replication: pg_replication} do
    #   # Insert some data to ensure there's a last committed timestamp
    #   CharacterFactory.insert_character!()

    #   assert_receive {ReplicationExt, :flush_messages}, 1000

    #   # Call add_info
    #   pg_replication_with_info = Replication.add_info(pg_replication)

    #   # Assert that the info field is populated
    #   assert %PostgresReplicationSlot.Info{} = pg_replication_with_info.info

    #   # Check last_committed_at
    #   assert pg_replication_with_info.info.last_committed_at != nil
    #   assert DateTime.before?(pg_replication_with_info.info.last_committed_at, DateTime.utc_now())
    # end

    # test "replication with 'with_operation' key format", %{
    #   stream: stream,
    #   pg_replication: pg_replication,
    #   sup: sup
    # } do
    #   {:ok, pg_replication} = Replication.update_pg_replication(pg_replication, %{key_format: :with_operation})

    #   # Restart the server
    #   ReplicationRuntime.Supervisor.stop_for_pg_replication(sup, pg_replication)

    #   {:ok, _pid} = ReplicationRuntime.Supervisor.start_for_pg_replication(sup, pg_replication, test_pid: self())

    #   CharacterFactory.insert_character!()

    #   assert_receive {ReplicationExt, :flush_messages}, 500

    #   [message] = Streams.list_messages_for_stream(stream.id)
    #   assert message.key =~ "public.characters.insert"
    # end

    # test "updates with FULL replica identity include changed fields", %{
    #   stream: stream,
    #   source_db: source_db
    # } do
    #   character =
    #     CharacterFactory.insert_character!(%{
    #       name: "Chani",
    #       house: "Fremen",
    #       planet: "Arrakis",
    #       is_active: true,
    #       tags: ["warrior", "seer", "royal,compound"]
    #     })

    #   assert_receive {ReplicationExt, :flush_messages}, 1_000

    #   UnboxedRepo.update!(
    #     Ecto.Changeset.change(character,
    #       house: "Atreides",
    #       planet: "Caladan",
    #       is_active: false,
    #       tags: ["warrior", "seer", "royal,interest"]
    #     )
    #   )

    #   assert_receive {ReplicationExt, :flush_messages}, 1_000

    #   dbg(Streams.list_messages_for_stream(stream.id))
    #   [update_message] = Streams.list_messages_for_stream(stream.id)
    #   assert update_message.key == "#{source_db.name}.public.characters.1"

    #   decoded_data = Jason.decode!(update_message.data)

    #   assert decoded_data["data"] == %{
    #            "id" => 1,
    #            "name" => "Chani",
    #            "house" => "Atreides",
    #            "planet" => "Caladan",
    #            "is_active" => false,
    #            "tags" => ["warrior", "seer", "royal,interest"]
    #          }

    #   refute decoded_data["deleted"]

    #   assert decoded_data["changes"] == %{
    #            "house" => "Fremen",
    #            "planet" => "Arrakis",
    #            "is_active" => true,
    #            "tags" => ["warrior", "seer", "royal,compound"]
    #          }

    #   assert decoded_data["action"] == "update"
    # end
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

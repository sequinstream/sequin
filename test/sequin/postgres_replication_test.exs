defmodule Sequin.PostgresReplicationTest do
  @moduledoc """
  This test file contains both unit tests for the Replication extension as well as integration
  tests with PostgresReplicationSlot. The reason for combining the two is interference: when the two
  test suites are run side-by-side, they interfere with each other. We didn't get to the bottom of
  the interference (issues starting the replication connection?), but it definitely seems to occur
  when the two tests are running and connecting to slots at the same time.

  We're making this test async: false to avoid interference with other tests, as they need to use
  character tables un-sandboxed (to produce WAL). We can make these async true with a clever trick
  like isolating tests with partitioned tables. (Cost is low, at time of writing these tests take 0.6s)
  """
  use Sequin.DataCase, async: false

  alias Sequin.Consumers
  alias Sequin.Consumers.SequenceFilter
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.DatabasesRuntime
  alias Sequin.DatabasesRuntime.MessageHandlerMock
  alias Sequin.DatabasesRuntime.SlotProcessor
  alias Sequin.DatabasesRuntime.SlotProcessor.Message
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.CharacterFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Replication
  alias Sequin.Test.UnboxedRepo
  alias Sequin.TestSupport.Models.Character
  alias Sequin.TestSupport.Models.CharacterDetailed
  alias Sequin.TestSupport.Models.CharacterIdentFull
  alias Sequin.TestSupport.Models.CharacterMultiPK
  alias Sequin.TestSupport.ReplicationSlots

  @moduletag :unboxed

  @publication "characters_publication"

  def replication_slot, do: ReplicationSlots.slot_name(__MODULE__)

  setup do
    # Fast-forward the replication slot to the current WAL position
    :ok = ReplicationSlots.reset_slot(UnboxedRepo, replication_slot())

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
          account_id: account_id,
          status: :active
        )

      character_sequence =
        DatabasesFactory.insert_sequence!(
          account_id: account_id,
          postgres_database_id: source_db.id,
          table_oid: Character.table_oid()
        )

      character_ident_full_sequence =
        DatabasesFactory.insert_sequence!(
          account_id: account_id,
          postgres_database_id: source_db.id,
          table_oid: CharacterIdentFull.table_oid()
        )

      character_multi_pk_sequence =
        DatabasesFactory.insert_sequence!(
          account_id: account_id,
          postgres_database_id: source_db.id,
          table_oid: CharacterMultiPK.table_oid()
        )

      # Create consumers for each table type (event)
      event_character_consumer =
        ConsumersFactory.insert_sink_consumer!(
          name: "event_character_consumer",
          message_kind: :event,
          replication_slot_id: pg_replication.id,
          account_id: account_id,
          source_tables: [],
          sequence_id: character_sequence.id,
          sequence_filter:
            ConsumersFactory.sequence_filter_attrs(
              actions: [:insert, :update, :delete],
              column_filters: [],
              group_column_attnums: [Character.column_attnum("id")]
            )
        )

      event_character_ident_consumer =
        ConsumersFactory.insert_sink_consumer!(
          name: "event_character_ident_consumer",
          message_kind: :event,
          replication_slot_id: pg_replication.id,
          account_id: account_id,
          source_tables: [],
          sequence_id: character_ident_full_sequence.id,
          sequence_filter:
            ConsumersFactory.sequence_filter_attrs(
              actions: [:insert, :update, :delete],
              column_filters: [],
              group_column_attnums: [CharacterIdentFull.column_attnum("id")]
            )
        )

      event_character_multi_pk_consumer =
        ConsumersFactory.insert_sink_consumer!(
          name: "event_character_multi_pk_consumer",
          message_kind: :event,
          replication_slot_id: pg_replication.id,
          account_id: account_id,
          source_tables: [],
          sequence_id: character_multi_pk_sequence.id,
          sequence_filter:
            ConsumersFactory.sequence_filter_attrs(
              actions: [:insert, :update, :delete],
              column_filters: [],
              group_column_attnums: [
                CharacterMultiPK.column_attnum("id_integer"),
                CharacterMultiPK.column_attnum("id_string"),
                CharacterMultiPK.column_attnum("id_uuid")
              ]
            )
        )

      # Create consumers for each table type (record)
      record_character_consumer =
        ConsumersFactory.insert_sink_consumer!(
          name: "record_character_consumer",
          message_kind: :record,
          replication_slot_id: pg_replication.id,
          account_id: account_id,
          source_tables: [],
          sequence_id: character_sequence.id,
          sequence_filter:
            ConsumersFactory.sequence_filter_attrs(
              actions: [:insert, :update, :delete],
              column_filters: [],
              group_column_attnums: [Character.column_attnum("id")]
            )
        )

      record_character_ident_consumer =
        ConsumersFactory.insert_sink_consumer!(
          name: "record_character_ident_consumer",
          message_kind: :record,
          replication_slot_id: pg_replication.id,
          account_id: account_id,
          source_tables: [],
          sequence_id: character_ident_full_sequence.id,
          sequence_filter:
            ConsumersFactory.sequence_filter_attrs(
              actions: [:insert, :update, :delete],
              column_filters: [],
              group_column_attnums: [CharacterIdentFull.column_attnum("id")]
            )
        )

      record_character_multi_pk_consumer =
        ConsumersFactory.insert_sink_consumer!(
          name: "record_character_multi_pk_consumer",
          message_kind: :record,
          replication_slot_id: pg_replication.id,
          account_id: account_id,
          source_tables: [],
          sequence_id: character_multi_pk_sequence.id,
          sequence_filter:
            ConsumersFactory.sequence_filter_attrs(
              actions: [:insert, :update, :delete],
              column_filters: [],
              group_column_attnums: [
                CharacterMultiPK.column_attnum("id_integer"),
                CharacterMultiPK.column_attnum("id_string"),
                CharacterMultiPK.column_attnum("id_uuid")
              ]
            )
        )

      event_character_consumer = Repo.preload(event_character_consumer, :postgres_database)
      event_character_ident_consumer = Repo.preload(event_character_ident_consumer, :postgres_database)
      event_character_multi_pk_consumer = Repo.preload(event_character_multi_pk_consumer, :postgres_database)

      record_character_consumer = Repo.preload(record_character_consumer, :postgres_database)
      record_character_ident_consumer = Repo.preload(record_character_ident_consumer, :postgres_database)
      record_character_multi_pk_consumer = Repo.preload(record_character_multi_pk_consumer, :postgres_database)

      sup = Module.concat(__MODULE__, DatabasesRuntime.Supervisor)
      start_supervised!(Sequin.DynamicSupervisor.child_spec(name: sup))
      {:ok, _} = DatabasesRuntime.Supervisor.start_replication(sup, pg_replication, test_pid: self())

      %{
        pg_replication: pg_replication,
        source_db: source_db,
        event_character_consumer: event_character_consumer,
        event_character_ident_consumer: event_character_ident_consumer,
        event_character_multi_pk_consumer: event_character_multi_pk_consumer,
        record_character_consumer: record_character_consumer,
        record_character_ident_consumer: record_character_ident_consumer,
        record_character_multi_pk_consumer: record_character_multi_pk_consumer
      }
    end

    test "inserts are replicated to consumer events", %{event_character_consumer: consumer} do
      # Insert a character
      character = CharacterFactory.insert_character!([], repo: UnboxedRepo)

      # Wait for the message to be handled
      assert_receive {SlotProcessor, :flush_messages}, 500

      # Fetch consumer events
      [consumer_event] = Consumers.list_consumer_events_for_consumer(consumer.id)

      # Assert the consumer event details
      assert consumer_event.consumer_id == consumer.id
      assert consumer_event.table_oid == Character.table_oid()
      assert consumer_event.record_pks == [to_string(character.id)]
      %{data: data} = consumer_event

      assert_maps_equal(data.record, Map.from_struct(character), ["id", "name", "house", "planet", "is_active", "tags"],
        indifferent_keys: true
      )

      assert is_nil(data.changes)
      assert data.action == :insert

      assert_maps_equal(
        data.metadata,
        %{table_name: "Characters", table_schema: "public", database_name: consumer.postgres_database.name},
        [:table_name, :table_schema, :database_name]
      )

      assert is_struct(data.metadata.commit_timestamp, DateTime)
    end

    test "inserts are replicated to consumer records", %{record_character_consumer: consumer} do
      # Insert a character
      character = CharacterFactory.insert_character!([], repo: UnboxedRepo)

      # Wait for the message to be handled
      assert_receive {SlotProcessor, :flush_messages}, 500

      # Fetch consumer records
      [consumer_record] = Consumers.list_consumer_records_for_consumer(consumer.id)

      # Assert the consumer record details
      assert consumer_record.consumer_id == consumer.id
      assert consumer_record.table_oid == Character.table_oid()
      assert consumer_record.record_pks == [to_string(character.id)]
      assert consumer_record.group_id == to_string(character.id)
    end

    test "updates are replicated to consumer events when replica identity default", %{event_character_consumer: consumer} do
      # Insert a character
      character =
        CharacterFactory.insert_character!([name: "Leto Atreides", house: "Atreides", planet: "Caladan"],
          repo: UnboxedRepo
        )

      # Wait for the message to be handled
      assert_receive {SlotProcessor, :flush_messages}, 500

      # Update the character
      UnboxedRepo.update!(Ecto.Changeset.change(character, planet: "Arrakis"))

      # Wait for the update message to be handled
      assert_receive {SlotProcessor, :flush_messages}, 500

      # Fetch consumer events
      [_insert_event, update_event] =
        Consumers.list_consumer_events_for_consumer(consumer.id, order_by: [asc: :inserted_at])

      # Assert the consumer event details
      assert update_event.consumer_id == consumer.id
      assert update_event.table_oid == Character.table_oid()
      assert update_event.record_pks == [to_string(character.id)]
      %{data: data} = update_event

      character = Repo.reload(character)

      assert data.record == %{
               "id" => character.id,
               "name" => character.name,
               "house" => character.house,
               "planet" => "Arrakis",
               "is_active" => character.is_active,
               "tags" => character.tags,
               "metadata" => character.metadata,
               "inserted_at" => NaiveDateTime.to_iso8601(character.inserted_at),
               "updated_at" => NaiveDateTime.to_iso8601(character.updated_at)
             }

      assert data.changes == %{}
      assert data.action == :update

      assert_maps_equal(
        data.metadata,
        %{table_name: "Characters", table_schema: "public", database_name: consumer.postgres_database.name},
        [:table_name, :table_schema, :database_name]
      )

      assert is_struct(data.metadata.commit_timestamp, DateTime)
    end

    test "updates are replicated to consumer records when replica identity default", %{
      record_character_consumer: consumer
    } do
      # Insert a character
      character = CharacterFactory.insert_character!([], repo: UnboxedRepo)

      # Wait for the message to be handled
      assert_receive {SlotProcessor, :flush_messages}, 500

      [insert_record] = Consumers.list_consumer_records_for_consumer(consumer.id)

      # Update the character
      UnboxedRepo.update!(Ecto.Changeset.change(character, planet: "Arrakis"))

      # Wait for the update message to be handled
      assert_receive {SlotProcessor, :flush_messages}, 500

      # Fetch consumer records
      [update_record] = Consumers.list_consumer_records_for_consumer(consumer.id)

      # Assert the consumer record details
      assert update_record.consumer_id == consumer.id
      assert update_record.table_oid == Character.table_oid()
      assert update_record.record_pks == [to_string(character.id)]
      assert DateTime.compare(insert_record.inserted_at, update_record.inserted_at) == :eq
      refute DateTime.compare(insert_record.updated_at, update_record.updated_at) == :eq
    end

    test "updates are replicated to consumer events when replica identity full", %{
      event_character_ident_consumer: consumer
    } do
      # Insert a character with full replica identity
      character =
        CharacterFactory.insert_character_ident_full!(
          [
            name: "Paul Atreides",
            house: "Atreides",
            planet: "Caladan",
            is_active: true,
            tags: ["heir", "kwisatz haderach"]
          ],
          repo: UnboxedRepo
        )

      # Wait for the message to be handled
      assert_receive {SlotProcessor, :flush_messages}, 500

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
      assert_receive {SlotProcessor, :flush_messages}, 500

      # Fetch consumer events
      [_insert_event, update_event] =
        Consumers.list_consumer_events_for_consumer(consumer.id, order_by: [asc: :inserted_at])

      # Assert the consumer event details
      %{data: data} = update_event

      assert_maps_equal(
        data.changes,
        %{
          "house" => "Atreides",
          "planet" => "Caladan",
          "is_active" => true,
          "tags" => ["heir", "kwisatz haderach"]
        },
        ["house", "planet", "is_active", "tags"]
      )

      assert data.action == :update
    end

    test "deletes are replicated to consumer events when replica identity default", %{event_character_consumer: consumer} do
      character = CharacterFactory.insert_character!([], repo: UnboxedRepo)

      assert_receive {SlotProcessor, :flush_messages}, 500

      UnboxedRepo.delete!(character)
      assert_receive {SlotProcessor, :flush_messages}, 500

      [_insert_event, delete_event] = Consumers.list_consumer_events_for_consumer(consumer.id)

      %{data: data} = delete_event

      assert data.record == %{
               "house" => nil,
               "id" => character.id,
               "is_active" => nil,
               "name" => nil,
               "planet" => nil,
               "tags" => nil,
               "metadata" => nil,
               "inserted_at" => nil,
               "updated_at" => nil
             }

      assert data.changes == nil
      assert data.action == :delete
      assert_maps_equal(data.metadata, %{table_name: "Characters", table_schema: "public"}, [:table_name, :table_schema])
      assert is_struct(data.metadata.commit_timestamp, DateTime)
    end

    test "deletes are replicated to consumer records when replica identity default", %{
      record_character_consumer: consumer
    } do
      character = CharacterFactory.insert_character!([], repo: UnboxedRepo)

      assert_receive {SlotProcessor, :flush_messages}, 500

      [_insert_record] = Consumers.list_consumer_records_for_consumer(consumer.id)

      UnboxedRepo.delete!(character)
      assert_receive {SlotProcessor, :flush_messages}, 500

      [] = Consumers.list_consumer_records_for_consumer(consumer.id)
    end

    test "deletes are replicated to consumer events when replica identity full", %{
      event_character_ident_consumer: consumer
    } do
      character = CharacterFactory.insert_character_ident_full!([], repo: UnboxedRepo)

      assert_receive {SlotProcessor, :flush_messages}, 500

      UnboxedRepo.delete!(character)
      assert_receive {SlotProcessor, :flush_messages}, 500

      [_insert_event, delete_event] = Consumers.list_consumer_events_for_consumer(consumer.id)

      %{data: data} = delete_event

      assert_maps_equal(data.record, Map.from_struct(character), ["id", "name", "house", "planet", "is_active", "tags"],
        indifferent_keys: true
      )

      assert data.changes == nil
      assert data.action == :delete

      assert_maps_equal(data.metadata, %{table_name: "characters_ident_full", table_schema: "public"}, [
        :table_name,
        :table_schema
      ])

      assert is_struct(data.metadata.commit_timestamp, DateTime)
    end

    test "replication with multiple primary key columns", %{
      event_character_multi_pk_consumer: event_consumer,
      record_character_multi_pk_consumer: record_consumer
    } do
      # Randomly select a consumer
      consumer = Enum.random([event_consumer, record_consumer])

      # Insert
      character = CharacterFactory.insert_character_multi_pk!([], repo: UnboxedRepo)

      assert_receive {SlotProcessor, :flush_messages}, 1000

      [insert_message] = list_messages(consumer)

      # Assert the consumer message details
      assert insert_message.consumer_id == consumer.id
      assert insert_message.table_oid == CharacterMultiPK.table_oid()

      assert insert_message.record_pks == [
               to_string(character.id_integer),
               character.id_string,
               to_string(character.id_uuid)
             ]
    end

    test "consumer with column filter only receives relevant messages", %{
      event_character_consumer: event_consumer,
      record_character_consumer: record_consumer
    } do
      # Randomly select a consumer
      consumer = Enum.random([event_consumer, record_consumer])

      sequence_filter =
        ConsumersFactory.sequence_filter_attrs(
          actions: [:insert, :update],
          column_filters: [
            ConsumersFactory.sequence_filter_column_filter_attrs(
              column_attnum: Character.column_attnum("is_active"),
              operator: :==,
              value_type: :boolean,
              value: %{__type__: :boolean, value: true}
            )
          ]
        )

      {:ok, _} = Consumers.update_consumer_with_lifecycle(consumer, %{sequence_filter: sequence_filter})

      # Insert a character that doesn't match the filter
      CharacterFactory.insert_character!([is_active: false], repo: UnboxedRepo)

      # Wait for the message to be handled
      assert_receive {SlotProcessor, :flush_messages}, 500

      # Verify no consumer message was created
      assert list_messages(consumer) == []

      # Insert a character that matches the filter
      matching_character = CharacterFactory.insert_character!([is_active: true], repo: UnboxedRepo)

      # Wait for the message to be handled
      assert_receive {SlotProcessor, :flush_messages}, 500

      # Fetch consumer messages
      [consumer_message] = list_messages(consumer)

      # Assert the consumer message details
      assert consumer_message.consumer_id == consumer.id

      # Update the matching character (should not trigger a message)
      UnboxedRepo.update!(Ecto.Changeset.change(matching_character, is_active: false))

      # Wait for the message to be handled
      assert_receive {SlotProcessor, :flush_messages}, 500

      # Verify no new consumer message was created
      assert length(list_messages(consumer)) == 1
    end

    test "inserts are fanned out to both events and records", %{
      event_character_consumer: event_consumer,
      record_character_consumer: record_consumer
    } do
      # Insert a character
      CharacterFactory.insert_character!([], repo: UnboxedRepo)

      # Wait for the message to be handled
      assert_receive {SlotProcessor, :flush_messages}, 500

      # Fetch consumer events
      [consumer_event] = Consumers.list_consumer_events_for_consumer(event_consumer.id)

      # Fetch consumer records
      [consumer_record] = Consumers.list_consumer_records_for_consumer(record_consumer.id)

      # Assert both event and record were created
      assert consumer_event.consumer_id == event_consumer.id
      assert consumer_record.consumer_id == record_consumer.id

      # Assert both have the same data
      assert consumer_event.table_oid == consumer_record.table_oid
      assert consumer_event.record_pks == consumer_record.record_pks
    end

    test "empty array fields are replicated correctly", %{event_character_consumer: consumer} do
      # Insert a character with an empty array field
      character = CharacterFactory.insert_character!([tags: []], repo: UnboxedRepo)

      # Wait for the message to be handled
      assert_receive {SlotProcessor, :flush_messages}, 500

      # Fetch consumer events
      [consumer_event] = Consumers.list_consumer_events_for_consumer(consumer.id)

      # Assert the consumer event details
      assert consumer_event.consumer_id == consumer.id
      assert consumer_event.table_oid == Character.table_oid()
      assert consumer_event.record_pks == [to_string(character.id)]
      %{data: data} = consumer_event

      # Check that the tags field is an empty list, not [""]
      assert data.record["tags"] == [], "Expected empty array, got: #{inspect(data.record["tags"])}"
    end

    # Postgres quirk - the logical decoding process does not distinguish between an empty array and an array with an empty string.
    # https://chatgpt.com/share/6707334f-0978-8006-8358-ec2300d759a4
    test "array fields with empty string are returned as empty list", %{event_character_consumer: consumer} do
      # Insert a character with an array containing an empty string
      CharacterFactory.insert_character!([tags: [""]], repo: UnboxedRepo)

      # Wait for the message to be handled
      assert_receive {SlotProcessor, :flush_messages}, 500

      # Fetch consumer events
      [consumer_event] = Consumers.list_consumer_events_for_consumer(consumer.id)

      # Assert the consumer event details
      %{data: data} = consumer_event

      # Check that the tags field contains an empty string
      # Postgres quirk - the logical decoding slot will return `{}` for both `{}` and `{""}`.
      assert data.record["tags"] == [], "Expected array with empty string, got: #{inspect(data.record["tags"])}"
    end

    test "array fields are updated correctly from non-empty to empty", %{event_character_ident_consumer: consumer} do
      # Insert a character with a non-empty array field
      character = CharacterFactory.insert_character_ident_full!([tags: ["tag1", "tag2"]], repo: UnboxedRepo)

      # Wait for the insert message to be handled
      assert_receive {SlotProcessor, :flush_messages}, 500

      # Update the character with an empty array
      UnboxedRepo.update!(Ecto.Changeset.change(character, tags: []))

      # Wait for the update message to be handled
      assert_receive {SlotProcessor, :flush_messages}, 500

      # Fetch consumer events
      [_insert_event, update_event] =
        Consumers.list_consumer_events_for_consumer(consumer.id, order_by: [asc: :inserted_at])

      # Assert the consumer event details
      %{data: data} = update_event

      # Check that the changes field shows the previous non-empty array
      assert data.changes["tags"] == ["tag1", "tag2"],
             "Expected non-empty array in changes, got: #{inspect(data.changes["tags"])}"

      # Check that the record field shows the new empty array
      assert data.record["tags"] == [], "Expected empty array in record, got: #{inspect(data.record["tags"])}"
    end
  end

  def server_id, do: :"#{__MODULE__}-#{inspect(self())}"
  def server_via, do: SlotProcessor.via_tuple(server_id())

  describe "replication in isolation" do
    test "changes are buffered in the WAL, even if the listener is not up" do
      record =
        []
        |> CharacterFactory.insert_character!(repo: UnboxedRepo)
        |> Sequin.Map.from_ecto()
        |> Sequin.Map.stringify_keys()

      test_pid = self()

      stub(MessageHandlerMock, :handle_messages, fn ctx, msgs ->
        send(test_pid, {:change, msgs})
        {:ok, length(msgs), ctx}
      end)

      start_replication!(message_handler_module: MessageHandlerMock)
      assert_receive {:change, [change]}, :timer.seconds(5)

      assert is_action(change, :insert), "Expected change to be an insert, got: #{inspect(change)}"

      assert fields_equal?(change.fields, record)

      assert change.table_name == "Characters"
      assert change.table_schema == "public"
    end

    test "changes in a transaction are buffered then delivered to message handler in order" do
      test_pid = self()

      stub(MessageHandlerMock, :handle_messages, fn ctx, msgs ->
        send(test_pid, {:changes, msgs})
        {:ok, length(msgs), ctx}
      end)

      start_replication!(message_handler_module: MessageHandlerMock)

      # Create three characters in sequence
      UnboxedRepo.transaction(fn ->
        CharacterFactory.insert_character!([name: "Paul Atreides"], repo: UnboxedRepo)
        CharacterFactory.insert_character!([name: "Leto Atreides"], repo: UnboxedRepo)
        CharacterFactory.insert_character!([name: "Chani"], repo: UnboxedRepo)
      end)

      assert_receive {:changes, changes}, :timer.seconds(1)
      # Assert the order of changes
      assert length(changes) == 3
      [insert1, insert2, insert3] = changes

      # Assert seq values increase within transaction
      base_seq = insert1.seq
      assert insert2.seq == base_seq + 1
      assert insert3.seq == base_seq + 2

      assert is_action(insert1, :insert)
      assert get_field_value(insert1.fields, "name") == "Paul Atreides"
      assert insert1.commit_idx == 0

      assert is_action(insert2, :insert)
      assert get_field_value(insert2.fields, "name") == "Leto Atreides"
      assert insert2.commit_idx == 1

      assert is_action(insert3, :insert)
      assert get_field_value(insert3.fields, "name") == "Chani"
      assert insert3.commit_idx == 2

      # Insert another character
      CharacterFactory.insert_character!([name: "Duncan Idaho"], repo: UnboxedRepo)

      assert_receive {:changes, [insert4]}, :timer.seconds(1)
      # commit_idx resets but seq should be higher than previous transaction
      assert insert4.commit_idx == 0
      assert insert4.seq > insert3.seq
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

      record =
        []
        |> CharacterFactory.insert_character!(repo: UnboxedRepo)
        |> Sequin.Map.from_ecto()
        |> Sequin.Map.stringify_keys()

      assert_receive {:change, _}, :timer.seconds(1)

      stop_replication!()

      stub(MessageHandlerMock, :handle_messages, fn ctx, msgs ->
        send(test_pid, {:change, msgs})
        {:ok, length(msgs), ctx}
      end)

      start_replication!(message_handler_module: MessageHandlerMock)

      assert_receive {:change, [change]}, :timer.seconds(1)
      assert is_action(change, :insert)

      # Should have received the record (it was re-delivered)
      assert fields_equal?(change.fields, record)
    end

    test "creates, updates, and deletes are captured" do
      test_pid = self()

      stub(MessageHandlerMock, :handle_messages, fn ctx, msgs ->
        send(test_pid, {:change, msgs})
        {:ok, length(msgs), ctx}
      end)

      start_replication!(message_handler_module: MessageHandlerMock)

      # Test create
      character = CharacterFactory.insert_character_ident_full!([planet: "Caladan"], repo: UnboxedRepo)
      record = character |> Sequin.Map.from_ecto() |> Sequin.Map.stringify_keys()

      assert_receive {:change, [create_change]}, :timer.seconds(1)
      assert is_action(create_change, :insert)
      assert is_integer(create_change.seq)
      assert create_change.seq == create_change.commit_lsn + create_change.commit_idx

      assert fields_equal?(create_change.fields, record)
      assert create_change.action == :insert

      # Test update
      UnboxedRepo.update!(Ecto.Changeset.change(character, planet: "Arrakis"))
      record = Map.put(record, "planet", "Arrakis")

      assert_receive {:change, [update_change]}, :timer.seconds(1)
      assert is_action(update_change, :update)
      assert is_integer(update_change.seq)
      assert update_change.seq > create_change.seq
      assert update_change.seq == update_change.commit_lsn + update_change.commit_idx

      assert fields_equal?(update_change.fields, record)
      refute is_nil(update_change.old_fields)
      assert Enum.find(update_change.old_fields, &(&1.column_name == "planet")).value == "Caladan"
      assert update_change.action == :update

      # Test delete
      UnboxedRepo.delete!(character)

      assert_receive {:change, [delete_change]}, :timer.seconds(1)
      assert is_action(delete_change, :delete)
      assert is_integer(delete_change.seq)
      assert delete_change.seq > update_change.seq
      assert delete_change.seq == delete_change.commit_lsn + delete_change.commit_idx

      assert fields_equal?(delete_change.old_fields, record)
      assert delete_change.action == :delete
    end

    test "messages are processed exactly once, even after crash and reboot" do
      test_pid = self()

      stub(MessageHandlerMock, :handle_messages, fn ctx, msgs ->
        send(test_pid, {:changes, msgs})
        {:ok, length(msgs), ctx}
      end)

      start_replication!(message_handler_module: MessageHandlerMock)

      # Insert a record
      character1 = CharacterFactory.insert_character!([], repo: UnboxedRepo)

      # Wait for the message to be handled
      assert_receive {:changes, [change]}, :timer.seconds(1)
      assert is_action(change, :insert)
      assert get_field_value(change.fields, "id") == character1.id

      Replication.put_last_processed_seq!(server_id(), change.seq)

      # Stop the replication - likely before the message was acked, but there is a race here
      stop_replication!()

      # Restart the replication
      start_replication!(message_handler_module: MessageHandlerMock)

      # Insert another record to verify replication is working
      character2 = CharacterFactory.insert_character!([], repo: UnboxedRepo)

      # Wait for the new message to be handled
      assert_receive {:changes, [change]}, :timer.seconds(1)

      # Verify we only get the new record
      assert is_action(change, :insert)
      assert get_field_value(change.fields, "id") == character2.id
    end

    @tag capture_log: true
    test "fails to start when replication slot does not exist" do
      # Use a non-existent slot name
      non_existent_slot = "non_existent_slot"

      # Attempt to start replication with the non-existent slot
      start_replication!(slot_name: non_existent_slot)

      assert_receive {:stop_replication, _id}, 2000

      # Verify that the Health status was updated
      {:ok, health} = Sequin.Health.get(%PostgresDatabase{id: "test_db_id"})
      assert health.status == :error

      check = Enum.find(health.checks, &(&1.id == :replication_connected))
      assert check.status == :error
      assert check.error.message =~ "Replication slot '#{non_existent_slot}' does not exist"
    end

    test "emits heartbeat messages" do
      # Attempt to start replication with the non-existent slot
      start_replication!(heartbeat_interval: 5)

      assert_receive {SlotProcessor, :heartbeat_received}, 1000
      assert_receive {SlotProcessor, :heartbeat_received}, 1000

      # Verify that the Health status was updated
      {:ok, health} = Sequin.Health.get(%PostgresDatabase{id: "test_db_id"})
      assert health.status == :healthy

      check = Enum.find(health.checks, &(&1.id == :replication_messages))
      assert check.status == :healthy
    end
  end

  describe "PostgresReplicationSlot end-to-end with sequences" do
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
          account_id: account_id,
          status: :active
        )

      sequence =
        DatabasesFactory.insert_sequence!(
          account_id: account_id,
          postgres_database_id: source_db.id,
          table_oid: Character.table_oid()
        )

      # Create a consumer for this replication slot (event)
      event_consumer =
        ConsumersFactory.insert_sink_consumer!(
          message_kind: :event,
          replication_slot_id: pg_replication.id,
          account_id: account_id,
          source_tables: [],
          sequence_id: sequence.id,
          sequence_filter: ConsumersFactory.sequence_filter_attrs(column_filters: [])
        )

      # Create a consumer for this replication slot (record)
      record_consumer =
        ConsumersFactory.insert_sink_consumer!(
          message_kind: :record,
          replication_slot_id: pg_replication.id,
          account_id: account_id,
          source_tables: [],
          sequence_id: sequence.id,
          sequence_filter:
            ConsumersFactory.sequence_filter_attrs(
              group_column_attnums: [Character.column_attnum("id")],
              column_filters: []
            )
        )

      # Start replication
      sup = Module.concat(__MODULE__, DatabasesRuntime.Supervisor)
      start_supervised!(Sequin.DynamicSupervisor.child_spec(name: sup))

      {:ok, _} = DatabasesRuntime.Supervisor.start_replication(sup, pg_replication, test_pid: self())

      %{
        pg_replication: pg_replication,
        source_db: source_db,
        event_consumer: event_consumer,
        record_consumer: record_consumer
      }
    end

    test "inserts are replicated to consumer events", %{event_consumer: consumer} do
      # Insert a character
      character = CharacterFactory.insert_character!([], repo: UnboxedRepo)

      # Wait for the message to be handled
      assert_receive {SlotProcessor, :flush_messages}, 500

      # Fetch consumer events
      [consumer_event] = Consumers.list_consumer_events_for_consumer(consumer.id)

      # Assert the consumer event details
      assert consumer_event.consumer_id == consumer.id
      assert consumer_event.table_oid == Character.table_oid()
      assert consumer_event.record_pks == [to_string(character.id)]
      %{data: data} = consumer_event

      assert_maps_equal(data.record, Map.from_struct(character), ["id", "name", "house", "planet", "is_active", "tags"],
        indifferent_keys: true
      )

      assert is_nil(data.changes)
      assert data.action == :insert
      assert_maps_equal(data.metadata, %{table_name: "Characters", table_schema: "public"}, [:table_name, :table_schema])
      assert is_struct(data.metadata.commit_timestamp, DateTime)
    end

    test "inserts are replicated to consumer records", %{record_consumer: consumer} do
      # Insert a character
      character = CharacterFactory.insert_character!([], repo: UnboxedRepo)

      # Wait for the message to be handled
      assert_receive {SlotProcessor, :flush_messages}, 500

      # Fetch consumer records
      [consumer_record] = Consumers.list_consumer_records_for_consumer(consumer.id)

      # Assert the consumer record details
      assert consumer_record.consumer_id == consumer.id
      assert consumer_record.table_oid == Character.table_oid()
      assert consumer_record.record_pks == [to_string(character.id)]
      assert consumer_record.group_id == to_string(character.id)
    end

    test "updates are replicated to consumer events when replica identity default", %{event_consumer: consumer} do
      # Insert a character
      character =
        CharacterFactory.insert_character!([name: "Leto Atreides", house: "Atreides", planet: "Caladan"],
          repo: UnboxedRepo
        )

      # Wait for the message to be handled
      assert_receive {SlotProcessor, :flush_messages}, 500

      # Update the character
      UnboxedRepo.update!(Ecto.Changeset.change(character, planet: "Arrakis"))

      # Wait for the update message to be handled
      assert_receive {SlotProcessor, :flush_messages}, 500

      # Fetch consumer events
      [_insert_event, update_event] =
        Consumers.list_consumer_events_for_consumer(consumer.id, order_by: [asc: :inserted_at])

      # Assert the consumer event details
      assert update_event.consumer_id == consumer.id
      assert update_event.table_oid == Character.table_oid()
      assert update_event.record_pks == [to_string(character.id)]
      %{data: data} = update_event

      character = Repo.reload(character)

      assert data.record == %{
               "id" => character.id,
               "name" => character.name,
               "house" => character.house,
               "planet" => "Arrakis",
               "is_active" => character.is_active,
               "tags" => character.tags,
               "metadata" => character.metadata,
               "inserted_at" => NaiveDateTime.to_iso8601(character.inserted_at),
               "updated_at" => NaiveDateTime.to_iso8601(character.updated_at)
             }

      assert data.changes == %{}
      assert data.action == :update
      assert_maps_equal(data.metadata, %{table_name: "Characters", table_schema: "public"}, [:table_name, :table_schema])
      assert is_struct(data.metadata.commit_timestamp, DateTime)
    end

    test "updates are replicated to consumer records when replica identity default", %{record_consumer: consumer} do
      # Insert a character
      character = CharacterFactory.insert_character!([], repo: UnboxedRepo)

      # Wait for the message to be handled
      assert_receive {SlotProcessor, :flush_messages}, 500

      [insert_record] = Consumers.list_consumer_records_for_consumer(consumer.id)

      # Update the character
      UnboxedRepo.update!(Ecto.Changeset.change(character, planet: "Arrakis"))

      # Wait for the update message to be handled
      assert_receive {SlotProcessor, :flush_messages}, 500

      # Fetch consumer records
      [update_record] = Consumers.list_consumer_records_for_consumer(consumer.id)

      # Assert the consumer record details
      assert update_record.consumer_id == consumer.id
      assert update_record.table_oid == Character.table_oid()
      assert update_record.record_pks == [to_string(character.id)]
      assert DateTime.compare(insert_record.inserted_at, update_record.inserted_at) == :eq
      refute DateTime.compare(insert_record.updated_at, update_record.updated_at) == :eq
    end

    test "deletes are replicated to consumer events when replica identity default", %{event_consumer: consumer} do
      character = CharacterFactory.insert_character!([], repo: UnboxedRepo)

      assert_receive {SlotProcessor, :flush_messages}, 500

      UnboxedRepo.delete!(character)
      assert_receive {SlotProcessor, :flush_messages}, 500

      [_insert_event, delete_event] = Consumers.list_consumer_events_for_consumer(consumer.id)

      %{data: data} = delete_event

      assert data.record == %{
               "house" => nil,
               "id" => character.id,
               "is_active" => nil,
               "name" => nil,
               "planet" => nil,
               "tags" => nil,
               "metadata" => nil,
               "inserted_at" => nil,
               "updated_at" => nil
             }

      assert data.changes == nil
      assert data.action == :delete
      assert_maps_equal(data.metadata, %{table_name: "Characters", table_schema: "public"}, [:table_name, :table_schema])
      assert is_struct(data.metadata.commit_timestamp, DateTime)
    end

    test "deletes are replicated to consumer records when replica identity default", %{record_consumer: consumer} do
      character = CharacterFactory.insert_character!([], repo: UnboxedRepo)

      assert_receive {SlotProcessor, :flush_messages}, 500

      [_insert_record] = Consumers.list_consumer_records_for_consumer(consumer.id)

      UnboxedRepo.delete!(character)
      assert_receive {SlotProcessor, :flush_messages}, 500

      [] = Consumers.list_consumer_records_for_consumer(consumer.id)
    end

    test "consumer with column filter only receives relevant messages", %{
      event_consumer: event_consumer,
      record_consumer: record_consumer
    } do
      # Randomly select a consumer
      consumer = Enum.random([event_consumer, record_consumer])

      # Create a consumer with a column filter
      sequence_filter =
        ConsumersFactory.sequence_filter_attrs(
          actions: [:insert, :update],
          column_filters: [
            ConsumersFactory.sequence_filter_column_filter_attrs(
              column_attnum: Character.column_attnum("is_active"),
              column_name: "is_active",
              operator: :==,
              value_type: :boolean,
              value: %{__type__: :boolean, value: true}
            )
          ]
        )

      consumer
      |> Ecto.Changeset.cast(%{sequence_filter: sequence_filter}, [])
      |> Ecto.Changeset.cast_embed(:sequence_filter, with: &SequenceFilter.create_changeset/2)
      |> Repo.update!()

      # Restart the consumer to apply the changes
      Consumers.update_consumer_with_lifecycle(consumer, %{})

      # Insert a character that doesn't match the filter
      CharacterFactory.insert_character!([is_active: false], repo: UnboxedRepo)

      # Wait for the message to be handled
      assert_receive {SlotProcessor, :flush_messages}, 500

      # Verify no consumer message was created
      assert list_messages(consumer) == []

      # Insert a character that matches the filter
      matching_character = CharacterFactory.insert_character!([is_active: true], repo: UnboxedRepo)

      # Wait for the message to be handled
      assert_receive {SlotProcessor, :flush_messages}, 500

      # Fetch consumer messages
      [consumer_message] = list_messages(consumer)

      # Assert the consumer message details
      assert consumer_message.consumer_id == consumer.id

      # Update the matching character (should not trigger a message)
      UnboxedRepo.update!(Ecto.Changeset.change(matching_character, is_active: false))

      # Wait for the message to be handled
      assert_receive {SlotProcessor, :flush_messages}, 500

      # Verify no new consumer message was created
      assert length(list_messages(consumer)) == 1
    end

    test "inserts are fanned out to both events and records", %{
      event_consumer: event_consumer,
      record_consumer: record_consumer
    } do
      # Insert a character
      CharacterFactory.insert_character!([], repo: UnboxedRepo)

      # Wait for the message to be handled
      assert_receive {SlotProcessor, :flush_messages}, 500

      # Fetch consumer events
      [consumer_event] = Consumers.list_consumer_events_for_consumer(event_consumer.id)

      # Fetch consumer records
      [consumer_record] = Consumers.list_consumer_records_for_consumer(record_consumer.id)

      # Assert both event and record were created
      assert consumer_event.consumer_id == event_consumer.id
      assert consumer_record.consumer_id == record_consumer.id

      # Assert both have the same data
      assert consumer_event.table_oid == consumer_record.table_oid
      assert consumer_event.record_pks == consumer_record.record_pks
    end

    test "empty array fields are replicated correctly", %{event_consumer: consumer} do
      # Insert a character with an empty array field
      character = CharacterFactory.insert_character!([tags: []], repo: UnboxedRepo)

      # Wait for the message to be handled
      assert_receive {SlotProcessor, :flush_messages}, 500

      # Fetch consumer events
      [consumer_event] = Consumers.list_consumer_events_for_consumer(consumer.id)

      # Assert the consumer event details
      assert consumer_event.consumer_id == consumer.id
      assert consumer_event.table_oid == Character.table_oid()
      assert consumer_event.record_pks == [to_string(character.id)]
      %{data: data} = consumer_event

      # Check that the tags field is an empty list, not [""]
      assert data.record["tags"] == [], "Expected empty array, got: #{inspect(data.record["tags"])}"
    end

    # Postgres quirk - the logical decoding process does not distinguish between an empty array and an array with an empty string.
    # https://chatgpt.com/share/6707334f-0978-8006-8358-ec2300d759a4
    test "array fields with empty string are returned as empty list", %{event_consumer: consumer} do
      # Insert a character with an array containing an empty string
      CharacterFactory.insert_character!([tags: [""]], repo: UnboxedRepo)

      # Wait for the message to be handled
      assert_receive {SlotProcessor, :flush_messages}, 500

      # Fetch consumer events
      [consumer_event] = Consumers.list_consumer_events_for_consumer(consumer.id)

      # Assert the consumer event details
      %{data: data} = consumer_event

      # Check that the tags field contains an empty string
      # Postgres quirk - the logical decoding slot will return `{}` for both `{}` and `{""}`.
      assert data.record["tags"] == [], "Expected array with empty string, got: #{inspect(data.record["tags"])}"
    end
  end

  describe "PostgresReplicationSlot end-to-end with sequences for characters_detailed" do
    setup do
      # Create source database
      account_id = AccountsFactory.insert_account!().id
      source_db = DatabasesFactory.insert_configured_postgres_database!(account_id: account_id)

      # Create PostgresReplicationSlot entity
      pg_replication =
        ReplicationFactory.insert_postgres_replication!(
          account_id: account_id,
          postgres_database_id: source_db.id,
          slot_name: replication_slot(),
          publication_name: @publication,
          status: :active
        )

      sequence =
        DatabasesFactory.insert_sequence!(
          account_id: account_id,
          postgres_database_id: source_db.id,
          table_oid: CharacterDetailed.table_oid()
        )

      # Create a consumer for this replication slot (record)
      consumer =
        ConsumersFactory.insert_sink_consumer!(
          message_kind: :event,
          replication_slot_id: pg_replication.id,
          account_id: account_id,
          source_tables: [],
          sequence_id: sequence.id,
          sequence_filter: ConsumersFactory.sequence_filter_attrs(column_filters: [])
        )

      # Start replication
      sup = Module.concat(__MODULE__, DatabasesRuntime.Supervisor)
      start_supervised!(Sequin.DynamicSupervisor.child_spec(name: sup))

      {:ok, _} = DatabasesRuntime.Supervisor.start_replication(sup, pg_replication, test_pid: self())

      %{
        source_db: source_db,
        consumer: consumer
      }
    end

    test "columns flow through properly", %{consumer: consumer} do
      # Insert a character with specific enum value
      character =
        CharacterFactory.insert_character_detailed!(
          [status: :retired],
          repo: UnboxedRepo
        )

      # Wait for the message to be handled
      assert_receive {SlotProcessor, :flush_messages}, 500

      # Fetch consumer records
      [message] = list_messages(consumer)

      # Assert the consumer record details
      assert message.consumer_id == consumer.id
      assert message.table_oid == CharacterDetailed.table_oid()
      assert message.record_pks == [to_string(character.id)]
      assert message.data.record["status"] == "retired"
    end

    test "consumer with JSONB column filter only receives relevant messages", %{
      consumer: consumer
    } do
      # Create a consumer with a JSONB column filter
      sequence_filter =
        ConsumersFactory.sequence_filter_attrs(
          actions: [:insert, :update],
          column_filters: [
            ConsumersFactory.sequence_filter_column_filter_attrs(
              column_attnum: CharacterDetailed.column_attnum("metadata"),
              column_name: "metadata",
              operator: :==,
              value_type: :string,
              value: %{__type__: :string, value: "good"},
              is_jsonb: true,
              jsonb_path: "alignment"
            )
          ]
        )

      # Update the consumer with the new sequence filter
      # we can't do this via the lifecycle because the sequence filter is not updated via the changeset
      Repo.update!(
        consumer
        |> Ecto.Changeset.cast(%{sequence_filter: sequence_filter}, [])
        |> Ecto.Changeset.cast_embed(:sequence_filter, with: &SequenceFilter.create_changeset/2)
      )

      # Restart the consumer to apply the changes
      {:ok, _} = Consumers.update_consumer_with_lifecycle(consumer, %{})

      {:ok, matching_character} =
        UnboxedRepo.transaction(fn ->
          # Insert a character that doesn't match the filter
          CharacterFactory.insert_character_detailed!(
            [metadata: %{"alignment" => "evil"}],
            repo: UnboxedRepo
          )

          # Insert a character with null metadata
          CharacterFactory.insert_character_detailed!(
            [metadata: nil],
            repo: UnboxedRepo
          )

          # Insert a character that matches the filter
          CharacterFactory.insert_character_detailed!(
            [metadata: %{"alignment" => "good"}],
            repo: UnboxedRepo
          )
        end)

      # Wait for the messages to be handled
      assert_receive {SlotProcessor, :flush_messages}, 500

      # Fetch consumer messages
      messages = list_messages(consumer)

      # Verify only one message was created (the matching one)
      assert length(messages) == 1
      [consumer_message] = messages

      # Assert the consumer message details
      assert consumer_message.consumer_id == consumer.id
      assert consumer_message.table_oid == CharacterDetailed.table_oid()
      assert consumer_message.record_pks == [to_string(matching_character.id)]
    end

    test "consumer with citext column filter matches case-insensitively", %{
      consumer: consumer
    } do
      # Create a consumer with a citext column filter
      sequence_filter =
        ConsumersFactory.sequence_filter_attrs(
          actions: [:insert],
          column_filters: [
            ConsumersFactory.sequence_filter_column_filter_attrs(
              column_attnum: CharacterDetailed.column_attnum("email"),
              column_name: "email",
              operator: :==,
              value_type: :cistring,
              value: %{__type__: :cistring, value: "TEST@EXAMPLE.COM"}
            )
          ]
        )

      # Update consumer with new filter
      Repo.update!(
        consumer
        |> Ecto.Changeset.cast(%{sequence_filter: sequence_filter}, [])
        |> Ecto.Changeset.cast_embed(:sequence_filter, with: &SequenceFilter.create_changeset/2)
      )

      # Restart the consumer to apply changes
      {:ok, _} = Consumers.update_consumer_with_lifecycle(consumer, %{})

      {:ok, matching_character} =
        UnboxedRepo.transaction(fn ->
          # Insert character with different case email
          CharacterFactory.insert_character_detailed!(
            [email: "test@example.com"],
            repo: UnboxedRepo
          )
        end)

      # Wait for message handling
      assert_receive {SlotProcessor, :flush_messages}, 500

      # Verify message was created and matched case-insensitively
      [consumer_message] = list_messages(consumer)
      assert consumer_message.consumer_id == consumer.id
      assert consumer_message.table_oid == CharacterDetailed.table_oid()
      assert consumer_message.record_pks == [to_string(matching_character.id)]
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
          id: server_id(),
          message_handler_module: MessageHandlerMock,
          message_handler_ctx: nil,
          postgres_database: %PostgresDatabase{id: "test_db_id"}
        ],
        opts
      )

    start_supervised!(SlotProcessor.child_spec(opts))
  end

  defp is_action(change, action) do
    is_struct(change, Message) and change.action == action
  end

  defp stop_replication! do
    stop_supervised!(server_via())
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
      if field1.column_name == "updated_at" and field2.column_name == "updated_at" do
        # updated_at timestamp may not be exactly the same
        NaiveDateTime.diff(field1.value, field2.value, :second) < 2
      else
        field1.column_name == field2.column_name && field1.value == field2.value
      end
    end)
  end

  defp list_messages(%{message_kind: :event} = consumer) do
    Consumers.list_consumer_events_for_consumer(consumer.id)
  end

  defp list_messages(%{message_kind: :record} = consumer) do
    Consumers.list_consumer_records_for_consumer(consumer.id)
  end
end

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
  alias Sequin.Databases.ConnectionCache
  alias Sequin.Databases.DatabaseUpdateWorker
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.CharacterFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Factory.TestEventLogFactory
  alias Sequin.Functions.TestMessages
  alias Sequin.Replication
  alias Sequin.Replication.PostgresReplicationSlot
  alias Sequin.Runtime
  alias Sequin.Runtime.MessageHandlerMock
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Runtime.SlotMessageStore
  alias Sequin.Runtime.SlotProcessor.Message
  alias Sequin.Runtime.SlotProcessorServer
  alias Sequin.Runtime.SlotProcessorSupervisor
  alias Sequin.Sinks.RedisMock
  alias Sequin.Test.UnboxedRepo
  alias Sequin.TestSupport.Models.Character
  alias Sequin.TestSupport.Models.CharacterDetailed
  alias Sequin.TestSupport.Models.CharacterIdentFull
  alias Sequin.TestSupport.Models.CharacterMultiPK
  alias Sequin.TestSupport.Models.TestEventLogPartitioned
  alias Sequin.TestSupport.ReplicationSlots
  alias Sequin.TestSupport.SimpleHttpServer

  @moduletag :unboxed

  @publication "characters_publication"

  def replication_slot, do: ReplicationSlots.slot_name(__MODULE__)

  setup do
    # Fast-forward the replication slot to the current WAL position
    :ok = ReplicationSlots.reset_slot(UnboxedRepo, replication_slot())
    TestMessages.create_ets_table()

    :ok
  end

  describe "PostgresReplicationSlot end-to-end" do
    setup do
      # Create source database
      account_id = AccountsFactory.insert_account!().id
      source_db = DatabasesFactory.insert_configured_postgres_database!(account_id: account_id, pg_major_version: 17)

      ConnectionCache.cache_connection(source_db, UnboxedRepo)

      # Create PostgresReplicationSlot entity
      pg_replication =
        ReplicationFactory.insert_postgres_replication!(
          postgres_database_id: source_db.id,
          slot_name: replication_slot(),
          publication_name: @publication,
          account_id: account_id,
          status: :active
        )

      # Create consumers for each table type (event)
      event_character_consumer =
        ConsumersFactory.insert_sink_consumer!(
          name: "event_character_consumer",
          message_kind: :event,
          status: :paused,
          replication_slot_id: pg_replication.id,
          account_id: account_id,
          source_tables: [
            ConsumersFactory.source_table_attrs(
              table_oid: Character.table_oid(),
              group_column_attnums: [Character.column_attnum("id")]
            )
          ]
        )

      event_character_ident_consumer =
        ConsumersFactory.insert_sink_consumer!(
          name: "event_character_ident_consumer",
          message_kind: :event,
          status: :paused,
          replication_slot_id: pg_replication.id,
          account_id: account_id,
          source_tables: [
            ConsumersFactory.source_table_attrs(
              table_oid: CharacterIdentFull.table_oid(),
              group_column_attnums: [CharacterIdentFull.column_attnum("id")]
            )
          ]
        )

      event_character_multi_pk_consumer =
        ConsumersFactory.insert_sink_consumer!(
          name: "event_character_multi_pk_consumer",
          message_kind: :event,
          status: :paused,
          replication_slot_id: pg_replication.id,
          account_id: account_id,
          source_tables: [
            ConsumersFactory.source_table_attrs(
              table_oid: CharacterMultiPK.table_oid(),
              group_column_attnums: [
                CharacterMultiPK.column_attnum("id_integer"),
                CharacterMultiPK.column_attnum("id_string"),
                CharacterMultiPK.column_attnum("id_uuid")
              ]
            )
          ]
        )

      # Create consumers for each table type (record)
      record_character_consumer =
        ConsumersFactory.insert_sink_consumer!(
          name: "record_character_consumer",
          message_kind: :record,
          status: :paused,
          replication_slot_id: pg_replication.id,
          account_id: account_id
        )

      record_character_ident_consumer =
        ConsumersFactory.insert_sink_consumer!(
          name: "record_character_ident_consumer",
          message_kind: :record,
          status: :paused,
          replication_slot_id: pg_replication.id,
          account_id: account_id
        )

      record_character_multi_pk_consumer =
        ConsumersFactory.insert_sink_consumer!(
          name: "record_character_multi_pk_consumer",
          message_kind: :record,
          status: :paused,
          replication_slot_id: pg_replication.id,
          account_id: account_id,
          source_tables: [
            ConsumersFactory.source_table_attrs(
              table_oid: CharacterMultiPK.table_oid(),
              group_column_attnums: [
                CharacterMultiPK.column_attnum("id_integer"),
                CharacterMultiPK.column_attnum("id_string"),
                CharacterMultiPK.column_attnum("id_uuid")
              ]
            )
          ]
        )

      http_endpoint =
        ConsumersFactory.insert_http_endpoint!(
          account_id: account_id,
          scheme: :http,
          host: Application.get_env(:sequin, :jepsen_http_host),
          port: Application.get_env(:sequin, :jepsen_http_port),
          path: "/",
          headers: %{"Content-Type" => "application/json"}
        )

      test_event_log_partitioned_consumer_http =
        ConsumersFactory.insert_sink_consumer!(
          account_id: account_id,
          type: :http_push,
          source: %{
            include_table_oids: [TestEventLogPartitioned.table_oid()]
          },
          sink: %{
            type: :http_push,
            http_endpoint_id: http_endpoint.id,
            http_endpoint: http_endpoint,
            batch: true
          },
          replication_slot_id: pg_replication.id,
          message_kind: :event,
          status: :active
        )

      test_event_log_partitioned_consumer =
        ConsumersFactory.insert_sink_consumer!(
          name: "test_event_log_partitioned_consumer",
          message_kind: :event,
          status: :paused,
          replication_slot_id: pg_replication.id,
          account_id: account_id,
          source_tables: [
            ConsumersFactory.source_table_attrs(
              table_oid: TestEventLogPartitioned.table_oid(),
              group_column_attnums: [TestEventLogPartitioned.column_attnum("id")]
            )
          ]
        )

      event_character_consumer = Repo.preload(event_character_consumer, :postgres_database)
      event_character_ident_consumer = Repo.preload(event_character_ident_consumer, :postgres_database)
      event_character_multi_pk_consumer = Repo.preload(event_character_multi_pk_consumer, :postgres_database)

      record_character_consumer = Repo.preload(record_character_consumer, :postgres_database)
      record_character_ident_consumer = Repo.preload(record_character_ident_consumer, :postgres_database)
      record_character_multi_pk_consumer = Repo.preload(record_character_multi_pk_consumer, :postgres_database)
      test_event_log_partitioned_consumer = Repo.preload(test_event_log_partitioned_consumer, :postgres_database)
      sup = Module.concat(__MODULE__, Runtime.Supervisor)
      start_supervised!(Sequin.DynamicSupervisor.child_spec(name: sup))
      {:ok, _} = Runtime.Supervisor.start_replication(sup, pg_replication, test_pid: self())

      %{
        sup: sup,
        pg_replication: pg_replication,
        source_db: source_db,
        event_character_consumer: event_character_consumer,
        event_character_ident_consumer: event_character_ident_consumer,
        event_character_multi_pk_consumer: event_character_multi_pk_consumer,
        record_character_consumer: record_character_consumer,
        record_character_ident_consumer: record_character_ident_consumer,
        record_character_multi_pk_consumer: record_character_multi_pk_consumer,
        test_event_log_partitioned_consumer: test_event_log_partitioned_consumer,
        test_event_log_partitioned_consumer_http: test_event_log_partitioned_consumer_http
      }
    end

    test "inserts are replicated to consumer events", %{event_character_consumer: consumer} do
      # Insert a character
      character = CharacterFactory.insert_character!([], repo: UnboxedRepo)

      # Wait for the message to be handled
      [consumer_event] = receive_messages(consumer, 1)

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
      [consumer_record] = receive_messages(consumer, 1)

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

      # Wait for the insert message to be handled
      await_messages(1)

      # Update the character
      UnboxedRepo.update!(Ecto.Changeset.change(character, planet: "Arrakis"))

      # Wait for the update message to be handled and fetch consumer events
      await_messages(1)
      events = list_messages(consumer)
      update_event = Enum.find(events, &(&1.data.action == :update))

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
               "inserted_at" => character.inserted_at,
               "updated_at" => character.updated_at
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
      await_messages(1)

      # Update the character
      UnboxedRepo.update!(Ecto.Changeset.change(character, planet: "Arrakis"))

      # Wait for the update message to be handled and fetch consumer records
      await_messages(1)
      records = list_messages(consumer)

      # Assert the consumer record details
      Enum.each(records, fn record ->
        assert record.consumer_id == consumer.id
        assert record.table_oid == Character.table_oid()
        assert record.record_pks == [to_string(character.id)]
      end)
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

      # Wait for the insert message to be handled
      await_messages(1)

      # Update the character
      UnboxedRepo.update!(
        Ecto.Changeset.change(character, %{
          house: "Emperor",
          planet: "Arrakis",
          is_active: false,
          tags: ["emperor", "kwisatz haderach"]
        })
      )

      # Wait for the update message to be handled and fetch consumer events
      await_messages(1)
      events = list_messages(consumer)
      update_event = Enum.find(events, &(&1.data.action == :update))

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

      await_messages(1)

      UnboxedRepo.delete!(character)

      await_messages(1)
      events = list_messages(consumer)
      delete_event = Enum.find(events, &(&1.data.action == :delete))

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

      await_messages(1)

      UnboxedRepo.delete!(character)

      await_messages(1)
      records = list_messages(consumer)
      assert length(records) == 1
      refute Enum.any?(records, & &1.deleted)
    end

    test "deletes are replicated to consumer events when replica identity full", %{
      event_character_ident_consumer: consumer
    } do
      character = CharacterFactory.insert_character_ident_full!([], repo: UnboxedRepo)

      await_messages(1)

      UnboxedRepo.delete!(character)

      await_messages(1)
      events = list_messages(consumer)
      delete_event = Enum.find(events, &(&1.data.action == :delete))

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

      [insert_message] = receive_messages(consumer, 1)

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

      source = ConsumersFactory.source_attrs(include_table_oids: [Character.table_oid()])
      {:ok, consumer} = Consumers.update_sink_consumer(consumer, %{actions: [:insert, :update], source: source})

      :ok = Runtime.Supervisor.refresh_message_handler_ctx(consumer.replication_slot_id)

      # Insert a character that doesn't match the filter
      CharacterFactory.insert_character_detailed!([], repo: UnboxedRepo)

      # Insert a character that matches the filter
      matching_character = CharacterFactory.insert_character!([], repo: UnboxedRepo)

      # Wait for the message to be handled and fetch consumer messages
      [consumer_message] = receive_messages(consumer, 2)

      # Assert the consumer message details
      assert consumer_message.consumer_id == consumer.id
      assert consumer_message.table_oid == Character.table_oid()
      assert consumer_message.record_pks == [to_string(matching_character.id)]

      UnboxedRepo.delete!(matching_character)

      # Wait for the message to be handled and fetch consumer messages, no new message should be created
      messages = receive_messages(consumer, 1)
      assert [^consumer_message] = messages
    end

    test "inserts are fanned out to both events and records", %{
      event_character_consumer: event_consumer,
      record_character_consumer: record_consumer
    } do
      # Insert a character
      CharacterFactory.insert_character!([], repo: UnboxedRepo)

      # Wait for the message to be handled and fetch consumer events and records
      await_messages(1)
      [event_message] = list_messages(event_consumer)
      [record_message] = list_messages(record_consumer)

      # Assert both event and record were created
      assert event_message.consumer_id == event_consumer.id
      assert record_message.consumer_id == record_consumer.id

      # Assert both have the same data
      assert event_message.table_oid == record_message.table_oid
      assert event_message.record_pks == record_message.record_pks
    end

    test "empty array fields are replicated correctly", %{event_character_consumer: consumer} do
      # Insert a character with an empty array field
      character = CharacterFactory.insert_character!([tags: []], repo: UnboxedRepo)

      # Wait for the message to be handled and fetch consumer events
      [consumer_event] = receive_messages(consumer, 1)

      # Assert the consumer event details
      assert consumer_event.consumer_id == consumer.id
      assert consumer_event.table_oid == Character.table_oid()
      assert consumer_event.record_pks == [to_string(character.id)]
      %{data: data} = consumer_event

      # Check that the tags field is an empty list, not [""]
      assert data.record["tags"] == [], "Expected empty array, got: #{inspect(data.record["tags"])}"
    end

    test "transaction annotations are propagated correctly", %{event_character_consumer: consumer} do
      # Insert two characters in the same transaction with annotations
      {:ok, {character1, character2}} =
        UnboxedRepo.transaction(fn ->
          # Set initial transaction annotations
          {:ok, _} =
            UnboxedRepo.query(
              ~s|select pg_logical_emit_message(true, 'sequin:transaction_annotations.set', '{ "username": "yahya" }')|
            )

          c1 = CharacterFactory.insert_character!([name: "Paul"], repo: UnboxedRepo)
          c2 = CharacterFactory.insert_character!([name: "Leto"], repo: UnboxedRepo)
          {c1, c2}
        end)

      # Wait for the annotated messages to be handled
      await_messages(2)

      # Insert a character without annotations
      character3 = CharacterFactory.insert_character!([name: "Duncan"], repo: UnboxedRepo)

      # Wait for the message to be handled
      await_messages(1)

      # Insert final character with new annotations
      {:ok, character4} =
        UnboxedRepo.transaction(fn ->
          # Set new annotations
          {:ok, _} =
            UnboxedRepo.query(
              ~s|select pg_logical_emit_message(true, 'sequin:transaction_annotations.set', '{ "spice": "flow" }')|
            )

          CharacterFactory.insert_character!([name: "Chani"], repo: UnboxedRepo)
        end)

      # Wait for final messages to be handled
      await_messages(1)

      # Fetch all consumer events
      events = list_messages(consumer)

      # Find events for each character
      event1 = Enum.find(events, &(hd(&1.record_pks) == to_string(character1.id)))
      event2 = Enum.find(events, &(hd(&1.record_pks) == to_string(character2.id)))
      event3 = Enum.find(events, &(hd(&1.record_pks) == to_string(character3.id)))
      event4 = Enum.find(events, &(hd(&1.record_pks) == to_string(character4.id)))

      # First two events should have the same annotations
      assert event1.data.metadata.transaction_annotations == %{"username" => "yahya"}
      assert event2.data.metadata.transaction_annotations == %{"username" => "yahya"}

      # Third event should have no annotations
      assert event3.data.metadata.transaction_annotations == nil

      # Fourth event should have new annotations
      assert event4.data.metadata.transaction_annotations == %{"spice" => "flow"}
    end

    @tag capture_log: true
    test "invalid transaction annotations are ignored", %{event_character_consumer: consumer} do
      # Insert a character with invalid JSON annotations
      {:ok, character1} =
        UnboxedRepo.transaction(fn ->
          # Set invalid JSON as transaction annotations
          {:ok, _} =
            UnboxedRepo.query(
              "select pg_logical_emit_message(true, 'sequin:transaction_annotations.set', '{ invalid json }')"
            )

          CharacterFactory.insert_character!([name: "Paul"], repo: UnboxedRepo)
        end)

      # Wait for the message to be handled
      await_messages(1)

      # Insert another character with valid annotations
      {:ok, character2} =
        UnboxedRepo.transaction(fn ->
          # Set valid annotations
          {:ok, _} =
            UnboxedRepo.query(
              ~s|select pg_logical_emit_message(true, 'sequin:transaction_annotations.set', '{ "username": "leto" }')|
            )

          CharacterFactory.insert_character!([name: "Leto"], repo: UnboxedRepo)
        end)

      # Wait for the message to be handled
      await_messages(1)

      # Fetch all consumer events
      events = list_messages(consumer)

      # Find events for each character
      event1 = Enum.find(events, &(hd(&1.record_pks) == to_string(character1.id)))
      event2 = Enum.find(events, &(hd(&1.record_pks) == to_string(character2.id)))

      # First event should have no annotations due to parse error
      assert event1.data.metadata.transaction_annotations == nil

      # Second event should have valid annotations
      assert event2.data.metadata.transaction_annotations == %{"username" => "leto"}
    end

    # Postgres quirk - the logical decoding process does not distinguish between an empty array and an array with an empty string.
    # https://chatgpt.com/share/6707334f-0978-8006-8358-ec2300d759a4
    test "array fields with empty string are returned as empty list", %{event_character_consumer: consumer} do
      # Insert a character with an array containing an empty string
      CharacterFactory.insert_character!([tags: [""]], repo: UnboxedRepo)

      # Wait for the message to be handled and fetch consumer events
      [consumer_event] = receive_messages(consumer, 1)

      # Assert the consumer event details
      %{data: data} = consumer_event

      # Check that the tags field contains an empty string
      # Postgres quirk - the logical decoding slot will return `{}` for both `{}` and `{""}`.
      assert data.record["tags"] == [], "Expected array with empty string, got: #{inspect(data.record["tags"])}"
    end

    test "array fields are updated correctly from non-empty to empty", %{event_character_ident_consumer: consumer} do
      # Insert a character with a non-empty array field
      character = CharacterFactory.insert_character_ident_full!([tags: ["tag1", "tag2"]], repo: UnboxedRepo)

      await_messages(1)

      # Update the character with an empty array
      UnboxedRepo.update!(Ecto.Changeset.change(character, tags: []))

      # Wait for the message to be handled and fetch consumer events
      await_messages(1)
      events = list_messages(consumer)
      update_event = Enum.find(events, &(&1.data.action == :update))

      # Assert the consumer event details
      %{data: data} = update_event

      # Check that the changes field shows the previous non-empty array
      assert data.changes["tags"] == ["tag1", "tag2"],
             "Expected non-empty array in changes, got: #{inspect(data.changes["tags"])}"

      # Check that the record field shows the new empty array
      assert data.record["tags"] == [], "Expected empty array in record, got: #{inspect(data.record["tags"])}"
    end

    test "changes to partitioned tables are replicated", %{test_event_log_partitioned_consumer: consumer} do
      # Insert a record into the partitioned table
      TestEventLogFactory.insert_test_event_log_partitioned!(
        [
          seq: 1,
          source_table_name: "test_table",
          action: "insert",
          record: %{"field1" => "test value"}
        ],
        repo: UnboxedRepo
      )

      # Wait for the message to be handled and fetch consumer events
      [consumer_event] = receive_messages(consumer, 1)

      # Assert the consumer event details
      assert consumer_event.consumer_id == consumer.id
      assert consumer_event.table_oid == TestEventLogPartitioned.table_oid()
    end

    @tag :jepsen
    @tag capture_log: true
    test "batch updates are delivered in sequence order via HTTP", %{sup: sup} do
      {:ok, pid} = SimpleHttpServer.start_link(%{caller: self()})

      on_exit(fn ->
        try do
          GenServer.stop(pid)
        catch
          _, _ -> :ok
        end
      end)

      ref = make_ref()
      initial_seq = 0
      event = [seq: initial_seq, source_table_schema: inspect(ref)]
      event = TestEventLogFactory.insert_test_event_log_partitioned!(event, repo: UnboxedRepo)

      transactions_count = Application.get_env(:sequin, :jepsen_transactions_count)
      transaction_queries_count = Application.get_env(:sequin, :jepsen_transaction_queries_count)

      Enum.reduce(1..transactions_count, initial_seq, fn _, seq ->
        {events, seq} =
          Enum.reduce(1..transaction_queries_count, {[], seq}, fn _, {acc, seq} ->
            seq = seq + 1
            {[%{seq: seq} | acc], seq}
          end)

        TestEventLogFactory.update_test_event_log_partitioned!(event, Enum.reverse(events), repo: UnboxedRepo)
        seq
      end)

      assert wait_and_validate_data(inspect(ref), -1, transactions_count * transaction_queries_count)
      stop_supervised!(sup)
    end
  end

  describe "PostgresReplication end-to-end with http push" do
    setup do
      # Create source database
      account_id = AccountsFactory.insert_account!().id
      source_db = DatabasesFactory.insert_configured_postgres_database!(account_id: account_id, pg_major_version: 17)

      ConnectionCache.cache_connection(source_db, UnboxedRepo)

      # Create PostgresReplicationSlot entity
      pg_replication =
        ReplicationFactory.insert_postgres_replication!(
          postgres_database_id: source_db.id,
          slot_name: replication_slot(),
          publication_name: @publication,
          account_id: account_id,
          status: :active
        )

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          name: "consumer",
          type: :http_push,
          status: :active,
          partition_count: 1,
          replication_slot_id: pg_replication.id,
          account_id: account_id,
          source_tables: [
            ConsumersFactory.source_table_attrs(
              table_oid: Character.table_oid(),
              group_column_attnums: [Character.column_attnum("id")]
            )
          ]
        )

      consumer = Repo.preload(consumer, :postgres_database)

      %{consumer: consumer, pg_replication: pg_replication}
    end

    test "messages are successfully delivered to HTTP", %{consumer: consumer, pg_replication: pg_replication} do
      test_pid = self()

      adapter = fn %Req.Request{} = req ->
        send(test_pid, {:http_request, req})
        {req, Req.Response.new(status: 200)}
      end

      sup = Module.concat(__MODULE__, Runtime.Supervisor)
      start_supervised!(Sequin.DynamicSupervisor.child_spec(name: sup))

      {:ok, _} =
        Runtime.Supervisor.start_replication(sup, pg_replication, test_pid: test_pid, req_opts: [adapter: adapter])

      # Insert a character
      character = CharacterFactory.insert_character!([], repo: UnboxedRepo)

      # Wait for the message to be handled
      assert_receive {:http_request, req}, 500
      assert to_string(req.body) =~ "Characters"
      assert to_string(req.body) =~ "insert"
      assert to_string(req.body) =~ to_string(character.id)

      assert_receive {SinkPipeline, :ack_finished, [_ack_id], []}, 500

      assert [] == list_messages(consumer)
    end

    @tag capture_log: true
    test "failed messages are written to disk", %{consumer: consumer, pg_replication: pg_replication} do
      test_pid = self()

      adapter = fn %Req.Request{} = req ->
        send(test_pid, {:http_request, req})
        {req, Req.Response.new(status: 500)}
      end

      sup = Module.concat(__MODULE__, Runtime.Supervisor)
      start_supervised!(Sequin.DynamicSupervisor.child_spec(name: sup))

      {:ok, _} =
        Runtime.Supervisor.start_replication(sup, pg_replication, test_pid: test_pid, req_opts: [adapter: adapter])

      # Insert a character
      character = CharacterFactory.insert_character!([], repo: UnboxedRepo)

      # Wait for the message to be handled
      assert_receive {:http_request, req}, 500
      assert to_string(req.body) =~ "Characters"
      assert to_string(req.body) =~ "insert"
      assert to_string(req.body) =~ to_string(character.id)

      assert_receive {SinkPipeline, :ack_finished, [], [_ack_id]}, 500

      assert [failed_message] = list_messages(consumer)
      assert failed_message.deliver_count == 1
    end
  end

  describe "replication in isolation" do
    setup do
      # Create a real account and configured database for isolation tests
      account = AccountsFactory.insert_account!()

      postgres_database =
        DatabasesFactory.insert_configured_postgres_database!(
          account_id: account.id,
          pg_major_version: 17
        )

      ConnectionCache.cache_connection(postgres_database, UnboxedRepo)

      # Create a real PostgresReplicationSlot
      pg_replication =
        ReplicationFactory.insert_postgres_replication!(
          account_id: account.id,
          postgres_database_id: postgres_database.id,
          slot_name: replication_slot(),
          publication_name: @publication,
          status: :active
        )

      test_pid = self()

      stub(MessageHandlerMock, :before_handle_messages, fn _ctx, _msgs -> :ok end)
      stub(MessageHandlerMock, :put_high_watermark_wal_cursor, fn _id, _cursor -> :ok end)

      stub(MessageHandlerMock, :handle_messages, fn _ctx, msgs ->
        send(test_pid, {:changes, msgs})
        {:ok, 0}
      end)

      {:ok, %{postgres_database: postgres_database, pg_replication: pg_replication}}
    end

    test "changes are buffered in the WAL, even if the listener is not up", %{pg_replication: pg_replication} do
      record =
        []
        |> CharacterFactory.insert_character!(repo: UnboxedRepo)
        |> Sequin.Map.from_ecto()
        |> Sequin.Map.stringify_keys()

      start_replication!(message_handler_module: MessageHandlerMock, replication_slot_id: pg_replication.id)
      assert_receive {:changes, [change]}, :timer.seconds(5)

      assert action?(change, :insert), "Expected change to be an insert, got: #{inspect(change)}"

      assert fields_equal?(change.fields, record)

      assert change.table_name == "Characters"
      assert change.table_schema == "public"
    end

    test "changes in a transaction are buffered then delivered to message handler in order", %{
      pg_replication: pg_replication
    } do
      start_replication!(message_handler_module: MessageHandlerMock, replication_slot_id: pg_replication.id)

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
      assert insert1.commit_idx == 1
      assert insert2.commit_idx == 2
      assert insert3.commit_idx == 3

      assert action?(insert1, :insert)
      assert get_field_value(insert1.fields, "name") == "Paul Atreides"

      assert action?(insert2, :insert)
      assert get_field_value(insert2.fields, "name") == "Leto Atreides"

      assert action?(insert3, :insert)
      assert get_field_value(insert3.fields, "name") == "Chani"

      # Insert another character
      CharacterFactory.insert_character!([name: "Duncan Idaho"], repo: UnboxedRepo)

      assert_receive {:changes, [insert4]}, :timer.seconds(1)
      # commit_idx resets but seq should be higher than previous transaction
      assert insert4.commit_lsn > insert3.commit_lsn
      assert insert4.commit_idx == 0
    end

    @tag capture_log: true
    test "changes are delivered at least once", %{pg_replication: pg_replication} do
      test_pid = self()

      stub(MessageHandlerMock, :handle_messages, fn _ctx, msgs ->
        send(test_pid, {:change, msgs})
        raise "Simulated crash"
      end)

      start_replication!(message_handler_module: MessageHandlerMock, replication_slot_id: pg_replication.id)

      record =
        []
        |> CharacterFactory.insert_character!(repo: UnboxedRepo)
        |> Sequin.Map.from_ecto()
        |> Sequin.Map.stringify_keys()

      assert_receive {:change, _}, :timer.seconds(1)

      stop_replication!(pg_replication)

      stub(MessageHandlerMock, :handle_messages, fn _ctx, msgs ->
        send(test_pid, {:change, msgs})
        {:ok, length(msgs)}
      end)

      start_replication!(message_handler_module: MessageHandlerMock, replication_slot_id: pg_replication.id)

      assert_receive {:change, [change]}, :timer.seconds(1)
      assert action?(change, :insert)

      # Should have received the record (it was re-delivered)
      assert fields_equal?(change.fields, record)
    end

    test "creates, updates, and deletes are captured", %{pg_replication: pg_replication} do
      test_pid = self()

      stub(MessageHandlerMock, :handle_messages, fn _ctx, msgs ->
        send(test_pid, {:change, msgs})
        {:ok, length(msgs)}
      end)

      start_replication!(message_handler_module: MessageHandlerMock, replication_slot_id: pg_replication.id)

      # Test create
      character = CharacterFactory.insert_character_ident_full!([planet: "Caladan"], repo: UnboxedRepo)
      record = character |> Sequin.Map.from_ecto() |> Sequin.Map.stringify_keys()

      assert_receive {:change, [create_change]}, :timer.seconds(1)
      assert action?(create_change, :insert)
      assert is_integer(create_change.commit_lsn)
      assert create_change.commit_idx == 1

      assert fields_equal?(create_change.fields, record)
      assert create_change.action == :insert

      # Test update
      UnboxedRepo.update!(Ecto.Changeset.change(character, planet: "Arrakis"))
      record = Map.put(record, "planet", "Arrakis")

      assert_receive {:change, [update_change]}, :timer.seconds(1)
      assert action?(update_change, :update)
      assert update_change.commit_lsn > create_change.commit_lsn
      assert update_change.commit_idx == 0

      assert fields_equal?(update_change.fields, record)
      refute is_nil(update_change.old_fields)
      assert Enum.find(update_change.old_fields, &(&1.column_name == "planet")).value == "Caladan"
      assert update_change.action == :update

      # Test delete
      UnboxedRepo.delete!(character)

      assert_receive {:change, [delete_change]}, :timer.seconds(1)
      assert action?(delete_change, :delete)
      assert delete_change.commit_lsn > update_change.commit_lsn
      assert delete_change.commit_idx == 0

      assert fields_equal?(delete_change.old_fields, record)
      assert delete_change.action == :delete
    end

    @tag capture_log: true
    test "messages are processed exactly once, even after crash and reboot", %{pg_replication: pg_replication} do
      start_replication!(message_handler_module: MessageHandlerMock, replication_slot_id: pg_replication.id)

      # Insert a record
      character1 = CharacterFactory.insert_character!([], repo: UnboxedRepo)

      # Wait for the message to be handled
      assert_receive {:changes, [change]}, :timer.seconds(1)
      assert action?(change, :insert)
      assert get_field_value(change.fields, "id") == character1.id

      # Insert another record
      character2 = CharacterFactory.insert_character!([], repo: UnboxedRepo)

      # Wait for the message to be handled
      assert_receive {:changes, [change]}, :timer.seconds(1)
      assert action?(change, :insert)
      assert get_field_value(change.fields, "id") == character2.id

      # write the low watermark for character2
      Replication.put_restart_wal_cursor!(pg_replication.id, %{
        commit_lsn: change.commit_lsn,
        commit_idx: change.commit_idx
      })

      # Stop the replication - likely before the message was acked, but there is a race here
      stop_replication!(pg_replication)

      # Restart the replication
      start_replication!(message_handler_module: MessageHandlerMock, replication_slot_id: pg_replication.id)

      # Insert another record to verify replication is working
      character3 = CharacterFactory.insert_character!([], repo: UnboxedRepo)

      # Wait for the new message to be handled
      assert_receive {:changes, [change2, change3]}, :timer.seconds(1)

      # Verify we only get the records >= low watermark
      assert action?(change2, :insert)
      assert get_field_value(change2.fields, "id") == character2.id

      assert action?(change3, :insert)
      assert get_field_value(change3.fields, "id") == character3.id
    end

    @tag capture_log: true
    test "retries flushing when payload size limit exceeded", %{pg_replication: pg_replication} do
      test_pid = self()
      # First call succeeds, updating the last_flushed_wal_cursor
      stub(MessageHandlerMock, :handle_messages, fn _ctx, msgs ->
        {:ok, length(msgs)}
      end)

      # Start replication with our custom reconnect interval
      start_replication!(
        message_handler_module: MessageHandlerMock,
        reconnect_interval: 5,
        replication_slot_id: pg_replication.id,
        slot_producer: [slot_producer_opts: [batch_flush_interval: [max_age: 50]]]
      )

      # Insert a character to generate a message
      _character1 = CharacterFactory.insert_character!([], repo: UnboxedRepo)

      await_messages(1)

      # Second call will fail with payload_size_limit_exceeded
      stub(MessageHandlerMock, :handle_messages, fn _ctx, msgs ->
        if length(msgs) == 1 do
          send(test_pid, :sent_error)
        end

        # Return the error that should trigger disconnection
        {:error, Sequin.Error.invariant(code: :payload_size_limit_exceeded, message: "Payload size limit exceeded")}
      end)

      # Insert another character to trigger the payload size limit exceeded error
      character2 = CharacterFactory.insert_character!([], repo: UnboxedRepo)

      assert_receive :sent_error, 100

      # Last call succeeds
      expect(MessageHandlerMock, :handle_messages, fn _ctx, messages ->
        assert length(messages) == 1
        message = List.first(messages)
        send(test_pid, {:changes, [message]})
        {:ok, length(messages)}
      end)

      stub(MessageHandlerMock, :handle_messages, fn _ctx, messages ->
        {:ok, length(messages)}
      end)

      # Should process ONLY the second message
      assert_receive {:changes, [change]}, :timer.seconds(1)
      assert get_field_value(change.fields, "id") == character2.id
    end

    @tag capture_log: true
    test "fails to start when replication slot does not exist", %{pg_replication: pg_replication} do
      # Use a non-existent slot name
      non_existent_slot = "non_existent_slot"
      Replication.update_pg_replication(pg_replication, %{slot_name: non_existent_slot})

      # Attempt to start replication with the non-existent slot
      test_pid = self()
      on_connect_fail = fn _, _ -> send(test_pid, :connect_fail) end

      start_replication!(
        replication_slot_id: pg_replication.id,
        slot_producer: [slot_producer_opts: [on_connect_fail: on_connect_fail]]
      )

      assert_receive :connect_fail, 1000
    end

    test "emits heartbeat messages for latest postgres version", %{pg_replication: pg_replication} do
      # Attempt to start replication with the non-existent slot
      start_replication!(heartbeat_interval: 5, replication_slot_id: pg_replication.id)

      assert_receive {SlotProcessorServer, :heartbeat_received}, 1000
      assert_receive {SlotProcessorServer, :heartbeat_received}, 1000

      # Verify that the Health status was updated
      {:ok, health} =
        Sequin.Health.health(%PostgresReplicationSlot{id: pg_replication.id, inserted_at: DateTime.utc_now()})

      check = Enum.find(health.checks, &(&1.slug == :replication_messages))
      assert check.status == :healthy
    end

    test "emits heartbeat messages for older postgres version", %{pg_replication: pg_replication} do
      # Attempt to start replication with the non-existent slot
      start_replication!(heartbeat_interval: 5, replication_slot_id: pg_replication.id)

      assert_receive {SlotProcessorServer, :heartbeat_received}, 1000
      assert_receive {SlotProcessorServer, :heartbeat_received}, 1000

      # Verify that the Health status was updated
      {:ok, health} =
        Sequin.Health.health(%PostgresReplicationSlot{id: pg_replication.id, inserted_at: DateTime.utc_now()})

      check = Enum.find(health.checks, &(&1.slug == :replication_messages))
      assert check.status == :healthy
    end
  end

  describe "PostgresReplicationSlot end-to-end with sequences" do
    setup do
      # Create source database
      account_id = AccountsFactory.insert_account!().id
      source_db = DatabasesFactory.insert_configured_postgres_database!(account_id: account_id, pg_major_version: 17)

      ConnectionCache.cache_connection(source_db, UnboxedRepo)

      # Create PostgresReplicationSlot entity
      pg_replication =
        ReplicationFactory.insert_postgres_replication!(
          postgres_database_id: source_db.id,
          slot_name: replication_slot(),
          publication_name: @publication,
          account_id: account_id,
          status: :active
        )

      # Create a consumer for this replication slot (event)
      event_consumer =
        ConsumersFactory.insert_sink_consumer!(
          message_kind: :event,
          status: :paused,
          replication_slot_id: pg_replication.id,
          account_id: account_id,
          source_tables: [
            ConsumersFactory.source_table_attrs(
              table_oid: Character.table_oid(),
              group_column_attnums: [Character.column_attnum("id")]
            )
          ]
        )

      # Create a consumer for this replication slot (record)
      record_consumer =
        ConsumersFactory.insert_sink_consumer!(
          message_kind: :record,
          status: :paused,
          replication_slot_id: pg_replication.id,
          account_id: account_id,
          source_tables: [
            ConsumersFactory.source_table_attrs(
              table_oid: Character.table_oid(),
              group_column_attnums: [Character.column_attnum("id")]
            )
          ]
        )

      # Start replication
      sup = Module.concat(__MODULE__, Runtime.Supervisor)
      start_supervised!(Sequin.DynamicSupervisor.child_spec(name: sup))

      {:ok, _} = Runtime.Supervisor.start_replication(sup, pg_replication, test_pid: self())

      %{
        sup_name: sup,
        pg_replication: pg_replication,
        source_db: source_db,
        event_consumer: event_consumer,
        record_consumer: record_consumer
      }
    end

    test "inserts are replicated to consumer events", %{event_consumer: consumer} do
      # Insert a character
      character = CharacterFactory.insert_character!([], repo: UnboxedRepo)

      # Wait for the message to be handled and fetch consumer events
      [consumer_event] = receive_messages(consumer, 1)

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

      # Wait for the message to be handled and fetch consumer records
      [consumer_record] = receive_messages(consumer, 1)

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
      await_messages(1)
      # Update the character
      UnboxedRepo.update!(Ecto.Changeset.change(character, planet: "Arrakis"))

      # Wait for the update message to be handled and fetch consumer events
      await_messages(1)
      events = list_messages(consumer)
      update_event = Enum.find(events, &(&1.data.action == :update))

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
               "inserted_at" => character.inserted_at,
               "updated_at" => character.updated_at
             }

      assert data.changes == %{}
      assert data.action == :update
      assert_maps_equal(data.metadata, %{table_name: "Characters", table_schema: "public"}, [:table_name, :table_schema])
      assert is_struct(data.metadata.commit_timestamp, DateTime)
    end

    test "updates are replicated to consumer records when replica identity default", %{record_consumer: consumer} do
      # Insert a character
      character = CharacterFactory.insert_character!([], repo: UnboxedRepo)

      # Wait for the insert message to be handled
      await_messages(1)

      # Update the character
      UnboxedRepo.update!(Ecto.Changeset.change(character, planet: "Arrakis"))

      # Wait for the update message to be handled and fetch consumer records
      await_messages(1)
      records = list_messages(consumer)

      # Assert the consumer record details
      Enum.each(records, fn record ->
        assert record.consumer_id == consumer.id
        assert record.table_oid == Character.table_oid()
        assert record.record_pks == [to_string(character.id)]
      end)
    end

    test "deletes are replicated to consumer events when replica identity default", %{event_consumer: consumer} do
      character = CharacterFactory.insert_character!([], repo: UnboxedRepo)

      await_messages(1)

      UnboxedRepo.delete!(character)

      await_messages(1)
      events = list_messages(consumer)
      delete_event = Enum.find(events, &(&1.data.action == :delete))

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

    test "deletes are rejected from consumer records when replica identity default", %{record_consumer: consumer} do
      character = CharacterFactory.insert_character!([], repo: UnboxedRepo)

      await_messages(1)

      [_insert_record] = list_messages(consumer)

      UnboxedRepo.delete!(character)
      await_messages(1)

      records = list_messages(consumer)
      refute Enum.any?(records, & &1.deleted)
    end

    test "consumer fans in events/records from multiple tables", %{
      event_consumer: event_consumer,
      record_consumer: record_consumer
    } do
      # Randomly select a consumer
      consumer = Enum.random([event_consumer, record_consumer])

      # Attach a schema filter to the consumer
      Consumers.update_sink_consumer(consumer, %{source: nil}, skip_lifecycle: true)

      # Restart the consumer to apply the changes
      Consumers.update_sink_consumer(consumer, %{})
      Runtime.Supervisor.refresh_message_handler_ctx(consumer.replication_slot_id)

      # Verify no consumer messages yet
      assert list_messages(consumer) == []

      # Insert characters in two different tables and wait for each to be flushed
      matching_character = CharacterFactory.insert_character!([], repo: UnboxedRepo)
      await_messages(1)

      matching_character_detailed = CharacterFactory.insert_character_detailed!([], repo: UnboxedRepo)
      await_messages(1)

      # Fetch consumer messages
      messages = list_messages(consumer)
      assert length(messages) == 2
      [consumer_message1, consumer_message2] = messages

      # Assert the consumer message details
      assert consumer_message1.consumer_id == consumer.id
      assert consumer_message2.consumer_id == consumer.id

      assert consumer_message1.table_oid == Character.table_oid()
      assert consumer_message2.table_oid == CharacterDetailed.table_oid()

      assert consumer_message1.record_pks == [to_string(matching_character.id)]
      assert consumer_message2.record_pks == [to_string(matching_character_detailed.id)]
    end

    test "inserts are fanned out to both events and records", %{
      event_consumer: event_consumer,
      record_consumer: record_consumer
    } do
      # Insert a character
      CharacterFactory.insert_character!([], repo: UnboxedRepo)
      await_messages(1)

      # Wait for the message to be handled and fetch consumer events and records
      [consumer_event] = list_messages(event_consumer)
      [consumer_record] = list_messages(record_consumer)

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

      # Wait for the message to be handled and fetch consumer events
      [consumer_event] = receive_messages(consumer, 1)

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

      # Wait for the message to be handled and fetch consumer events
      [consumer_event] = receive_messages(consumer, 1)

      # Assert the consumer event details
      %{data: data} = consumer_event

      # Check that the tags field contains an empty string
      # Postgres quirk - the logical decoding slot will return `{}` for both `{}` and `{""}`.
      assert data.record["tags"] == [], "Expected array with empty string, got: #{inspect(data.record["tags"])}"
    end

    @tag capture_log: true
    test "schema changes are detected and database update worker is enqueued", %{
      pg_replication: pg_replication,
      sup_name: sup,
      source_db: source_db
    } do
      on_exit(fn ->
        UnboxedRepo.query!(
          "alter table #{Character.quoted_table_name()} drop column if exists test_column_for_schema_change",
          []
        )
      end)

      do_migration = fn ->
        UnboxedRepo.query!(
          "alter table #{Character.quoted_table_name()} drop column if exists test_column_for_schema_change",
          []
        )

        UnboxedRepo.query!(
          "alter table #{Character.quoted_table_name()} add column if not exists test_column_for_schema_change text",
          []
        )
      end

      do_migration.()

      # Insert a record to trigger WAL processing
      CharacterFactory.insert_character!([], repo: UnboxedRepo)

      await_messages(1)

      # Verify that DatabaseUpdateWorker was enqueued with the correct args
      assert_enqueued(worker: DatabaseUpdateWorker, args: %{postgres_database_id: source_db.id})

      GenServer.stop(sup)

      do_migration.()

      {:ok, _} = Runtime.Supervisor.start_replication(sup, pg_replication, test_pid: self())

      await_messages(1)

      # Verify that DatabaseUpdateWorker was enqueued with the correct args
      # twice now
      [_, _] = all_enqueued(worker: DatabaseUpdateWorker, args: %{postgres_database_id: source_db.id})
    end
  end

  describe "PostgresReplicationSlot end-to-end with sequences for characters_detailed" do
    setup do
      # Create source database
      account_id = AccountsFactory.insert_account!().id
      source_db = DatabasesFactory.insert_configured_postgres_database!(account_id: account_id, pg_major_version: 17)

      ConnectionCache.cache_connection(source_db, UnboxedRepo)

      Mox.stub(RedisMock, :send_messages, fn _sink, _redis_messages ->
        :ok
      end)

      # Create PostgresReplicationSlot entity
      pg_replication =
        ReplicationFactory.insert_postgres_replication!(
          account_id: account_id,
          postgres_database_id: source_db.id,
          slot_name: replication_slot(),
          publication_name: @publication,
          status: :active
        )

      # Create a consumer for this replication slot (record)
      consumer =
        ConsumersFactory.insert_sink_consumer!(
          message_kind: :event,
          status: :paused,
          replication_slot_id: pg_replication.id,
          account_id: account_id,
          source_tables: [
            ConsumersFactory.source_table_attrs(
              table_oid: CharacterDetailed.table_oid(),
              group_column_attnums: [CharacterDetailed.column_attnum("id")]
            )
          ]
        )

      # Start replication
      sup = Module.concat(__MODULE__, Runtime.Supervisor)
      start_supervised!(Sequin.DynamicSupervisor.child_spec(name: sup))

      {:ok, _} = Runtime.Supervisor.start_replication(sup, pg_replication, test_pid: self())

      %{
        source_db: source_db,
        consumer: consumer
      }
    end

    test "columns flow through properly", %{consumer: consumer} do
      # Insert a character with specific enum value, daterange and domain type
      character =
        CharacterFactory.insert_character_detailed!(
          [
            status: :retired,
            active_period: [~D[2010-01-01], ~D[2020-12-31]],
            # It's over 9000!
            power_level: 9001,
            embedding: [1.0, 2.0, 3.0]
          ],
          repo: UnboxedRepo
        )

      # Fetch consumer records
      [message] = receive_messages(consumer, 1)

      # Assert the consumer record details
      assert message.consumer_id == consumer.id
      assert message.table_oid == CharacterDetailed.table_oid()
      assert message.record_pks == [to_string(character.id)]
      assert message.data.record["status"] == "retired"
      assert message.data.record["active_period"] == "[2010-01-01,2020-12-31)"
      assert message.data.record["power_level"] == 9001
      assert message.data.record["embedding"] == [1.0, 2.0, 3.0]
    end
  end

  defp start_replication!(opts) do
    replication_slot_id = Keyword.get(opts, :replication_slot_id, "test_slot_id")

    # Merge supervisor options
    supervisor_opts =
      Keyword.merge(
        [
          replication_slot_id: replication_slot_id,
          message_handler_module: MessageHandlerMock,
          test_pid: self()
        ],
        opts
      )

    start_supervised!({SlotProcessorSupervisor, supervisor_opts})
  end

  defp action?(change, action) do
    is_struct(change, Message) and change.action == action
  end

  defp stop_replication!(pg_replication) do
    # Stop the supervisor using its via_tuple
    stop_supervised!(SlotProcessorSupervisor.via_tuple(pg_replication.id))
  end

  # defp config do
  #   :sequin
  #   |> Application.get_env(Sequin.Repo)
  #   |> Keyword.take([:username, :password, :hostname, :database, :port])
  # end

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

  defp await_messages(count, acc_count \\ 0) do
    assert_receive {SlotProcessorServer, :flush_messages, flush_count}, 1_000
    acc_count = acc_count + flush_count

    case count - acc_count do
      0 -> :ok
      total when total < 0 -> flunk("Expected #{count} messages but got #{acc_count}")
      _ -> await_messages(count, acc_count)
    end
  rescue
    err ->
      case err do
        %ExUnit.AssertionError{message: "Assertion failed, no matching message after" <> _rest} ->
          flunk("Did not receive remaining #{count - acc_count} messages")

        err ->
          reraise err, __STACKTRACE__
      end
  end

  defp receive_messages(consumer, count) do
    await_messages(count)

    list_messages(consumer)
  end

  defp list_messages(consumer) do
    SlotMessageStore.peek_messages(consumer, 1000)
  end

  @spec wait_and_validate_data(binary(), non_neg_integer(), non_neg_integer()) :: any()
  defp wait_and_validate_data(_, _, 0), do: true

  defp wait_and_validate_data(ref, prev_seq, expected_count) do
    receive do
      %{"action" => "update", "record" => %{"source_table_schema" => ^ref, "seq" => current_seq}} ->
        if current_seq > prev_seq,
          do: wait_and_validate_data(ref, current_seq, expected_count - 1),
          else: flunk("Received message with seq #{current_seq} which is less than prev_seq #{prev_seq}")

      _ ->
        wait_and_validate_data(ref, prev_seq, expected_count)
    after
      5_000 ->
        flunk("Did not receive :simple_http_server_loaded within 5 seconds")
    end
  end
end

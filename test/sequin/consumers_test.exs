defmodule Sequin.ConsumersTest do
  use Sequin.DataCase, async: true

  alias Sequin.Consumers
  alias Sequin.Consumers.AcknowledgedMessages
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Consumers.SequenceFilter
  alias Sequin.Consumers.SequenceFilter.BooleanValue
  alias Sequin.Consumers.SequenceFilter.DateTimeValue
  alias Sequin.Consumers.SequenceFilter.ListValue
  alias Sequin.Consumers.SequenceFilter.NullValue
  alias Sequin.Consumers.SequenceFilter.NumberValue
  alias Sequin.Consumers.SequenceFilter.StringValue
  alias Sequin.Databases
  alias Sequin.Databases.ConnectionCache
  alias Sequin.Databases.DatabaseUpdateWorker
  alias Sequin.Databases.Sequence
  alias Sequin.Error.NotFoundError
  alias Sequin.Factory
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.CharacterFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Test.UnboxedRepo
  alias Sequin.TestSupport.Models.Character
  alias Sequin.TestSupport.Models.CharacterDetailed
  alias Sequin.TestSupport.Models.CharacterMultiPK

  describe "ack_messages/2" do
    setup do
      consumer = ConsumersFactory.insert_sink_consumer!()
      {:ok, consumer: consumer}
    end

    test "acknowledges records" do
      consumer = ConsumersFactory.insert_sink_consumer!(message_kind: :record)

      records =
        for _ <- 1..3 do
          ConsumersFactory.insert_consumer_record!(
            consumer_id: consumer.id,
            state: :delivered
          )
        end

      ack_ids = Enum.map(records, & &1.ack_id)

      assert {:ok, 3} = Consumers.ack_messages(consumer, ack_ids)

      assert Repo.all(ConsumerRecord) == []
    end

    test "acknowledges events" do
      consumer = ConsumersFactory.insert_sink_consumer!(message_kind: :event)

      events =
        for _ <- 1..3 do
          ConsumersFactory.insert_consumer_event!(
            consumer_id: consumer.id,
            not_visible_until: DateTime.utc_now()
          )
        end

      ack_ids = Enum.map(events, & &1.ack_id)

      assert {:ok, 3} = Consumers.ack_messages(consumer, ack_ids)

      assert Repo.all(ConsumerEvent) == []
    end

    test "silently ignores non-existent ack_ids" do
      consumer = ConsumersFactory.insert_sink_consumer!(message_kind: :record)
      valid_record = ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, state: :delivered)
      non_existent_ack_id = UUID.uuid4()

      assert {:ok, 1} = Consumers.ack_messages(consumer, [valid_record.ack_id, non_existent_ack_id])
      assert Repo.all(ConsumerRecord) == []
    end

    test "handles empty ack_ids list", %{consumer: consumer} do
      assert {:ok, 0} = Consumers.ack_messages(consumer, [])
    end

    test "acknowledges only records/events for the given consumer" do
      consumer = ConsumersFactory.insert_sink_consumer!(message_kind: :record)
      other_consumer = ConsumersFactory.insert_sink_consumer!(max_ack_pending: 100, message_kind: :record)

      record1 = ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, state: :delivered)
      record2 = ConsumersFactory.insert_consumer_record!(consumer_id: other_consumer.id, state: :delivered)

      assert {:ok, 1} = Consumers.ack_messages(consumer, [record1.ack_id, record2.ack_id])

      assert [ignore] = Repo.all(ConsumerRecord)
      assert ignore.id == record2.id
    end

    test "acknowledged messages are stored in redis" do
      consumer = ConsumersFactory.insert_sink_consumer!(message_kind: :record)

      record1 = ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, state: :delivered)
      record2 = ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, state: :delivered)

      assert {:ok, 2} = Consumers.ack_messages(consumer, [record1.ack_id, record2.ack_id])

      assert {:ok, messages} = AcknowledgedMessages.fetch_messages(consumer.id)
      assert length(messages) == 2
      assert Enum.all?(messages, &(&1.consumer_id == consumer.id))
    end
  end

  describe "put_source_data/3" do
    setup do
      database = DatabasesFactory.insert_configured_postgres_database!(tables: [])
      # Load tables from actual database
      {:ok, _tables} = Databases.tables(database)

      slot =
        ReplicationFactory.insert_postgres_replication!(
          postgres_database_id: database.id,
          account_id: database.account_id
        )

      source_tables = [
        ConsumersFactory.source_table(
          oid: Character.table_oid(),
          column_filters: []
        ),
        ConsumersFactory.source_table(
          oid: CharacterDetailed.table_oid(),
          column_filters: []
        ),
        ConsumersFactory.source_table(
          oid: CharacterMultiPK.table_oid(),
          column_filters: []
        )
      ]

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          message_kind: :record,
          account_id: database.account_id,
          replication_slot_id: slot.id,
          source_tables: source_tables
        )

      consumer = Repo.preload(consumer, :postgres_database)

      ConnectionCache.cache_connection(consumer.postgres_database, Repo)

      {:ok, consumer: consumer}
    end

    test "fetches multiple records from the same table with single PK", %{consumer: consumer} do
      characters = for _ <- 1..3, do: CharacterFactory.insert_character!()
      _other_characters = for _ <- 1..3, do: CharacterFactory.insert_character!()
      records = Enum.map(characters, &build_consumer_record(consumer, &1))

      {:ok, fetched_records} = Consumers.put_source_data(consumer, records)
      fetched_records = Enum.sort_by(fetched_records, & &1.data.record["id"])

      assert length(fetched_records) == 3

      for {record, character} <- Enum.zip(fetched_records, characters) do
        assert_maps_equal(
          record.data.record,
          Map.from_struct(character),
          ["id", "name", "house", "planet", "is_active", "tags"],
          indifferent_keys: true
        )

        assert record.data.metadata.table_name == "Characters"
        assert record.data.metadata.table_schema == "public"
      end
    end

    test "fetches multiple records from the same table with compound PK", %{consumer: consumer} do
      characters = for _ <- 1..3, do: CharacterFactory.insert_character_multi_pk!()
      _other_characters = for _ <- 1..3, do: CharacterFactory.insert_character_multi_pk!()
      records = Enum.map(characters, &build_consumer_record(consumer, &1, :multi_pk))

      {:ok, fetched_records} = Consumers.put_source_data(consumer, records)
      fetched_records = Enum.sort_by(fetched_records, & &1.data.record["id_integer"])

      assert length(fetched_records) == 3

      for {record, character} <- Enum.zip(fetched_records, characters) do
        assert_maps_equal(record.data.record, Map.from_struct(character), ["id_integer", "id_string", "id_uuid", "name"],
          indifferent_keys: true
        )

        assert record.data.metadata.table_name == "characters_multi_pk"
        assert record.data.metadata.table_schema == "public"
      end
    end

    test "handles different primary key data types", %{consumer: consumer} do
      character = CharacterFactory.insert_character_multi_pk!()
      record = build_consumer_record(consumer, character, :multi_pk)

      {:ok, [fetched_record]} = Consumers.put_source_data(consumer, [record])

      assert fetched_record.data.record["id_integer"] == character.id_integer
      assert fetched_record.data.record["id_string"] == character.id_string
      assert fetched_record.data.record["id_uuid"] == character.id_uuid
    end

    test "casts different column types appropriately", %{consumer: consumer} do
      character = CharacterFactory.insert_character_detailed!()
      record = build_consumer_record(consumer, character, :detailed)

      {:ok, [fetched_record]} = Consumers.put_source_data(consumer, [record])

      assert fetched_record.data.record["age"] == character.age
      assert fetched_record.data.record["height"] == character.height
      assert fetched_record.data.record["is_hero"] == character.is_hero
      assert fetched_record.data.record["birth_date"] == character.birth_date

      assert Time.truncate(fetched_record.data.record["last_seen"], :second) ==
               character.last_seen

      assert NaiveDateTime.truncate(fetched_record.data.record["inserted_at"], :second) ==
               NaiveDateTime.truncate(character.inserted_at, :second)

      assert NaiveDateTime.truncate(fetched_record.data.record["updated_at"], :second) ==
               NaiveDateTime.truncate(character.updated_at, :second)

      assert fetched_record.data.record["powers"] == character.powers
      assert fetched_record.data.record["metadata"] == character.metadata
      assert Decimal.equal?(fetched_record.data.record["rating"], character.rating)
      assert fetched_record.data.record["avatar"] == "\\x" <> Base.encode16(character.avatar, case: :lower)
    end

    @tag capture_log: true
    test "errors when the source table is not in PostgresDatabase", %{consumer: consumer} do
      character = CharacterFactory.insert_character!()
      record = build_consumer_record(consumer, character)
      record = %{record | table_oid: 999_999}

      assert {:error, %NotFoundError{}} = Consumers.put_source_data(consumer, [record])
    end

    test "errors when the source table is listed in PostgresDatabase but not in the database", %{
      consumer: consumer
    } do
      character = CharacterFactory.insert_character!()
      record = build_consumer_record(consumer, character)
      record = %{record | table_oid: 999_999}

      # Simulate the table being listed but not in the database
      column = DatabasesFactory.column(%{name: "id", type: "integer", is_pk?: true})

      fake_table =
        DatabasesFactory.table(%{oid: 999_999, name: "non_existent_table", schema: "public", columns: [column]})

      consumer = %{consumer | postgres_database: %{consumer.postgres_database | tables: [fake_table]}}

      assert {:error, %Postgrex.Error{postgres: %{code: :undefined_table}}} =
               Consumers.put_source_data(consumer, [record])
    end

    test "returns error when PostgresDatabase.tables lists a non-existent column", %{consumer: consumer} do
      character = CharacterFactory.insert_character!()
      record = build_consumer_record(consumer, character)

      # Add a non-existent column to the table definition
      fake_column = DatabasesFactory.column(%{name: "non_existent_column", type: "text", is_pk?: false})

      consumer =
        update_in(consumer.postgres_database.tables, fn tables ->
          Enum.map(tables, fn table ->
            if table.name == "Characters", do: %{table | columns: [fake_column | table.columns]}, else: table
          end)
        end)

      {:error, %Postgrex.Error{postgres: %{code: :undefined_column}}} = Consumers.put_source_data(consumer, [record])

      assert_enqueued(worker: DatabaseUpdateWorker, args: %{postgres_database_id: consumer.postgres_database.id})
    end

    @tag capture_log: true
    test "errors when the source database is unreachable", %{consumer: consumer} do
      character = CharacterFactory.insert_character!()
      record = build_consumer_record(consumer, character)
      config = Keyword.merge(UnboxedRepo.config(), hostname: "unreachable_host", queue_target: 25, queue_interval: 25)
      {:ok, conn} = Postgrex.start_link(config)
      ConnectionCache.cache_connection(consumer.postgres_database, conn)

      assert {:error, %DBConnection.ConnectionError{}} = Consumers.put_source_data(consumer, [record])

      GenServer.stop(conn)
    end

    test "deletes consumer records when they are missing from the source table", %{consumer: consumer} do
      # Insert characters into the database
      existing_character = CharacterFactory.insert_character!()
      deleted_record_id = Factory.unique_integer()

      # Create consumer records for both characters
      existing_record =
        ConsumersFactory.insert_consumer_record!(
          consumer_id: consumer.id,
          table_oid: Character.table_oid(),
          record_pks: [existing_character.id]
        )

      deleted_record =
        ConsumersFactory.insert_consumer_record!(
          consumer_id: consumer.id,
          table_oid: Character.table_oid(),
          record_pks: [deleted_record_id]
        )

      # Call put_source_data with both records
      records = [deleted_record, existing_record]

      {:ok, fetched_records} = Consumers.put_source_data(consumer, records)

      # Assert that only the existing record is returned
      assert length(fetched_records) == 2
      records_with_data = Enum.filter(fetched_records, &(&1.data.record != nil))
      assert length(records_with_data) == 1
      assert hd(records_with_data).data.record["id"] == existing_character.id
      assert hd(records_with_data).record_pks == [to_string(existing_character.id)]
    end
  end

  describe "matches_record?/3" do
    @table_oid 12_345
    setup do
      consumer =
        ConsumersFactory.sink_consumer(
          sequence: %Sequence{table_oid: @table_oid},
          sequence_filter: %SequenceFilter{
            actions: [:insert, :update, :delete],
            column_filters: [
              ConsumersFactory.sequence_filter_column_filter(
                column_attnum: 1,
                operator: :==,
                value: %StringValue{value: "test_value"}
              ),
              ConsumersFactory.sequence_filter_column_filter(
                column_attnum: 2,
                operator: :>,
                value: %NumberValue{value: 10}
              )
            ]
          }
        )

      {:ok, consumer: consumer}
    end

    test "matches when all column filters match", %{consumer: consumer} do
      record = %{
        1 => "test_value",
        2 => 15
      }

      assert Consumers.matches_record?(consumer, @table_oid, record)
    end

    test "does not match when any column filter doesn't match", %{consumer: consumer} do
      record1 = %{
        1 => "wrong_value",
        2 => 15
      }

      record2 = %{
        1 => "test_value",
        2 => 5
      }

      refute Consumers.matches_record?(consumer, @table_oid, record1)
      refute Consumers.matches_record?(consumer, @table_oid, record2)
    end

    test "matches when no column filters are present" do
      consumer =
        ConsumersFactory.sink_consumer(
          sequence: %Sequence{table_oid: @table_oid},
          sequence_filter: %SequenceFilter{
            actions: [:insert, :update, :delete],
            column_filters: []
          }
        )

      record = %{
        1 => "any_value",
        2 => 100
      }

      assert Consumers.matches_record?(consumer, @table_oid, record)
    end
  end

  describe "HttpEndpoint.url/1" do
    test "returns correct URL for standard endpoint" do
      http_endpoint =
        ConsumersFactory.insert_http_endpoint!(
          scheme: :https,
          host: "example.com",
          port: 8080,
          path: "/webhook",
          query: "param=value",
          fragment: "section"
        )

      assert HttpEndpoint.url(http_endpoint) == "https://example.com:8080/webhook?param=value#section"
    end

    test "returns correct URL when port is not specified" do
      http_endpoint =
        ConsumersFactory.insert_http_endpoint!(
          scheme: :https,
          host: "example.com",
          path: "/webhook"
        )

      assert HttpEndpoint.url(http_endpoint) == "https://example.com/webhook"
    end

    test "returns modified URL when using a local tunnel" do
      http_endpoint =
        ConsumersFactory.insert_http_endpoint!(
          scheme: :https,
          host: "example.com",
          path: "/webhook",
          use_local_tunnel: true
        )

      expected_url = "http://#{Application.fetch_env!(:sequin, :portal_hostname)}:#{http_endpoint.port}/webhook"
      assert HttpEndpoint.url(http_endpoint) == expected_url
    end
  end

  describe "create_http_endpoint_for_account/2" do
    test "creates a http_endpoint with valid attributes" do
      account = AccountsFactory.insert_account!()

      valid_attrs = %{
        name: "TestEndpoint",
        scheme: :https,
        host: "example.com",
        port: 443,
        path: "/webhook",
        headers: %{"Content-Type" => "application/json"},
        use_local_tunnel: false
      }

      assert {:ok, %HttpEndpoint{} = http_endpoint} = Consumers.create_http_endpoint(account.id, valid_attrs)
      assert http_endpoint.name == "TestEndpoint"
      assert http_endpoint.scheme == :https
      assert http_endpoint.host == "example.com"
      assert http_endpoint.port == 443
      assert http_endpoint.path == "/webhook"
      assert http_endpoint.headers == %{"Content-Type" => "application/json"}
      refute http_endpoint.use_local_tunnel
    end

    test "creates a http_endpoint with local tunnel" do
      account = AccountsFactory.insert_account!()

      valid_attrs = %{
        name: "my-endpoint",
        use_local_tunnel: true
      }

      assert {:ok, %HttpEndpoint{} = http_endpoint} = Consumers.create_http_endpoint(account.id, valid_attrs)
      assert http_endpoint.name == "my-endpoint"
      assert http_endpoint.use_local_tunnel
      assert http_endpoint.port
      refute http_endpoint.host
    end

    test "returns error changeset with invalid attributes" do
      account = AccountsFactory.insert_account!()

      invalid_attrs = %{
        name: nil,
        scheme: :invalid,
        host: "",
        port: -1,
        headers: "invalid"
      }

      assert {:error, %Ecto.Changeset{}} = Consumers.create_http_endpoint(account.id, invalid_attrs)
    end
  end

  describe "update_http_endpoint/2" do
    test "updates the http_endpoint with valid attributes" do
      http_endpoint = ConsumersFactory.insert_http_endpoint!()

      update_attrs = %{
        name: "update-endpoint",
        scheme: :http,
        host: "updated.example.com",
        port: 8080,
        path: "/updated",
        headers: %{"Authorization" => "Bearer token"},
        use_local_tunnel: false
      }

      assert {:ok, %HttpEndpoint{} = updated_endpoint} = Consumers.update_http_endpoint(http_endpoint, update_attrs)
      assert updated_endpoint.name == "update-endpoint"
      assert updated_endpoint.scheme == :http
      assert updated_endpoint.host == "updated.example.com"
      assert updated_endpoint.port == 8080
      assert updated_endpoint.path == "/updated"
      assert updated_endpoint.headers == %{"Authorization" => "Bearer token"}
    end

    test "returns error changeset with invalid attributes" do
      http_endpoint = ConsumersFactory.insert_http_endpoint!()

      invalid_attrs = %{
        name: nil,
        scheme: :invalid,
        host: "",
        port: -1,
        headers: "invalid"
      }

      assert {:error, %Ecto.Changeset{}} = Consumers.update_http_endpoint(http_endpoint, invalid_attrs)
    end
  end

  describe "matches_message/2" do
    test "matches when action is in allowed actions" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update],
              column_filters: []
            )
          ]
        )

      insert_message = ReplicationFactory.postgres_message(action: :insert, table_oid: table_oid)
      update_message = ReplicationFactory.postgres_message(action: :update, table_oid: table_oid)
      delete_message = ReplicationFactory.postgres_message(action: :delete, table_oid: table_oid)

      assert Consumers.matches_message?(consumer, insert_message)
      assert Consumers.matches_message?(consumer, update_message)
      refute Consumers.matches_message?(consumer, delete_message)
    end

    test "matches message with correct oid and ignores message with incorrect oid" do
      matching_oid = Sequin.Factory.unique_integer()
      non_matching_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: matching_oid,
              actions: [:insert, :update, :delete],
              column_filters: []
            )
          ]
        )

      matching_message = ReplicationFactory.postgres_message(action: :insert, table_oid: matching_oid)
      non_matching_message = ReplicationFactory.postgres_message(action: :insert, table_oid: non_matching_oid)

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "does not match when action is not in allowed actions" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update],
              column_filters: []
            )
          ]
        )

      delete_message = ReplicationFactory.postgres_message(action: :delete, table_oid: table_oid)
      refute Consumers.matches_message?(consumer, delete_message)

      # Test with a message that has a matching OID but disallowed action
      insert_message = ReplicationFactory.postgres_message(action: :insert, table_oid: table_oid)
      assert Consumers.matches_message?(consumer, insert_message)
    end

    test "matches when no column filters are present" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: []
            )
          ]
        )

      insert_message = ReplicationFactory.postgres_message(action: :insert, table_oid: table_oid)
      update_message = ReplicationFactory.postgres_message(action: :update, table_oid: table_oid)
      delete_message = ReplicationFactory.postgres_message(action: :delete, table_oid: table_oid)

      assert Consumers.matches_message?(consumer, insert_message)
      assert Consumers.matches_message?(consumer, update_message)
      assert Consumers.matches_message?(consumer, delete_message)
    end

    test "matches when all column filters match" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :==,
                  value: %StringValue{value: "test_value"}
                ),
                ConsumersFactory.column_filter(
                  column_attnum: 2,
                  operator: :>,
                  value: %NumberValue{value: 10}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "test_value"),
            ReplicationFactory.field(column_attnum: 2, value: 15)
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "test_value"),
            ReplicationFactory.field(column_attnum: 2, value: 5)
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "does not match when any column filter doesn't match" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :==,
                  value: %StringValue{value: "test_value"}
                ),
                ConsumersFactory.column_filter(
                  column_attnum: 2,
                  operator: :>,
                  value: %NumberValue{value: 10}
                ),
                ConsumersFactory.column_filter(
                  column_attnum: 3,
                  operator: :!=,
                  value: %StringValue{value: "excluded_value"}
                )
              ]
            )
          ]
        )

      non_matching_message1 =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "wrong_value"),
            ReplicationFactory.field(column_attnum: 2, value: 15),
            ReplicationFactory.field(column_attnum: 3, value: "some_value")
          ]
        )

      non_matching_message2 =
        ReplicationFactory.postgres_message(
          action: :update,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "test_value"),
            ReplicationFactory.field(column_attnum: 2, value: 5),
            ReplicationFactory.field(column_attnum: 3, value: "some_value")
          ]
        )

      non_matching_message3 =
        ReplicationFactory.postgres_message(
          action: :delete,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "test_value"),
            ReplicationFactory.field(column_attnum: 2, value: 15),
            ReplicationFactory.field(column_attnum: 3, value: "excluded_value")
          ]
        )

      refute Consumers.matches_message?(consumer, non_matching_message1)
      refute Consumers.matches_message?(consumer, non_matching_message2)
      refute Consumers.matches_message?(consumer, non_matching_message3)
    end

    test "equality operator (==) matches for string values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :==,
                  value: %StringValue{value: "test_value"}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "test_value")
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "wrong_value")
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "equality operator (==) matches for integer values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :==,
                  value: %NumberValue{value: 123}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: 123)
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: 456)
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "equality operator (==) matches for boolean values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :==,
                  value: %BooleanValue{value: true}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: true)
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: false)
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "equality operator (==) matches for datetime values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :==,
                  value: %DateTimeValue{value: ~U[2022-01-01 12:00:00Z]}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: ~U[2022-01-01 12:00:00Z])
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: ~U[2022-01-01 13:00:00Z])
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "inequality operator (!=) matches for string values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :!=,
                  value: %StringValue{value: "test_value"}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "wrong_value")
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "test_value")
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "inequality operator (!=) matches for integer values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :!=,
                  value: %NumberValue{value: 123}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: 456)
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: 123)
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "inequality operator (!=) matches for boolean values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :!=,
                  value: %BooleanValue{value: true}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: false)
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: true)
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "inequality operator (!=) matches for datetime values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :!=,
                  value: %DateTimeValue{value: ~U[2022-01-01 12:00:00Z]}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: ~U[2022-01-01 13:00:00Z])
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: ~U[2022-01-01 12:00:00Z])
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "greater than operator (>) matches for integer values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :>,
                  value: %NumberValue{value: 10}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: 15)
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: 5)
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "greater than operator (>) matches for datetime values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :>,
                  value: %DateTimeValue{value: ~U[2022-01-01 12:00:00Z]}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: ~U[2022-01-01 13:00:00Z])
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: ~U[2022-01-01 11:00:00Z])
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "less than operator (<) matches for integer values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :<,
                  value: %NumberValue{value: 10}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: 5)
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: 15)
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "less than operator (<) matches for datetime values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :<,
                  value: %DateTimeValue{value: ~U[2022-01-01 12:00:00Z]}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: ~U[2022-01-01 11:00:00Z])
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: ~U[2022-01-01 13:00:00Z])
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "greater than or equal to operator (>=) matches for integer values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :>=,
                  value: %NumberValue{value: 10}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: 15)
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: 5)
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "greater than or equal to operator (>=) matches for datetime values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :>=,
                  value: %DateTimeValue{value: ~U[2022-01-01 12:00:00Z]}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: ~U[2022-01-01 13:00:00Z])
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: ~U[2022-01-01 11:00:00Z])
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "less than or equal to operator (<=) matches for integer values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :<=,
                  value: %NumberValue{value: 10}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: 5)
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: 15)
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "less than or equal to operator (<=) matches for datetime values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :<=,
                  value: %DateTimeValue{value: ~U[2022-01-01 12:00:00Z]}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: ~U[2022-01-01 11:00:00Z])
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: ~U[2022-01-01 13:00:00Z])
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "in operator matches for string values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :in,
                  value: %ListValue{value: ["value1", "value2"]}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "value1")
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "value3")
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "in operator matches for integer values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :in,
                  value: %ListValue{value: [1, 2, 3]}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: 2)
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: 4)
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "not_in operator matches for string values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :not_in,
                  value: %ListValue{value: ["value1", "value2"]}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "value3")
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "value1")
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "not_in operator matches for integer values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :not_in,
                  value: %ListValue{value: [1, 2, 3]}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: 4)
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: 2)
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "is_null operator matches for null values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :is_null,
                  value: %NullValue{value: nil}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: nil)
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "value")
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "is_null operator does not match for non-null values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :is_null,
                  value: %NullValue{value: nil}
                )
              ]
            )
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "value")
          ]
        )

      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "not_null operator matches for non-null values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :not_null,
                  value: %NullValue{value: nil}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "value")
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: nil)
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "not_null operator does not match for null values" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :not_null,
                  value: %NullValue{value: nil}
                )
              ]
            )
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: nil)
          ]
        )

      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "correctly compares datetimes with after and before operators" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :>,
                  value: %DateTimeValue{value: ~U[2022-03-31 12:00:00Z]}
                ),
                ConsumersFactory.column_filter(
                  column_attnum: 2,
                  operator: :<,
                  value: %DateTimeValue{value: ~U[2022-04-02 12:00:00Z]}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: ~U[2022-04-01 12:00:00Z]),
            ReplicationFactory.field(column_attnum: 2, value: ~U[2022-04-01 12:00:00Z])
          ]
        )

      non_matching_message1 =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: ~U[2022-03-30 12:00:00Z]),
            ReplicationFactory.field(column_attnum: 2, value: ~U[2022-04-01 12:00:00Z])
          ]
        )

      non_matching_message2 =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: ~U[2022-04-01 12:00:00Z]),
            ReplicationFactory.field(column_attnum: 2, value: ~U[2022-04-03 12:00:00Z])
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message1)
      refute Consumers.matches_message?(consumer, non_matching_message2)
    end

    test "matches with multiple source tables" do
      table_oid1 = Sequin.Factory.unique_integer()
      table_oid2 = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid1,
              actions: [:insert, :update, :delete],
              column_filters: []
            ),
            ConsumersFactory.source_table(
              oid: table_oid2,
              actions: [:insert, :update, :delete],
              column_filters: []
            )
          ]
        )

      matching_message1 =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid1
        )

      matching_message2 =
        ReplicationFactory.postgres_message(
          action: :update,
          table_oid: table_oid2
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :delete,
          table_oid: Sequin.Factory.unique_integer()
        )

      assert Consumers.matches_message?(consumer, matching_message1)
      assert Consumers.matches_message?(consumer, matching_message2)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "matches with multiple column filters" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :==,
                  value: %StringValue{value: "test_value"}
                ),
                ConsumersFactory.column_filter(
                  column_attnum: 2,
                  operator: :>,
                  value: %NumberValue{value: 10}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "test_value"),
            ReplicationFactory.field(column_attnum: 2, value: 15)
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "test_value"),
            ReplicationFactory.field(column_attnum: 2, value: 5)
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "handles missing fields gracefully" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :==,
                  value: %StringValue{value: "test_value"}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "test_value")
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: []
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "matches case-sensitive string comparisons correctly" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :==,
                  value: %StringValue{value: "TestValue"}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "TestValue")
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "testvalue")
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "handles whitespace in string comparisons correctly" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :==,
                  value: %StringValue{value: " test value "}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: " test value ")
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: "test value")
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "handles timezone-aware datetime comparisons correctly" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :==,
                  value: %DateTimeValue{value: ~U[2022-01-01 12:00:00Z]}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: ~U[2022-01-01 12:00:00Z])
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: ~U[2022-01-01 13:00:00Z])
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "correctly handles boolean true/false values in comparisons" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :==,
                  value: %BooleanValue{value: true}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: true)
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: false)
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "properly handles empty lists in 'in' and 'not_in' operators" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :in,
                  value: %ListValue{value: [:in_value]}
                ),
                ConsumersFactory.column_filter(
                  column_attnum: 2,
                  operator: :not_in,
                  value: %ListValue{value: [:not_in_value]}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: :in_value),
            ReplicationFactory.field(column_attnum: 2, value: :in_value)
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: :not_in_value),
            ReplicationFactory.field(column_attnum: 2, value: :not_in_value)
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "correctly compares values at the boundaries of ranges" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :>,
                  value: %NumberValue{value: 10}
                ),
                ConsumersFactory.column_filter(
                  column_attnum: 2,
                  operator: :<,
                  value: %NumberValue{value: 20}
                ),
                ConsumersFactory.column_filter(
                  column_attnum: 3,
                  operator: :>=,
                  value: %NumberValue{value: 30}
                ),
                ConsumersFactory.column_filter(
                  column_attnum: 4,
                  operator: :<=,
                  value: %NumberValue{value: 40}
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: 15),
            ReplicationFactory.field(column_attnum: 2, value: 15),
            ReplicationFactory.field(column_attnum: 3, value: 30),
            ReplicationFactory.field(column_attnum: 4, value: 40)
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: 10),
            ReplicationFactory.field(column_attnum: 2, value: 20),
            ReplicationFactory.field(column_attnum: 3, value: 29),
            ReplicationFactory.field(column_attnum: 4, value: 41)
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "matches JSONB top-level field" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :==,
                  value: %StringValue{value: "test_value"},
                  jsonb_path: "top_level_key"
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: %{"top_level_key" => "test_value"})
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: %{"top_level_key" => "wrong_value"})
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "matches JSONB nested field" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :>,
                  value: %NumberValue{value: 10},
                  jsonb_path: "nested.field"
                )
              ]
            )
          ]
        )

      matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: %{"nested" => %{"field" => 15}})
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: %{"nested" => %{"field" => 5}})
          ]
        )

      assert Consumers.matches_message?(consumer, matching_message)
      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "handles missing JSONB nested field" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :==,
                  value: %StringValue{value: "test_value"},
                  jsonb_path: "nested.non_existent"
                )
              ]
            )
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: %{"nested" => %{"other_field" => "test_value"}})
          ]
        )

      refute Consumers.matches_message?(consumer, non_matching_message)
    end

    test "does not support traversing JSONB array elements" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          source_tables: [
            ConsumersFactory.source_table(
              oid: table_oid,
              actions: [:insert, :update, :delete],
              column_filters: [
                ConsumersFactory.column_filter(
                  column_attnum: 1,
                  operator: :in,
                  value: %ListValue{value: ["value1", "value2"]},
                  jsonb_path: "array.0"
                )
              ]
            )
          ]
        )

      non_matching_message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: table_oid,
          fields: [
            ReplicationFactory.field(column_attnum: 1, value: %{"array" => ["value1", "other"]}),
            ReplicationFactory.field(column_attnum: 2, value: "other")
          ]
        )

      refute Consumers.matches_message?(consumer, non_matching_message)
    end
  end

  describe "consumer_features/1" do
    test "returns legacy_event_transform feature when conditions are met" do
      account = AccountsFactory.account(features: ["legacy_event_transform"])
      event_table = DatabasesFactory.event_table()
      database = DatabasesFactory.postgres_database(account: account, tables: [event_table])

      sequence =
        DatabasesFactory.sequence(
          postgres_database_id: database.id,
          table_oid: event_table.oid
        )

      consumer =
        ConsumersFactory.sink_consumer(
          account: account,
          postgres_database: database,
          sequence: sequence,
          type: :http_push
        )

      assert Consumers.consumer_features(consumer) == [legacy_event_transform: true]
    end

    test "does not return legacy_event_transform feature when account doesn't have the feature" do
      account = AccountsFactory.account(features: [], inserted_at: DateTime.utc_now())
      event_table = DatabasesFactory.event_table()
      database = DatabasesFactory.postgres_database(account: account, tables: [event_table])

      sequence =
        DatabasesFactory.sequence(
          postgres_database_id: database.id,
          table_oid: event_table.oid
        )

      consumer =
        ConsumersFactory.sink_consumer(
          account: account,
          postgres_database: database,
          sequence: sequence,
          type: :http_push
        )

      assert Consumers.consumer_features(consumer) == []
    end

    test "returns legacy_event_singleton_transform when account is old enough" do
      account = AccountsFactory.account(features: [], inserted_at: ~D[2024-11-01])

      event_table = DatabasesFactory.event_table()
      database = DatabasesFactory.postgres_database(account: account, tables: [event_table])

      sequence =
        DatabasesFactory.sequence(
          postgres_database_id: database.id,
          table_oid: event_table.oid
        )

      consumer =
        ConsumersFactory.sink_consumer(
          account: account,
          postgres_database: database,
          sequence: sequence,
          type: :http_push
        )

      assert Consumers.consumer_features(consumer) == [{:legacy_event_singleton_transform, true}]
    end
  end

  # Helper function to create a consumer record from a character
  defp build_consumer_record(consumer, character, type \\ :default) do
    table_oid =
      case type do
        :default -> Character.table_oid()
        :multi_pk -> CharacterMultiPK.table_oid()
        :detailed -> CharacterDetailed.table_oid()
      end

    record_pks =
      case type do
        :default -> [character.id]
        :multi_pk -> [character.id_integer, character.id_string, character.id_uuid]
        :detailed -> [character.id]
      end

    ConsumersFactory.consumer_record(consumer_id: consumer.id, table_oid: table_oid, record_pks: record_pks)
  end

  describe "reset_message_visibility/2" do
    test "resets visibility for consumer record" do
      consumer = ConsumersFactory.insert_sink_consumer!(message_kind: :record)
      now = DateTime.utc_now()

      record =
        ConsumersFactory.insert_consumer_record!(
          consumer_id: consumer.id,
          state: :delivered,
          not_visible_until: DateTime.add(now, 30, :second)
        )

      assert {:ok, updated_record} = Consumers.reset_message_visibility(consumer, record.id)

      # Verify the record was updated
      assert DateTime.before?(updated_record.not_visible_until, record.not_visible_until)
      assert updated_record.state == :available

      # Verify database was updated
      db_record = Repo.get_by(ConsumerRecord, id: record.id)
      assert DateTime.before?(db_record.not_visible_until, record.not_visible_until)
      assert db_record.state == :available
    end

    test "resets visibility for consumer event" do
      consumer = ConsumersFactory.insert_sink_consumer!(message_kind: :event)
      now = DateTime.utc_now()

      event =
        ConsumersFactory.insert_consumer_event!(
          consumer_id: consumer.id,
          not_visible_until: DateTime.add(now, 30, :second)
        )

      assert {:ok, updated_event} = Consumers.reset_message_visibility(consumer, event.id)

      # Verify the event was updated
      assert DateTime.before?(updated_event.not_visible_until, event.not_visible_until)

      # Verify database was updated
      db_event = Repo.get_by(ConsumerEvent, id: event.id)
      assert DateTime.before?(db_event.not_visible_until, event.not_visible_until)
    end

    test "returns error when message does not exist" do
      consumer = ConsumersFactory.insert_sink_consumer!()
      non_existent_id = Factory.integer()

      assert {:error, %NotFoundError{}} = Consumers.reset_message_visibility(consumer, non_existent_id)
    end

    test "returns error when message belongs to different consumer" do
      consumer1 = ConsumersFactory.insert_sink_consumer!(message_kind: :record)
      consumer2 = ConsumersFactory.insert_sink_consumer!(message_kind: :record)

      record =
        ConsumersFactory.insert_consumer_record!(
          consumer_id: consumer1.id,
          state: :delivered,
          not_visible_until: DateTime.add(DateTime.utc_now(), 30, :second)
        )

      assert {:error, %NotFoundError{}} = Consumers.reset_message_visibility(consumer2, record.id)

      # Verify record was not modified
      db_record = Repo.get_by(ConsumerRecord, id: record.id)
      assert db_record.not_visible_until == record.not_visible_until
      assert db_record.state == :delivered
    end
  end

  describe "upsert_consumer_events/1" do
    test "inserts a new consumer event" do
      consumer = ConsumersFactory.insert_sink_consumer!(message_kind: :event)
      event = ConsumersFactory.consumer_event(consumer_id: consumer.id)

      assert {:ok, 1} = Consumers.upsert_consumer_events([event])

      inserted_event = Repo.get_by(ConsumerEvent, consumer_id: consumer.id)
      assert inserted_event
      assert inserted_event.ack_id == event.ack_id
    end

    test "updates existing consumer event" do
      consumer = ConsumersFactory.insert_sink_consumer!(message_kind: :event)
      existing_event = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id)

      updated_attrs = %ConsumerEvent{
        existing_event
        | not_visible_until: DateTime.add(DateTime.utc_now(), 30, :second)
      }

      assert {:ok, 1} = Consumers.upsert_consumer_events([updated_attrs])

      updated_event = Repo.get_by(ConsumerEvent, consumer_id: consumer.id)
      assert updated_event
      refute updated_event.not_visible_until == existing_event.not_visible_until
    end
  end

  describe "upsert_consumer_records/1" do
    test "inserts a new consumer record" do
      consumer = ConsumersFactory.insert_sink_consumer!(message_kind: :record)
      record = ConsumersFactory.consumer_record(consumer_id: consumer.id)

      assert {:ok, 1} = Consumers.upsert_consumer_records([record])

      inserted_record = Repo.get_by(ConsumerRecord, consumer_id: consumer.id)
      assert inserted_record
      assert inserted_record.ack_id == record.ack_id
    end

    test "updates existing consumer record" do
      consumer = ConsumersFactory.insert_sink_consumer!(message_kind: :record)
      existing_record = ConsumersFactory.insert_consumer_record!(consumer_id: consumer.id, state: :available)

      updated_record = %ConsumerRecord{
        existing_record
        | state: :delivered
      }

      assert {:ok, 1} = Consumers.upsert_consumer_records([updated_record])

      updated_record = Repo.get_by(ConsumerRecord, consumer_id: consumer.id)
      assert updated_record
      assert updated_record.state == :delivered
    end
  end
end

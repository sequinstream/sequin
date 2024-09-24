defmodule Sequin.ConsumersTest do
  use Sequin.DataCase, async: true

  alias Sequin.Consumers
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Consumers.SourceTable.BooleanValue
  alias Sequin.Consumers.SourceTable.DateTimeValue
  alias Sequin.Consumers.SourceTable.ListValue
  alias Sequin.Consumers.SourceTable.NullValue
  alias Sequin.Consumers.SourceTable.NumberValue
  alias Sequin.Consumers.SourceTable.StringValue
  alias Sequin.Databases
  alias Sequin.Databases.ConnectionCache
  alias Sequin.Error.NotFoundError
  alias Sequin.Factory
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.CharacterFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Test.Support.Models.Character
  alias Sequin.Test.Support.Models.CharacterDetailed
  alias Sequin.Test.Support.Models.CharacterMultiPK
  alias Sequin.Test.UnboxedRepo

  describe "receive_for_consumer/2 with event message kind" do
    setup do
      consumer = ConsumersFactory.insert_consumer!(max_ack_pending: 1_000, message_kind: :event)
      %{consumer: consumer}
    end

    test "returns nothing if consumer_events is empty", %{consumer: consumer} do
      assert {:ok, []} = Consumers.receive_for_consumer(consumer)
    end

    test "delivers available outstanding events", %{consumer: consumer} do
      event = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, not_visible_until: nil, deliver_count: 0)
      ack_wait_ms = consumer.ack_wait_ms

      assert {:ok, [delivered_event]} = Consumers.receive_for_consumer(consumer)
      not_visible_until = DateTime.add(DateTime.utc_now(), ack_wait_ms - 1000, :millisecond)
      assert delivered_event.ack_id == event.ack_id
      assert delivered_event.id == event.id
      updated_event = Repo.get_by(ConsumerEvent, id: event.id)
      assert DateTime.after?(updated_event.not_visible_until, not_visible_until)
      assert updated_event.deliver_count == 1
      assert updated_event.last_delivered_at
    end

    test "redelivers expired outstanding events", %{consumer: consumer} do
      event =
        ConsumersFactory.insert_consumer_event!(
          consumer_id: consumer.id,
          not_visible_until: DateTime.add(DateTime.utc_now(), -1, :second),
          deliver_count: 1,
          last_delivered_at: DateTime.add(DateTime.utc_now(), -30, :second)
        )

      assert {:ok, [redelivered_event]} = Consumers.receive_for_consumer(consumer)
      assert redelivered_event.id == event.id
      assert redelivered_event.ack_id == event.ack_id
      updated_event = Repo.get_by(ConsumerEvent, id: event.id)
      assert DateTime.compare(updated_event.not_visible_until, event.not_visible_until) != :eq
      assert updated_event.deliver_count == 2
      assert DateTime.compare(updated_event.last_delivered_at, event.last_delivered_at) != :eq
    end

    test "does not redeliver unexpired outstanding events", %{consumer: consumer} do
      ConsumersFactory.insert_consumer_event!(
        consumer_id: consumer.id,
        not_visible_until: DateTime.add(DateTime.utc_now(), 30, :second)
      )

      assert {:ok, []} = Consumers.receive_for_consumer(consumer)
    end

    test "delivers only up to batch_size", %{consumer: consumer} do
      for _ <- 1..3 do
        ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, not_visible_until: nil)
      end

      assert {:ok, delivered} = Consumers.receive_for_consumer(consumer, batch_size: 2)
      assert length(delivered) == 2
      assert length(Repo.all(ConsumerEvent)) == 3
    end

    test "does not deliver outstanding events for another consumer", %{consumer: consumer} do
      other_consumer = ConsumersFactory.insert_consumer!(message_kind: :event)
      ConsumersFactory.insert_consumer_event!(consumer_id: other_consumer.id, not_visible_until: nil)

      assert {:ok, []} = Consumers.receive_for_consumer(consumer)
    end

    test "with a mix of available and unavailable events, delivers only available outstanding events", %{
      consumer: consumer
    } do
      available =
        for _ <- 1..3 do
          ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, not_visible_until: nil)
        end

      redeliver =
        for _ <- 1..3 do
          ConsumersFactory.insert_consumer_event!(
            consumer_id: consumer.id,
            not_visible_until: DateTime.add(DateTime.utc_now(), -30, :second)
          )
        end

      for _ <- 1..3 do
        ConsumersFactory.insert_consumer_event!(
          consumer_id: consumer.id,
          not_visible_until: DateTime.add(DateTime.utc_now(), 30, :second)
        )
      end

      assert {:ok, events} = Consumers.receive_for_consumer(consumer)
      assert length(events) == length(available ++ redeliver)
      assert_lists_equal(events, available ++ redeliver, &assert_maps_equal(&1, &2, [:consumer_id, :id]))
    end

    test "does not deliver events if there is an outstanding event with same record_pks and table_oid", %{
      consumer: consumer
    } do
      # Create an outstanding event
      outstanding_event =
        ConsumersFactory.insert_consumer_event!(
          consumer_id: consumer.id,
          not_visible_until: DateTime.add(DateTime.utc_now(), 30, :second),
          record_pks: [1],
          table_oid: 12_345
        )

      # Create an event with the same record_pks and table_oid
      same_combo_event =
        ConsumersFactory.insert_consumer_event!(
          consumer_id: consumer.id,
          not_visible_until: nil,
          record_pks: outstanding_event.record_pks,
          table_oid: outstanding_event.table_oid
        )

      # Create a different event
      different_event =
        ConsumersFactory.insert_consumer_event!(
          consumer_id: consumer.id,
          not_visible_until: nil,
          record_pks: [2],
          table_oid: 67_890
        )

      # Attempt to receive events
      assert {:ok, delivered_events} = Consumers.receive_for_consumer(consumer)

      # Check that only the different event was delivered
      assert length(delivered_events) == 1
      assert hd(delivered_events).id == different_event.id

      # Verify that the same_combo_event was not delivered
      refute Repo.get_by(ConsumerEvent, id: same_combo_event.id).not_visible_until
    end

    test "delivers events according to id asc", %{consumer: consumer} do
      event1 = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, not_visible_until: nil)
      event2 = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, not_visible_until: nil)
      _event3 = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, not_visible_until: nil)

      assert {:ok, delivered} = Consumers.receive_for_consumer(consumer, batch_size: 2)
      assert length(delivered) == 2
      delivered_ids = Enum.map(delivered, & &1.id)
      assert_lists_equal(delivered_ids, [event1.id, event2.id])
    end

    test "respects a consumer's max_ack_pending", %{consumer: consumer} do
      max_ack_pending = 3
      consumer = %{consumer | max_ack_pending: max_ack_pending}

      event = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, not_visible_until: nil)

      for _ <- 1..2 do
        ConsumersFactory.insert_consumer_event!(
          consumer_id: consumer.id,
          not_visible_until: DateTime.add(DateTime.utc_now(), 30, :second)
        )
      end

      for _ <- 1..2 do
        ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, not_visible_until: nil)
      end

      assert {:ok, delivered} = Consumers.receive_for_consumer(consumer)
      assert length(delivered) == 1
      assert List.first(delivered).id == event.id
      assert {:ok, []} = Consumers.receive_for_consumer(consumer)
    end
  end

  describe "receive_for_consumer with concurrent workers" do
    setup do
      consumer = ConsumersFactory.insert_consumer!(max_ack_pending: 100, message_kind: :event)

      for _ <- 1..10 do
        ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, not_visible_until: nil)
      end

      {:ok, consumer: consumer}
    end

    test "ensures unique events are received by concurrent workers", %{consumer: consumer} do
      tasks =
        Enum.map(1..20, fn _ ->
          Task.async(fn ->
            Consumers.receive_for_consumer(consumer, batch_size: 1)
          end)
        end)

      results = Task.await_many(tasks, 5000)

      {successful, empty} =
        Enum.reduce(results, {[], []}, fn result, {successful, empty} ->
          case result do
            {:ok, [event]} -> {[event | successful], empty}
            {:ok, []} -> {successful, ["empty" | empty]}
          end
        end)

      assert length(successful) == 10
      assert length(empty) == 10

      unique_events = Enum.uniq_by(successful, & &1.id)
      assert length(unique_events) == 10
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
        ConsumersFactory.insert_consumer!(
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

        assert record.data.metadata.table_name == "characters"
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
      assert fetched_record.data.record["avatar"] == character.avatar
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

    test "handles when PostgresDatabase.tables lists a non-existent column", %{consumer: consumer} do
      character = CharacterFactory.insert_character!()
      record = build_consumer_record(consumer, character)

      # Add a non-existent column to the table definition
      fake_column = DatabasesFactory.column(%{name: "non_existent_column", type: "text", is_pk?: false})

      consumer =
        update_in(consumer.postgres_database.tables, fn tables ->
          Enum.map(tables, fn table ->
            if table.name == "characters", do: %{table | columns: [fake_column | table.columns]}, else: table
          end)
        end)

      {:ok, [fetched_record]} = Consumers.put_source_data(consumer, [record])

      assert fetched_record.data.record["non_existent_column"] == nil
    end

    @tag capture_log: true
    test "errors when the source database is unreachable", %{consumer: consumer} do
      character = CharacterFactory.insert_character!()
      record = build_consumer_record(consumer, character)
      config = Keyword.merge(UnboxedRepo.config(), hostname: "unreachable_host", queue_target: 25, queue_interval: 25)
      {:ok, conn} = Postgrex.start_link(config)
      ConnectionCache.cache_connection(consumer.postgres_database, conn)

      assert {:error, %DBConnection.ConnectionError{}} = Consumers.put_source_data(consumer, [record])
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
        ConsumersFactory.consumer(
          source_tables: [
            ConsumersFactory.source_table(
              oid: @table_oid,
              column_filters: [
                ConsumersFactory.column_filter(
                  column_name: "column_1",
                  operator: :==,
                  value: %StringValue{value: "test_value"}
                ),
                ConsumersFactory.column_filter(
                  column_name: "column_2",
                  operator: :>,
                  value: %NumberValue{value: 10}
                )
              ]
            )
          ]
        )

      {:ok, consumer: consumer}
    end

    test "matches when all column filters match", %{consumer: consumer} do
      record = %{
        "column_1" => "test_value",
        "column_2" => 15
      }

      assert Consumers.matches_record?(consumer, @table_oid, record)
    end

    test "does not match when any column filter doesn't match", %{consumer: consumer} do
      record1 = %{
        "column_1" => "wrong_value",
        "column_2" => 15
      }

      record2 = %{
        "column_1" => "test_value",
        "column_2" => 5
      }

      refute Consumers.matches_record?(consumer, @table_oid, record1)
      refute Consumers.matches_record?(consumer, @table_oid, record2)
    end

    test "matches when no column filters are present" do
      consumer =
        ConsumersFactory.http_push_consumer(
          source_tables: [
            ConsumersFactory.source_table(
              oid: @table_oid,
              column_filters: []
            )
          ]
        )

      record = %{
        "column_1" => "any_value",
        "column_2" => 100
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
        name: "Test Endpoint",
        scheme: :https,
        host: "example.com",
        port: 443,
        path: "/webhook",
        headers: %{"Content-Type" => "application/json"},
        use_local_tunnel: false
      }

      assert {:ok, %HttpEndpoint{} = http_endpoint} = Consumers.create_http_endpoint_for_account(account.id, valid_attrs)
      assert http_endpoint.name == "Test Endpoint"
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

      assert {:ok, %HttpEndpoint{} = http_endpoint} = Consumers.create_http_endpoint_for_account(account.id, valid_attrs)
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

      assert {:error, %Ecto.Changeset{}} = Consumers.create_http_endpoint_for_account(account.id, invalid_attrs)
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
        ConsumersFactory.http_push_consumer(
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
        ConsumersFactory.http_push_consumer(
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
        ConsumersFactory.http_push_consumer(
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
        ConsumersFactory.http_push_consumer(
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
        ConsumersFactory.http_push_consumer(
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
        ConsumersFactory.http_push_consumer(
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
        ConsumersFactory.http_push_consumer(
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
        ConsumersFactory.http_push_consumer(
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
        ConsumersFactory.http_push_consumer(
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
        ConsumersFactory.http_push_consumer(
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
        ConsumersFactory.http_push_consumer(
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
        ConsumersFactory.http_push_consumer(
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
        ConsumersFactory.http_push_consumer(
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
        ConsumersFactory.http_push_consumer(
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
        ConsumersFactory.http_push_consumer(
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
        ConsumersFactory.http_push_consumer(
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
        ConsumersFactory.http_push_consumer(
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
        ConsumersFactory.http_push_consumer(
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
        ConsumersFactory.http_push_consumer(
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
        ConsumersFactory.http_push_consumer(
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
        ConsumersFactory.http_push_consumer(
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
        ConsumersFactory.http_push_consumer(
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
        ConsumersFactory.http_push_consumer(
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
        ConsumersFactory.http_push_consumer(
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
        ConsumersFactory.http_push_consumer(
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
        ConsumersFactory.http_push_consumer(
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
        ConsumersFactory.http_push_consumer(
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
        ConsumersFactory.http_push_consumer(
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
        ConsumersFactory.http_push_consumer(
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
        ConsumersFactory.http_push_consumer(
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
        ConsumersFactory.http_push_consumer(
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
        ConsumersFactory.http_push_consumer(
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
        ConsumersFactory.http_push_consumer(
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
        ConsumersFactory.http_push_consumer(
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
        ConsumersFactory.http_push_consumer(
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
        ConsumersFactory.http_push_consumer(
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
        ConsumersFactory.http_push_consumer(
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
        ConsumersFactory.http_push_consumer(
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
        ConsumersFactory.http_push_consumer(
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
        ConsumersFactory.http_push_consumer(
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
end

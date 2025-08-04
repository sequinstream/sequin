defmodule Sequin.ConsumersTest do
  use Sequin.DataCase, async: true

  alias Sequin.Consumers
  alias Sequin.Consumers.AcknowledgedMessages
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerEventData
  alias Sequin.Consumers.ConsumerEventData.Metadata
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.EnrichmentFunction
  alias Sequin.Consumers.Function
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Consumers.Source
  alias Sequin.Error.InvariantError
  alias Sequin.Factory
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Size

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

      record1 = %{record1 | payload_size_bytes: Size.bytes(100)}
      record2 = %{record2 | payload_size_bytes: Size.bytes(200)}

      assert {:ok, 2} = Consumers.ack_messages(consumer, [record1.ack_id, record2.ack_id])
      assert {:ok, 2} = Consumers.after_messages_acked(consumer, [record1, record2])

      assert {:ok, messages} = AcknowledgedMessages.fetch_messages(consumer.id)
      assert length(messages) == 2
      assert Enum.all?(messages, &(&1.consumer_id == consumer.id))
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

  describe "update_sink_consumer/2" do
    test "updates the sink_consumer with valid attributes" do
      sink_consumer = ConsumersFactory.insert_sink_consumer!()

      update_attrs = %{
        name: "update-consumer",
        batch_size: 10,
        ack_wait_ms: 1000
      }

      assert {:ok, %SinkConsumer{} = updated_consumer} = Consumers.update_sink_consumer(sink_consumer, update_attrs)
      assert updated_consumer.name == "update-consumer"
      assert updated_consumer.batch_size == 10
      assert updated_consumer.ack_wait_ms == 1000
    end
  end

  describe "matches_message/2" do
    test "matches when action is in allowed actions" do
      table_oid = Sequin.Factory.unique_integer()

      consumer =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          actions: [:insert, :update]
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
          source: %Source{include_table_oids: [matching_oid]}
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
          actions: [:insert, :update]
        )

      delete_message = ReplicationFactory.postgres_message(action: :delete, table_oid: table_oid)
      refute Consumers.matches_message?(consumer, delete_message)

      # Test with a message that has a matching OID but disallowed action
      insert_message = ReplicationFactory.postgres_message(action: :insert, table_oid: table_oid)
      assert Consumers.matches_message?(consumer, insert_message)
    end
  end

  describe "list_active_sink_consumers" do
    test "returns active sink consumers with active replication slot" do
      account = AccountsFactory.insert_account!()
      db = DatabasesFactory.insert_postgres_database!(account_id: account.id)

      slot =
        ReplicationFactory.insert_postgres_replication!(
          account_id: account.id,
          status: :active,
          postgres_database_id: db.id
        )

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          account_id: account.id,
          postgres_database_id: db.id,
          replication_slot_id: slot.id
        )

      assert Consumers.list_active_sink_consumers() == [consumer]
    end

    test "does not return disabled sink with active replication slot" do
      account = AccountsFactory.insert_account!()
      db = DatabasesFactory.insert_postgres_database!(account_id: account.id)

      slot =
        ReplicationFactory.insert_postgres_replication!(
          account_id: account.id,
          status: :active,
          postgres_database_id: db.id
        )

      ConsumersFactory.insert_sink_consumer!(
        account_id: account.id,
        postgres_database_id: db.id,
        replication_slot_id: slot.id,
        status: :disabled
      )

      assert Consumers.list_active_sink_consumers() == []
    end

    test "does not return sink consumers with disabled replication slot" do
      account = AccountsFactory.insert_account!()
      db = DatabasesFactory.insert_postgres_database!(account_id: account.id)

      slot =
        ReplicationFactory.insert_postgres_replication!(
          account_id: account.id,
          status: :disabled,
          postgres_database_id: db.id
        )

      ConsumersFactory.insert_sink_consumer!(
        account_id: account.id,
        postgres_database_id: db.id,
        replication_slot_id: slot.id,
        status: :active
      )

      assert Consumers.list_active_sink_consumers() == []
    end
  end

  describe "list_sink_consumers_for_account_paginated/3" do
    import Sequin.Test.Assertions, only: [assert_lists_equal: 2]

    test "paginates sink consumers for an account with custom ordering" do
      # Create an account
      account = AccountsFactory.insert_account!()

      # Create 10 sink consumers with different names in reverse order
      # This will help prove the ordering is working correctly
      for i <- 10..1//-1 do
        ConsumersFactory.insert_sink_consumer!(
          account_id: account.id,
          name: "consumer-#{String.pad_leading(Integer.to_string(i), 2, "0")}"
        )
      end

      # First page (0-based index) with 3 items per page, ordered by name asc
      page_0 = Consumers.list_sink_consumers_for_account_paginated(account.id, 0, 3, order_by: [asc: :name])
      assert length(page_0) == 3
      assert_lists_equal(Enum.map(page_0, & &1.name), ["consumer-01", "consumer-02", "consumer-03"])

      # Second page with 3 items
      page_1 = Consumers.list_sink_consumers_for_account_paginated(account.id, 1, 3, order_by: [asc: :name])
      assert length(page_1) == 3
      assert_lists_equal(Enum.map(page_1, & &1.name), ["consumer-04", "consumer-05", "consumer-06"])

      # Third page with 3 items
      page_2 = Consumers.list_sink_consumers_for_account_paginated(account.id, 2, 3, order_by: [asc: :name])
      assert length(page_2) == 3
      assert_lists_equal(Enum.map(page_2, & &1.name), ["consumer-07", "consumer-08", "consumer-09"])

      # Fourth page with remaining items
      page_3 = Consumers.list_sink_consumers_for_account_paginated(account.id, 3, 3, order_by: [asc: :name])
      assert length(page_3) == 1
      assert_lists_equal(Enum.map(page_3, & &1.name), ["consumer-10"])

      # Empty page beyond available data
      page_4 = Consumers.list_sink_consumers_for_account_paginated(account.id, 4, 3, order_by: [asc: :name])
      assert Enum.empty?(page_4)

      # Test with different page size
      page_large = Consumers.list_sink_consumers_for_account_paginated(account.id, 0, 5, order_by: [asc: :name])
      assert length(page_large) == 5

      assert_lists_equal(
        Enum.map(page_large, & &1.name),
        ["consumer-01", "consumer-02", "consumer-03", "consumer-04", "consumer-05"]
      )

      # Test with name descending order
      page_desc = Consumers.list_sink_consumers_for_account_paginated(account.id, 0, 3, order_by: [desc: :name])
      assert length(page_desc) == 3
      assert_lists_equal(Enum.map(page_desc, & &1.name), ["consumer-10", "consumer-09", "consumer-08"])

      # Test with preloads
      page_with_preloads =
        Consumers.list_sink_consumers_for_account_paginated(account.id, 0, 3,
          preload: [:postgres_database],
          order_by: [asc: :name]
        )

      assert length(page_with_preloads) == 3
      # Verify preloads worked - would raise error if not preloaded
      for consumer <- page_with_preloads do
        assert %Ecto.Association.NotLoaded{} != consumer.postgres_database
      end
    end
  end

  describe "upsert_consumer_messages/1" do
    test "inserts a new message" do
      consumer = ConsumersFactory.insert_sink_consumer!()
      msg = ConsumersFactory.consumer_message(message_kind: consumer.message_kind, consumer_id: consumer.id)

      assert {:ok, 1} = Consumers.upsert_consumer_messages(consumer, [msg])

      assert [inserted_msg] = Consumers.list_consumer_messages_for_consumer(consumer)
      assert inserted_msg.ack_id == msg.ack_id
    end

    test "inserts a new message with record_serializers" do
      consumer = ConsumersFactory.insert_sink_consumer!()
      msg = ConsumersFactory.consumer_message(message_kind: consumer.message_kind, consumer_id: consumer.id)
      msg = put_in(msg.data.record["date_field"], Date.utc_today())

      assert {:ok, 1} = Consumers.upsert_consumer_messages(consumer, [msg])

      assert [inserted_msg] = Consumers.list_consumer_messages_for_consumer(consumer)
      assert %Date{} = inserted_msg.data.record["date_field"]
    end

    test "updates existing message" do
      consumer = ConsumersFactory.insert_sink_consumer!(message_kind: :event)

      existing_msg =
        ConsumersFactory.insert_consumer_message!(message_kind: consumer.message_kind, consumer_id: consumer.id)

      updated_attrs = %{
        existing_msg
        | not_visible_until: DateTime.add(DateTime.utc_now(), 30, :second)
      }

      assert {:ok, 1} = Consumers.upsert_consumer_messages(consumer, [updated_attrs])

      assert [updated_msg] = Consumers.list_consumer_messages_for_consumer(consumer)
      refute updated_msg.not_visible_until == existing_msg.not_visible_until
    end
  end

  describe "where_wal_cursor_in/2" do
    test "finds events matching WAL cursors for a specific consumer" do
      # Create two consumers
      consumer1 = ConsumersFactory.insert_sink_consumer!()
      consumer2 = ConsumersFactory.insert_sink_consumer!()

      # Create events for consumer1 with different WAL cursors
      message1 =
        ConsumersFactory.insert_consumer_message!(
          message_kind: consumer1.message_kind,
          consumer_id: consumer1.id,
          commit_lsn: 100,
          commit_idx: 1
        )

      message2 =
        ConsumersFactory.insert_consumer_message!(
          message_kind: consumer1.message_kind,
          consumer_id: consumer1.id,
          commit_lsn: 200,
          commit_idx: 2
        )

      # Create events for consumer2 with different WAL cursors
      ConsumersFactory.insert_consumer_message!(
        message_kind: consumer2.message_kind,
        consumer_id: consumer2.id,
        commit_lsn: 100,
        commit_idx: 1
      )

      ConsumersFactory.insert_consumer_message!(
        message_kind: consumer2.message_kind,
        consumer_id: consumer2.id,
        commit_lsn: 200,
        commit_idx: 2
      )

      # Define WAL cursors to search for
      wal_cursors = [
        %{commit_lsn: 100, commit_idx: 1},
        %{commit_lsn: 200, commit_idx: 2}
      ]

      # Query for events matching the WAL cursors for consumer1
      messages = Consumers.list_consumer_messages_for_consumer(consumer1, wal_cursor_in: wal_cursors)

      # Verify results
      assert length(messages) == 2
      assert [found_message1, found_message2] = Enum.sort_by(messages, & &1.id)
      assert found_message1.id == message1.id
      assert found_message2.id == message2.id
      assert Enum.all?(messages, &(&1.consumer_id == consumer1.id))
    end
  end

  describe "list_consumer_messages_for_consumer/3" do
    test "correctly loads Date, DateTime, NaiveDateTime, and Decimal types from Postgres" do
      # Create a consumer
      consumer = ConsumersFactory.insert_sink_consumer!()

      # Set up sample data with different types
      now = DateTime.utc_now()
      today = Date.utc_today()
      naive_now = NaiveDateTime.utc_now()
      decimal_value = Decimal.new("123.45")

      # Create a record with these types in the record field
      record_data = %{
        "date_field" => today,
        "datetime_field" => now,
        "naive_datetime_field" => naive_now,
        "decimal_field" => decimal_value,
        "regular_field" => "test"
      }

      # Insert the consumer message
      ConsumersFactory.insert_consumer_message!(
        consumer_id: consumer.id,
        message_kind: consumer.message_kind,
        data:
          ConsumersFactory.consumer_message_data_attrs(%{
            message_kind: consumer.message_kind,
            record: record_data,
            action: :insert
          })
      )

      # Retrieve the message
      [retrieved_message] = Consumers.list_consumer_messages_for_consumer(consumer)

      # Verify the retrieved message has proper types
      retrieved_record = retrieved_message.data.record

      # Check that each field has the correct type
      assert retrieved_record["date_field"] == today
      assert retrieved_record["datetime_field"] == now
      assert retrieved_record["naive_datetime_field"] == naive_now
      assert retrieved_record["decimal_field"] == decimal_value
      assert retrieved_record["regular_field"] == "test"

      # Check the specific struct types to ensure proper deserialization
      assert is_struct(retrieved_record["date_field"], Date)
      assert is_struct(retrieved_record["datetime_field"], DateTime)
      assert is_struct(retrieved_record["naive_datetime_field"], NaiveDateTime)
      assert is_struct(retrieved_record["decimal_field"], Decimal)
    end
  end

  describe "stream_messages/3" do
    test "streams consummer messages ordered by commit_lsn and commit_idx" do
      consumer = ConsumersFactory.insert_sink_consumer!()

      # Create events with different commit_lsn and commit_idx values
      # The order here is intentionally mixed up
      events = [
        # LSN: 200, IDX: 2
        ConsumersFactory.insert_consumer_message!(
          consumer_id: consumer.id,
          message_kind: consumer.message_kind,
          commit_lsn: 200,
          commit_idx: 2
        ),
        # LSN: 100, IDX: 2
        ConsumersFactory.insert_consumer_message!(
          consumer_id: consumer.id,
          message_kind: consumer.message_kind,
          commit_lsn: 100,
          commit_idx: 2
        ),
        # LSN: 200, IDX: 1
        ConsumersFactory.insert_consumer_message!(
          consumer_id: consumer.id,
          message_kind: consumer.message_kind,
          commit_lsn: 200,
          commit_idx: 1
        ),
        # LSN: 100, IDX: 1
        ConsumersFactory.insert_consumer_message!(
          consumer_id: consumer.id,
          message_kind: consumer.message_kind,
          commit_lsn: 100,
          commit_idx: 1
        )
      ]

      # Expected order based on [commit_lsn, commit_idx] asc
      expected_ordered_ids = [
        # LSN: 100, IDX: 1
        Enum.at(events, 3).id,
        # LSN: 100, IDX: 2
        Enum.at(events, 1).id,
        # LSN: 200, IDX: 1
        Enum.at(events, 2).id,
        # LSN: 200, IDX: 2
        Enum.at(events, 0).id
      ]

      streamed_messages =
        consumer
        |> Consumers.stream_consumer_messages_for_consumer()
        |> Enum.to_list()

      streamed_ids = Enum.map(streamed_messages, & &1.id)
      assert streamed_ids == expected_ordered_ids
    end

    test "cursor-based pagination works correctly" do
      consumer = ConsumersFactory.insert_sink_consumer!()

      # Create 10 events with increasing LSNs
      Enum.map(1..10, fn i ->
        ConsumersFactory.insert_consumer_message!(
          consumer_id: consumer.id,
          message_kind: consumer.message_kind,
          commit_lsn: i * 100,
          commit_idx: 1
        )
      end)

      # Stream with a small batch size to force pagination
      batch_size = 3

      streamed_messages =
        consumer
        |> Consumers.stream_consumer_messages_for_consumer(batch_size: batch_size)
        |> Enum.to_list()

      # All 10 events should be retrieved in order
      assert length(streamed_messages) == 10

      # Check if they're in the correct LSN order
      streamed_lsns = Enum.map(streamed_messages, & &1.commit_lsn)
      assert streamed_lsns == Enum.map(1..10, fn i -> i * 100 end)
    end
  end

  describe "annotations size constraint" do
    test "fails when annotations exceed max size constraint" do
      # Generate a string larger than 8192 bytes (the max size constraint defined in the migration)
      large_annotation = String.duplicate("x", 8193)
      account = AccountsFactory.insert_account!()
      attrs = ConsumersFactory.sink_consumer_attrs(account_id: account.id, annotations: %{data: large_annotation})

      assert {:error, %Ecto.Changeset{} = changeset} = Consumers.create_sink_consumer(account.id, attrs)
      assert [annotations: {"annotations size limit exceeded", _}] = changeset.errors
    end
  end

  describe "consumer_partition_size_bytes/1" do
    test "returns the size of the consumer partition" do
      consumer = ConsumersFactory.insert_sink_consumer!()

      # Insert some messages to ensure the table exists and has data
      for _ <- 1..5 do
        ConsumersFactory.insert_consumer_message!(
          message_kind: consumer.message_kind,
          consumer_id: consumer.id
        )
      end

      # Check the size is positive
      {:ok, size} = Consumers.consumer_partition_size_bytes(consumer)
      assert is_integer(size)
      assert size > 0

      # Check that a non-existent consumer ID returns an error
      nonexistent_consumer = %{consumer | seq: Factory.unique_integer()}
      assert {:error, %Postgrex.Error{}} = Consumers.consumer_partition_size_bytes(nonexistent_consumer)
    end
  end

  describe "generate_group_id/2 and generate_group_id/3" do
    test "generates consistent group IDs between Message and ConsumerEvent" do
      consumer = ConsumersFactory.insert_sink_consumer!()
      database = DatabasesFactory.insert_postgres_database!()

      # Case 1: Simple primary key
      message = ReplicationFactory.postgres_message(action: :insert, ids: ["123"])
      event = ConsumersFactory.consumer_event(record_pks: ["123"])

      assert Consumers.generate_group_id(consumer, message) ==
               Consumers.generate_group_id(consumer, event, database)

      # Case 2: Multiple primary keys
      message_multi = ReplicationFactory.postgres_message(action: :insert, ids: ["123", "456"])
      event_multi = ConsumersFactory.consumer_event(record_pks: ["123", "456"])

      assert Consumers.generate_group_id(consumer, message_multi) ==
               Consumers.generate_group_id(consumer, event_multi, database)

      # Case 3: Empty primary keys
      message_empty = ReplicationFactory.postgres_message(action: :insert, ids: [])
      event_empty = ConsumersFactory.consumer_event(record_pks: [])

      assert Consumers.generate_group_id(consumer, message_empty) ==
               Consumers.generate_group_id(consumer, event_empty, database)

      # Case 4: With message grouping disabled
      consumer_no_grouping = ConsumersFactory.insert_sink_consumer!(message_grouping: false)

      assert Consumers.generate_group_id(consumer_no_grouping, message_empty) == nil
      assert Consumers.generate_group_id(consumer_no_grouping, event_empty, database) == nil
    end

    test "generates consistent group IDs with source tables and group columns" do
      table =
        DatabasesFactory.table(
          oid: 12_345,
          columns: [
            DatabasesFactory.column(attnum: 1, name: "col1"),
            DatabasesFactory.column(attnum: 2, name: "col2"),
            DatabasesFactory.column(attnum: 3, name: "col3")
          ]
        )

      consumer = ConsumersFactory.sink_consumer()
      database = DatabasesFactory.postgres_database(tables: [table])

      # Create source table with group column configuration
      source_table = %Sequin.Consumers.SourceTable{
        table_oid: 12_345,
        # Assuming column numbers 1 and 2 are used for grouping
        group_column_attnums: [1, 2]
      }

      consumer = %{consumer | source_tables: [source_table]}

      # Create message with matching fields
      fields = [
        %{column_attnum: 1, value: "user_1"},
        %{column_attnum: 2, value: "group_A"},
        %{column_attnum: 3, value: "other"}
      ]

      message =
        ReplicationFactory.postgres_message(
          action: :insert,
          table_oid: 12_345,
          fields: fields
        )

      # Create equivalent consumer event with matching record data
      event =
        ConsumersFactory.consumer_event(
          table_oid: 12_345,
          data: %ConsumerEventData{
            record: %{
              "col1" => "user_1",
              "col2" => "group_A",
              "col3" => "other"
            },
            action: :insert,
            metadata: %Metadata{
              table_name: "test_table",
              table_schema: "public"
            }
          }
        )

      # Both should generate the same group ID based on the specified columns
      group_id_from_message = Consumers.generate_group_id(consumer, message)
      group_id_from_event = Consumers.generate_group_id(consumer, event, database)

      assert group_id_from_message == group_id_from_event
      assert group_id_from_message == "user_1:group_A"
    end
  end

  describe "update_backfill/2 validates proper status transitions" do
    test "successfully pauses an active backfill" do
      backfill = ConsumersFactory.insert_active_backfill!()

      assert {:ok, paused_backfill} = Consumers.update_backfill(backfill, %{state: :paused})
      assert paused_backfill.state == :paused
      assert paused_backfill.id == backfill.id
    end

    test "successfully resumes a paused backfill" do
      backfill = ConsumersFactory.insert_backfill!(state: :paused)

      assert {:ok, resumed_backfill} = Consumers.update_backfill(backfill, %{state: :active})
      assert resumed_backfill.state == :active
      assert resumed_backfill.id == backfill.id
    end

    test "returns error when trying to pause non-active backfill" do
      completed_backfill = ConsumersFactory.insert_completed_backfill!()

      assert {:error, changeset} = Consumers.update_backfill(completed_backfill, %{state: :paused})
      assert changeset.valid? == false
    end
  end

  describe "enrich_messages!/4" do
    test "enriches a single message with the given enrichment function" do
      # Create a database with a table that has a primary key
      table =
        DatabasesFactory.table(
          oid: 12_345,
          columns: [
            DatabasesFactory.column(attnum: 1, name: "id", type: "integer", is_pk?: true),
            DatabasesFactory.column(attnum: 2, name: "name", type: "text", is_pk?: false)
          ]
        )

      database = DatabasesFactory.postgres_database(tables: [table])

      # Create a message with primary key data
      message =
        ConsumersFactory.consumer_event(
          table_oid: 12_345,
          data:
            ConsumersFactory.consumer_event_data(
              record: %{
                "id" => 1,
                "name" => "original"
              }
            )
        )

      # Create an enrichment function that adds data
      enrichment_function = %Function{
        function: %EnrichmentFunction{
          code: "SELECT id, 'enriched' as name, 'extra' as extra_field FROM unnest($1::int[]) id"
        }
      }

      # Mock the database query function
      query_fn = fn _db, _sql, _params ->
        {:ok,
         %Postgrex.Result{
           rows: [[1, "enriched", "extra"]],
           columns: ["id", "name", "extra_field"]
         }}
      end

      # Enrich the message
      enriched_message = Consumers.enrich_message!(database, enrichment_function, message, query_fn: query_fn)

      # Verify enrichment
      assert enriched_message.data.metadata.enrichment == %{
               "extra_field" => "extra",
               "id" => 1,
               "name" => "enriched"
             }
    end

    test "properly handles text values without hex encoding" do
      # Create a database with a table that has a primary key
      table =
        DatabasesFactory.table(
          oid: 12_345,
          columns: [
            DatabasesFactory.column(attnum: 1, name: "id", type: "integer", is_pk?: true),
            DatabasesFactory.column(attnum: 2, name: "name", type: "text", is_pk?: false)
          ]
        )

      database = DatabasesFactory.postgres_database(tables: [table])

      # Create a message with primary key data
      message =
        ConsumersFactory.consumer_event(
          table_oid: 12_345,
          data:
            ConsumersFactory.consumer_event_data(
              record: %{
                "id" => 1,
                "name" => "original"
              }
            )
        )

      # Create an enrichment function that returns text values
      enrichment_function = %Function{
        function: %EnrichmentFunction{
          code: "SELECT id, 'ABCDEFGHIJKLMNOP' as parent_name FROM unnest($1::int[]) id"
        }
      }

      query_fn = fn _db, _sql, _params ->
        # Postgrex would return the text as-is, but it's a binary in Elixir
        # This string should be exactly 16 bytes and triggers the UUID conversion bug
        text_value = "ABCDEFGHIJKLMNOP"
        assert byte_size(text_value) == 16, "Test string must be exactly 16 bytes"

        {:ok,
         %Postgrex.Result{
           rows: [[1, text_value]],
           columns: ["id", "parent_name"]
         }}
      end

      enriched_message = Consumers.enrich_message!(database, enrichment_function, message, query_fn: query_fn)

      # Check that the value does not get converted to a UUID by mistake
      assert enriched_message.data.metadata.enrichment["parent_name"] == "ABCDEFGHIJKLMNOP"
    end

    test "properly handles UUID binary values" do
      # Create a database with a table that has a primary key and UUID column
      table =
        DatabasesFactory.table(
          oid: 12_345,
          columns: [
            DatabasesFactory.column(attnum: 1, name: "id", type: "uuid", is_pk?: true),
            DatabasesFactory.column(attnum: 2, name: "name", type: "text", is_pk?: false)
          ]
        )

      database = DatabasesFactory.postgres_database(tables: [table])

      uuid = "123e4567-e89b-12d3-a456-426614174000"
      uuid_binary = Sequin.String.string_to_binary!(uuid)

      # Create a message with UUID primary key
      message =
        ConsumersFactory.consumer_event(
          table_oid: 12_345,
          data:
            ConsumersFactory.consumer_event_data(
              record: %{
                "id" => uuid,
                "name" => "original"
              }
            )
        )

      # Create an enrichment function that returns UUID values
      enrichment_function = %Function{
        function: %EnrichmentFunction{
          code: "SELECT $1 as id, 'enriched' as name"
        }
      }

      # Mock the database query function - simulate UUID binary from Postgrex
      query_fn = fn _db, _sql, _params ->
        {:ok,
         %Postgrex.Result{
           rows: [[uuid_binary, "enriched"]],
           columns: ["id", "name"]
         }}
      end

      # Enrich the message
      enriched_message = Consumers.enrich_message!(database, enrichment_function, message, query_fn: query_fn)

      # Verify enrichment - UUID should be properly converted from binary
      assert enriched_message.data.metadata.enrichment == %{
               "id" => uuid,
               "name" => "enriched"
             }
    end

    test "properly handles multiple messages" do
      # Create a database with a table that has a primary key
      table =
        DatabasesFactory.table(
          oid: 12_345,
          columns: [
            DatabasesFactory.column(attnum: 1, name: "id", type: "integer", is_pk?: true),
            DatabasesFactory.column(attnum: 2, name: "name", type: "text", is_pk?: false)
          ]
        )

      database = DatabasesFactory.postgres_database(tables: [table])

      # Create multiple messages
      messages = [
        ConsumersFactory.consumer_event(
          table_oid: 12_345,
          data: ConsumersFactory.consumer_event_data(record: %{"id" => 1, "name" => "original1"})
        ),
        ConsumersFactory.consumer_event(
          table_oid: 12_345,
          data: ConsumersFactory.consumer_event_data(record: %{"id" => 2, "name" => "original2"})
        )
      ]

      # Create an enrichment function
      enrichment_function = %Function{
        function: %EnrichmentFunction{
          code: "SELECT id, 'enriched' || id::text as name FROM unnest($1::int[]) id"
        }
      }

      # Mock the database query function
      query_fn = fn _db, _sql, _params ->
        {:ok,
         %Postgrex.Result{
           rows: [
             [1, "enriched1"],
             [2, "enriched2"]
           ],
           columns: ["id", "name"]
         }}
      end

      # Enrich the messages
      enriched_messages = Consumers.enrich_messages!(database, enrichment_function, messages, query_fn: query_fn)

      # Verify enrichments
      assert length(enriched_messages) == 2
      [msg1, msg2] = enriched_messages

      assert msg1.data.metadata.enrichment == %{"id" => 1, "name" => "enriched1"}
      assert msg2.data.metadata.enrichment == %{"id" => 2, "name" => "enriched2"}
    end

    test "succeeds if the enrichment function returns 0 rows for a message" do
      # Create a database with a table that has a primary key
      table =
        DatabasesFactory.table(
          oid: 12_345,
          columns: [
            DatabasesFactory.column(attnum: 1, name: "id", type: "integer", is_pk?: true),
            DatabasesFactory.column(attnum: 2, name: "name", type: "text", is_pk?: false)
          ]
        )

      database = DatabasesFactory.postgres_database(tables: [table])

      # Create a message
      message =
        ConsumersFactory.consumer_event(
          table_oid: 12_345,
          data: ConsumersFactory.consumer_event_data(record: %{"id" => 1, "name" => "original"})
        )

      # Create an enrichment function
      enrichment_function = %Function{
        function: %EnrichmentFunction{
          code: "SELECT id, name FROM (SELECT 2 as id, 'enriched' as name) t WHERE id = ANY($1::int[])"
        }
      }

      # Mock the database query function that returns no rows
      query_fn = fn _db, _sql, _params ->
        {:ok, %Postgrex.Result{rows: [], columns: ["id", "name"]}}
      end

      # Enrich the message - should return original message unchanged
      [enriched_message] = Consumers.enrich_messages!(database, enrichment_function, [message], query_fn: query_fn)

      # Verify the message is unchanged
      assert enriched_message.data.record["name"] == "original"
      assert enriched_message.data.record["id"] == 1
    end

    test "fails if the enrichment function returns > 1 rows for a message" do
      # Create a database with a table that has a primary key
      table =
        DatabasesFactory.table(
          oid: 12_345,
          columns: [
            DatabasesFactory.column(attnum: 1, name: "id", type: "integer", is_pk?: true),
            DatabasesFactory.column(attnum: 2, name: "name", type: "text", is_pk?: false)
          ]
        )

      database = DatabasesFactory.postgres_database(tables: [table])

      # Create a message
      message =
        ConsumersFactory.consumer_event(
          table_oid: 12_345,
          data: ConsumersFactory.consumer_event_data(record: %{"id" => 1, "name" => "original"})
        )

      # Create an enrichment function
      enrichment_function = %Function{
        function: %EnrichmentFunction{
          code: "SELECT * FROM (VALUES (1, 'enriched1'), (1, 'enriched2')) t(id, name)"
        }
      }

      # Mock the database query function that returns multiple rows for same ID
      query_fn = fn _db, _sql, _params ->
        {:ok,
         %Postgrex.Result{
           rows: [
             [1, "enriched1"],
             [1, "enriched2"]
           ],
           columns: ["id", "name"]
         }}
      end

      # Attempt to enrich the message - should raise an error
      assert_raise InvariantError, ~r/Expected 0 or 1 enrichment results, got 2/, fn ->
        Consumers.enrich_messages!(database, enrichment_function, [message], query_fn: query_fn)
      end
    end

    test "properly casts uuid primary keys" do
      # Create a database with a table that has a UUID primary key
      table =
        DatabasesFactory.table(
          oid: 12_345,
          columns: [
            DatabasesFactory.column(attnum: 1, name: "id", type: "uuid", is_pk?: true),
            DatabasesFactory.column(attnum: 2, name: "name", type: "text", is_pk?: false)
          ]
        )

      database = DatabasesFactory.postgres_database(tables: [table])

      uuid = Sequin.uuid4()
      as_binary = Sequin.String.string_to_binary!(uuid)

      # Create a message with UUID primary key
      message =
        ConsumersFactory.consumer_event(
          table_oid: 12_345,
          data:
            ConsumersFactory.consumer_event_data(
              record: %{
                "id" => uuid,
                "name" => "original"
              }
            )
        )

      # Create an enrichment function
      enrichment_function = %Function{
        function: %EnrichmentFunction{
          code: "SELECT id::uuid, 'enriched' as name FROM unnest($1::uuid[]) id"
        }
      }

      # Mock the database query function
      query_fn = fn _db, _sql, params ->
        # Verify the UUID is properly cast in the parameters
        assert [[^as_binary]] = params
        assert Sequin.String.uuid?(uuid)

        {:ok,
         %Postgrex.Result{
           rows: [[as_binary, "enriched"]],
           columns: ["id", "name"]
         }}
      end

      # Enrich the message
      [enriched_message] = Consumers.enrich_messages!(database, enrichment_function, [message], query_fn: query_fn)

      # Verify enrichment
      assert enriched_message.data.metadata.enrichment == %{"id" => uuid, "name" => "enriched"}
    end

    test "loads rows from postgres types to elixir types before merging into record" do
      # Create a database with a table that has a primary key
      table =
        DatabasesFactory.table(
          oid: 12_345,
          columns: [
            DatabasesFactory.column(attnum: 1, name: "id", type: "integer", is_pk?: true)
          ]
        )

      database = DatabasesFactory.postgres_database(tables: [table])

      # Create a message
      message =
        ConsumersFactory.consumer_event(
          table_oid: 12_345,
          data:
            ConsumersFactory.consumer_event_data(
              record: %{
                "id" => 1
              }
            )
        )

      uuid = Sequin.uuid4()
      as_binary = Sequin.String.string_to_binary!(uuid)
      datetime = DateTime.utc_now()

      # Create an enrichment function
      enrichment_function = %Function{
        function: %EnrichmentFunction{
          code: "SELECT id, o.account_id, o.created_at FROM unnest($1::int[]) id LEFT JOIN other_table o ON o.id = id"
        }
      }

      query_fn = fn _db, _sql, params ->
        assert [[1]] = params

        {:ok,
         %Postgrex.Result{
           rows: [[1, as_binary, datetime]],
           columns: ["id", "account_id", "created_at"]
         }}
      end

      # Enrich the message
      [enriched_message] = Consumers.enrich_messages!(database, enrichment_function, [message], query_fn: query_fn)

      # Verify enrichment
      assert enriched_message.data.metadata.enrichment == %{
               "id" => 1,
               "account_id" => uuid,
               "created_at" => datetime
             }
    end
  end
end

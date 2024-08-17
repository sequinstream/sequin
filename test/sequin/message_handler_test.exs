defmodule Sequin.MessageHandlerTest do
  use Sequin.DataCase, async: true

  alias Sequin.Consumers
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Replication.MessageHandler

  describe "handle_messages/3" do
    test "two messages with two consumers are fanned out to each consumer" do
      # Create two messages with specific table_oid
      message1 = ReplicationFactory.postgres_message(table_oid: 123)
      message2 = ReplicationFactory.postgres_message(table_oid: 123)

      # Create a source table with matching oid
      source_table = ConsumersFactory.source_table(oid: 123, column_filters: [])

      # Create two consumers with the matching source table
      consumer1 = ConsumersFactory.insert_http_push_consumer!(message_kind: :event, source_tables: [source_table])
      consumer2 = ConsumersFactory.insert_http_push_consumer!(message_kind: :event, source_tables: [source_table])

      # Create context with both consumers
      context = %MessageHandler.Context{consumers: [consumer1, consumer2]}

      # Handle messages
      {:ok, 4} = MessageHandler.handle_messages(context, [message1, message2])

      # Verify consumer events for consumer1
      consumer1_events = Consumers.list_consumer_events_for_consumer(consumer1.id)
      assert length(consumer1_events) == 2
      assert Enum.all?(consumer1_events, &(&1.consumer_id == consumer1.id))
      assert Enum.all?(consumer1_events, &(&1.table_oid == 123))

      # Verify consumer events for consumer2
      consumer2_events = Consumers.list_consumer_events_for_consumer(consumer2.id)
      assert length(consumer2_events) == 2
      assert Enum.all?(consumer2_events, &(&1.consumer_id == consumer2.id))
      assert Enum.all?(consumer2_events, &(&1.table_oid == 123))

      # Verify that the events contain the correct data
      all_events = consumer1_events ++ consumer2_events
      assert Enum.any?(all_events, &(&1.commit_lsn == DateTime.to_unix(message1.commit_timestamp, :microsecond)))
      assert Enum.any?(all_events, &(&1.commit_lsn == DateTime.to_unix(message2.commit_timestamp, :microsecond)))
    end

    test "inserts message for consumer with matching source table and no filters" do
      message = ReplicationFactory.postgres_message(table_oid: 123, action: :insert)
      source_table = ConsumersFactory.source_table(oid: 123, column_filters: [])
      consumer = ConsumersFactory.insert_http_push_consumer!(message_kind: :event, source_tables: [source_table])
      context = %MessageHandler.Context{consumers: [consumer]}

      {:ok, 1} = MessageHandler.handle_messages(context, [message])

      consumer_events = Consumers.list_consumer_events_for_consumer(consumer.id)
      assert length(consumer_events) == 1
      assert hd(consumer_events).table_oid == 123
      assert hd(consumer_events).consumer_id == consumer.id
    end

    test "does not insert message for consumer with non-matching source table" do
      message = ReplicationFactory.postgres_message(table_oid: 123)
      source_table = ConsumersFactory.source_table(oid: 456)
      consumer = ConsumersFactory.insert_http_push_consumer!(message_kind: :event, source_tables: [source_table])
      context = %MessageHandler.Context{consumers: [consumer]}

      {:ok, 0} = MessageHandler.handle_messages(context, [message])

      consumer_events = Consumers.list_consumer_events_for_consumer(consumer.id)
      assert Enum.empty?(consumer_events)
    end

    test "inserts message for consumer with matching source table and passing filters" do
      message = ReplicationFactory.postgres_message(table_oid: 123, action: :insert)
      column_filter = ConsumersFactory.column_filter(column_attnum: 1, operator: :==, value: "test")
      source_table = ConsumersFactory.source_table(oid: 123, column_filters: [column_filter])
      consumer = ConsumersFactory.insert_http_push_consumer!(message_kind: :event, source_tables: [source_table])

      test_field = ReplicationFactory.field(column_attnum: 1, value: "test")
      message = %{message | fields: [test_field | message.fields]}

      context = %MessageHandler.Context{consumers: [consumer]}

      {:ok, 1} = MessageHandler.handle_messages(context, [message])

      consumer_events = Consumers.list_consumer_events_for_consumer(consumer.id)
      assert length(consumer_events) == 1
      assert hd(consumer_events).table_oid == 123
      assert hd(consumer_events).consumer_id == consumer.id
    end

    test "does not insert message for consumer with matching source table but failing filters" do
      message = ReplicationFactory.postgres_message(table_oid: 123, action: :insert)
      column_filter = ConsumersFactory.column_filter(column_attnum: 1, operator: :==, value: "test")
      source_table = ConsumersFactory.source_table(oid: 123, column_filters: [column_filter])
      consumer = ConsumersFactory.insert_http_push_consumer!(message_kind: :event, source_tables: [source_table])

      # Ensure the message has a non-matching field for the filter
      message = %{message | fields: [%{column_attnum: 1, value: "not_test"} | message.fields]}

      context = %MessageHandler.Context{consumers: [consumer]}

      {:ok, 0} = MessageHandler.handle_messages(context, [message])

      consumer_events = Consumers.list_consumer_events_for_consumer(consumer.id)
      assert Enum.empty?(consumer_events)
    end

    test "fans out messages correctly for multiple consumers with different source table and filter combinations" do
      message1 = ReplicationFactory.postgres_message(table_oid: 123, action: :insert)
      message2 = ReplicationFactory.postgres_message(table_oid: 456, action: :update)

      source_table1 = ConsumersFactory.source_table(oid: 123, column_filters: [])
      source_table2 = ConsumersFactory.source_table(oid: 456, column_filters: [])

      consumer1 = ConsumersFactory.insert_http_push_consumer!(message_kind: :event, source_tables: [source_table1])
      consumer2 = ConsumersFactory.insert_http_push_consumer!(message_kind: :event, source_tables: [source_table2])

      consumer3 =
        ConsumersFactory.insert_http_push_consumer!(message_kind: :event, source_tables: [source_table1, source_table2])

      context = %MessageHandler.Context{consumers: [consumer1, consumer2, consumer3]}

      {:ok, 4} = MessageHandler.handle_messages(context, [message1, message2])

      consumer1_events = Consumers.list_consumer_events_for_consumer(consumer1.id)
      consumer2_events = Consumers.list_consumer_events_for_consumer(consumer2.id)
      consumer3_events = Consumers.list_consumer_events_for_consumer(consumer3.id)

      assert length(consumer1_events) == 1
      assert hd(consumer1_events).table_oid == 123

      assert length(consumer2_events) == 1
      assert hd(consumer2_events).table_oid == 456

      assert length(consumer3_events) == 2
      assert Enum.any?(consumer3_events, &(&1.table_oid == 123))
      assert Enum.any?(consumer3_events, &(&1.table_oid == 456))
    end
  end
end

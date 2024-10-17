defmodule Sequin.MessageHandlerTest do
  use Sequin.DataCase, async: true

  alias Sequin.Consumers
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Replication
  alias Sequin.Replication.MessageHandler

  describe "handle_messages/2" do
    test "handles message_kind: event correctly" do
      message = ReplicationFactory.postgres_message(table_oid: 123, action: :insert)
      source_table = ConsumersFactory.source_table(oid: 123, column_filters: [])
      consumer = ConsumersFactory.insert_consumer!(message_kind: :event, source_tables: [source_table])
      context = %MessageHandler.Context{consumers: [consumer]}

      {:ok, 1} = MessageHandler.handle_messages(context, [message])

      [event] = Consumers.list_consumer_events_for_consumer(consumer.id)
      assert event.consumer_id == consumer.id
      assert event.table_oid == 123
      assert event.commit_lsn == DateTime.to_unix(message.commit_timestamp, :microsecond)
      assert event.record_pks == Enum.map(message.ids, &to_string/1)
      assert event.data.action == :insert
      assert event.data.record == fields_to_map(message.fields)
      assert event.data.changes == nil
      assert event.data.metadata.table_name == message.table_name
      assert event.data.metadata.table_schema == message.table_schema
      assert event.data.metadata.commit_timestamp == message.commit_timestamp
    end

    test "handles message_kind: record correctly" do
      message = ReplicationFactory.postgres_message(table_oid: 456, action: :update)
      source_table = ConsumersFactory.source_table(oid: 456, column_filters: [])
      consumer = ConsumersFactory.insert_consumer!(message_kind: :record, source_tables: [source_table])
      context = %MessageHandler.Context{consumers: [consumer]}

      {:ok, 1} = MessageHandler.handle_messages(context, [message])

      [record] = Consumers.list_consumer_records_for_consumer(consumer.id)
      assert record.consumer_id == consumer.id
      assert record.table_oid == 456
      assert record.commit_lsn == DateTime.to_unix(message.commit_timestamp, :microsecond)
      assert record.record_pks == Enum.map(message.ids, &to_string/1)
      assert record.group_id == Enum.join(message.ids, ",")
      assert record.state == :available
    end

    test "fans out messages correctly for mixed message_kind consumers and wal_pipelines" do
      message1 = ReplicationFactory.postgres_message(table_oid: 123, action: :insert)
      message2 = ReplicationFactory.postgres_message(table_oid: 456, action: :update)

      source_table1 = ConsumersFactory.source_table(oid: 123, column_filters: [])
      source_table2 = ConsumersFactory.source_table(oid: 456, column_filters: [])

      consumer1 = ConsumersFactory.insert_consumer!(message_kind: :event, source_tables: [source_table1])
      consumer2 = ConsumersFactory.insert_consumer!(message_kind: :record, source_tables: [source_table2])
      consumer3 = ConsumersFactory.insert_consumer!(message_kind: :event, source_tables: [source_table1, source_table2])
      wal_pipeline = ReplicationFactory.insert_wal_pipeline!(source_tables: [source_table1, source_table2])

      context = %MessageHandler.Context{
        consumers: [consumer1, consumer2, consumer3],
        wal_pipelines: [wal_pipeline]
      }

      {:ok, 6} = MessageHandler.handle_messages(context, [message1, message2])

      consumer1_messages = list_messages(consumer1.id)
      consumer2_messages = list_messages(consumer2.id)
      consumer3_messages = list_messages(consumer3.id)
      wal_events = Replication.list_wal_events(wal_pipeline.id)

      assert length(consumer1_messages) == 1
      assert hd(consumer1_messages).table_oid == 123

      assert length(consumer2_messages) == 1
      assert hd(consumer2_messages).table_oid == 456

      assert length(consumer3_messages) == 2
      assert Enum.any?(consumer3_messages, &(&1.table_oid == 123))
      assert Enum.any?(consumer3_messages, &(&1.table_oid == 456))

      assert length(wal_events) == 2
    end

    test "two messages with two consumers and one wal_pipeline are fanned out to each" do
      message1 = ReplicationFactory.postgres_message(table_oid: 123, action: :insert)
      message2 = ReplicationFactory.postgres_message(table_oid: 123, action: :insert)
      source_table = ConsumersFactory.source_table(oid: 123, column_filters: [])
      consumer1 = ConsumersFactory.insert_consumer!(source_tables: [source_table])
      consumer2 = ConsumersFactory.insert_consumer!(source_tables: [source_table])
      wal_pipeline = ReplicationFactory.insert_wal_pipeline!(source_tables: [source_table])

      context = %MessageHandler.Context{
        consumers: [consumer1, consumer2],
        wal_pipelines: [wal_pipeline]
      }

      {:ok, 6} = MessageHandler.handle_messages(context, [message1, message2])

      consumer1_messages = list_messages(consumer1.id)
      consumer2_messages = list_messages(consumer2.id)
      wal_events = Replication.list_wal_events(wal_pipeline.id)

      assert length(consumer1_messages) == 2
      assert Enum.all?(consumer1_messages, &(&1.consumer_id == consumer1.id))
      assert Enum.all?(consumer1_messages, &(&1.table_oid == 123))

      assert length(consumer2_messages) == 2
      assert Enum.all?(consumer2_messages, &(&1.consumer_id == consumer2.id))
      assert Enum.all?(consumer2_messages, &(&1.table_oid == 123))

      assert length(wal_events) == 2

      all_messages = consumer1_messages ++ consumer2_messages ++ wal_events
      assert Enum.any?(all_messages, &(&1.commit_lsn == DateTime.to_unix(message1.commit_timestamp, :microsecond)))
      assert Enum.any?(all_messages, &(&1.commit_lsn == DateTime.to_unix(message2.commit_timestamp, :microsecond)))
    end

    test "inserts message for consumer with matching source table and no filters" do
      message = ReplicationFactory.postgres_message(table_oid: 123, action: :insert)
      source_table = ConsumersFactory.source_table(oid: 123, column_filters: [])
      consumer = ConsumersFactory.insert_consumer!(source_tables: [source_table])
      context = %MessageHandler.Context{consumers: [consumer]}

      {:ok, 1} = MessageHandler.handle_messages(context, [message])

      messages = list_messages(consumer.id)
      assert length(messages) == 1
      assert hd(messages).table_oid == 123
      assert hd(messages).consumer_id == consumer.id
    end

    test "does not insert message for consumer with non-matching source table" do
      message = ReplicationFactory.postgres_message(table_oid: 123)
      source_table = ConsumersFactory.source_table(oid: 456)
      consumer = ConsumersFactory.insert_consumer!(source_tables: [source_table])
      context = %MessageHandler.Context{consumers: [consumer]}

      {:ok, 0} = MessageHandler.handle_messages(context, [message])

      messages = list_messages(consumer.id)
      assert Enum.empty?(messages)
    end

    test "inserts message for consumer with matching source table and passing filters" do
      message = ReplicationFactory.postgres_message(table_oid: 123, action: :insert)

      column_filter =
        ConsumersFactory.column_filter(
          column_attnum: 1,
          operator: :==,
          value: %{__type__: :string, value: "test"}
        )

      source_table = ConsumersFactory.source_table(oid: 123, column_filters: [column_filter])
      consumer = ConsumersFactory.insert_consumer!(source_tables: [source_table])

      test_field = ReplicationFactory.field(column_attnum: 1, value: "test")
      message = %{message | fields: [test_field | message.fields]}

      context = %MessageHandler.Context{consumers: [consumer]}

      {:ok, 1} = MessageHandler.handle_messages(context, [message])

      messages = list_messages(consumer.id)
      assert length(messages) == 1
      assert hd(messages).table_oid == 123
      assert hd(messages).consumer_id == consumer.id
    end

    test "does not insert message for consumer with matching source table but failing filters" do
      message = ReplicationFactory.postgres_message(table_oid: 123, action: :insert)

      column_filter =
        ConsumersFactory.column_filter(
          column_attnum: 1,
          operator: :==,
          value: %{__type__: :string, value: "test"}
        )

      source_table = ConsumersFactory.source_table(oid: 123, column_filters: [column_filter])
      consumer = ConsumersFactory.insert_consumer!(source_tables: [source_table])

      # Ensure the message has a non-matching field for the filter
      message = %{message | fields: [%{column_attnum: 1, value: "not_test"} | message.fields]}

      context = %MessageHandler.Context{consumers: [consumer]}

      {:ok, 0} = MessageHandler.handle_messages(context, [message])

      messages = list_messages(consumer.id)
      assert Enum.empty?(messages)
    end

    test "handles wal_pipelines correctly" do
      insert_message = ReplicationFactory.postgres_message(table_oid: 123, action: :insert)

      update_message =
        ReplicationFactory.postgres_message(
          table_oid: 123,
          action: :update,
          old_fields: [ReplicationFactory.field(column_name: "name", value: "old_name")]
        )

      source_table = ConsumersFactory.source_table(oid: 123, column_filters: [])
      wal_pipeline = ReplicationFactory.insert_wal_pipeline!(source_tables: [source_table])
      context = %MessageHandler.Context{wal_pipelines: [wal_pipeline]}

      {:ok, 2} = MessageHandler.handle_messages(context, [insert_message, update_message])

      {[insert_event], [update_event]} =
        wal_pipeline.id |> Replication.list_wal_events() |> Enum.split_with(&(&1.action == :insert))

      assert insert_event.action == :insert
      assert insert_event.wal_pipeline_id == wal_pipeline.id
      assert insert_event.commit_lsn == DateTime.to_unix(insert_message.commit_timestamp, :microsecond)
      assert insert_event.record_pks == Enum.map(insert_message.ids, &to_string/1)
      assert insert_event.replication_message_trace_id == insert_message.trace_id
      assert insert_event.source_table_oid == insert_message.table_oid
      assert insert_event.record == fields_to_map(insert_message.fields)
      assert insert_event.changes == nil
      assert insert_event.committed_at == insert_message.commit_timestamp

      assert update_event.action == :update
      assert update_event.changes == %{"name" => "old_name"}
    end

    test "inserts wal_event for wal_pipeline with matching source table and no filters" do
      message = ReplicationFactory.postgres_message(table_oid: 123)
      source_table = ConsumersFactory.source_table(oid: 123, column_filters: [])
      wal_pipeline = ReplicationFactory.insert_wal_pipeline!(source_tables: [source_table])
      context = %MessageHandler.Context{wal_pipelines: [wal_pipeline]}

      {:ok, 1} = MessageHandler.handle_messages(context, [message])

      wal_events = Replication.list_wal_events(wal_pipeline.id)
      assert length(wal_events) == 1
      assert hd(wal_events).wal_pipeline_id == wal_pipeline.id
    end

    test "does not insert wal_event for wal_pipeline with non-matching source table" do
      message = ReplicationFactory.postgres_message(table_oid: 123)
      source_table = ConsumersFactory.source_table(oid: 456)
      wal_pipeline = ReplicationFactory.insert_wal_pipeline!(source_tables: [source_table])
      context = %MessageHandler.Context{wal_pipelines: [wal_pipeline]}

      {:ok, 0} = MessageHandler.handle_messages(context, [message])

      wal_events = Replication.list_wal_events(wal_pipeline.id)
      assert Enum.empty?(wal_events)
    end

    test "inserts wal_event for pipeline with matching source table and passing filters" do
      message = ReplicationFactory.postgres_message(table_oid: 123)

      column_filter =
        ConsumersFactory.column_filter(
          column_attnum: 1,
          operator: :==,
          value: %{__type__: :string, value: "test"}
        )

      source_table = ConsumersFactory.source_table(oid: 123, column_filters: [column_filter])
      wal_pipeline = ReplicationFactory.insert_wal_pipeline!(source_tables: [source_table])

      test_field = ReplicationFactory.field(column_attnum: 1, value: "test")
      message = %{message | fields: [test_field | message.fields], old_fields: [test_field | message.old_fields]}

      context = %MessageHandler.Context{wal_pipelines: [wal_pipeline]}

      {:ok, 1} = MessageHandler.handle_messages(context, [message])

      wal_events = Replication.list_wal_events(wal_pipeline.id)
      assert length(wal_events) == 1
      assert hd(wal_events).wal_pipeline_id == wal_pipeline.id
    end

    test "does not insert wal_event for pipeline with matching source table but failing filters" do
      message = ReplicationFactory.postgres_message(table_oid: 123, action: :insert)

      column_filter =
        ConsumersFactory.column_filter(
          column_attnum: 1,
          operator: :==,
          value: %{__type__: :string, value: "test"}
        )

      source_table = ConsumersFactory.source_table(oid: 123, column_filters: [column_filter])
      wal_pipeline = ReplicationFactory.insert_wal_pipeline!(source_tables: [source_table])

      # Ensure the message has a non-matching field for the filter
      message = %{message | fields: [%{column_attnum: 1, value: "not_test"} | message.fields]}

      context = %MessageHandler.Context{wal_pipelines: [wal_pipeline]}

      {:ok, 0} = MessageHandler.handle_messages(context, [message])

      wal_events = Replication.list_wal_events(wal_pipeline.id)
      assert Enum.empty?(wal_events)
    end

    test "sets group_id based on PKs when group_column_attnums is nil" do
      message = ReplicationFactory.postgres_message(table_oid: 123, action: :insert, ids: [1, 2])
      source_table = ConsumersFactory.source_table(oid: 123, column_filters: [], group_column_attnums: nil)
      consumer = ConsumersFactory.insert_consumer!(message_kind: :record, source_tables: [source_table])
      context = %MessageHandler.Context{consumers: [consumer]}

      {:ok, 1} = MessageHandler.handle_messages(context, [message])

      [record] = Consumers.list_consumer_records_for_consumer(consumer.id)
      assert record.group_id == "1,2"
    end

    test "sets group_id based on group_column_attnums when it's set" do
      message =
        ReplicationFactory.postgres_message(
          table_oid: 123,
          action: :insert,
          ids: [1, 2],
          fields: [
            ReplicationFactory.field(column_attnum: 1, column_name: "id", value: 1),
            ReplicationFactory.field(column_attnum: 2, column_name: "group", value: "A"),
            ReplicationFactory.field(column_attnum: 3, column_name: "name", value: "Test")
          ]
        )

      source_table = ConsumersFactory.source_table(oid: 123, column_filters: [], group_column_attnums: [2])
      consumer = ConsumersFactory.insert_consumer!(message_kind: :record, source_tables: [source_table])
      context = %MessageHandler.Context{consumers: [consumer]}

      {:ok, 1} = MessageHandler.handle_messages(context, [message])

      [record] = Consumers.list_consumer_records_for_consumer(consumer.id)
      assert record.group_id == "A"
    end
  end

  defp list_messages(consumer_id) do
    events = Consumers.list_consumer_events_for_consumer(consumer_id)
    records = Consumers.list_consumer_records_for_consumer(consumer_id)
    events ++ records
  end

  defp fields_to_map(fields) do
    Map.new(fields, fn %{column_name: name, value: value} -> {name, value} end)
  end
end

defmodule Sequin.MessageHandlerTest do
  use Sequin.DataCase, async: false

  import ExUnit.CaptureLog

  alias Sequin.Constants
  alias Sequin.Consumers
  alias Sequin.DatabasesRuntime.SlotProcessor.MessageHandler
  alias Sequin.Factory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Mocks.TableReaderServerMock
  alias Sequin.Replication

  describe "handle_messages/2" do
    test "handles message_kind: event correctly" do
      message = ReplicationFactory.postgres_message(table_oid: 123, action: :insert)
      source_table = ConsumersFactory.source_table(oid: 123, column_filters: [])
      consumer = ConsumersFactory.insert_sink_consumer!(message_kind: :event, source_tables: [source_table])
      consumer = Repo.preload(consumer, :postgres_database)
      context = %MessageHandler.Context{consumers: [consumer], replication_slot_id: UUID.uuid4()}

      {:ok, 1, _} = MessageHandler.handle_messages(context, [message])

      [event] = Consumers.list_consumer_events_for_consumer(consumer.id)
      assert event.consumer_id == consumer.id
      assert event.table_oid == 123
      assert event.commit_lsn == message.commit_lsn
      assert event.record_pks == Enum.map(message.ids, &to_string/1)
      assert event.data.action == :insert
      assert event.data.record == fields_to_map(message.fields)
      assert event.data.changes == nil
      assert event.data.metadata.table_name == message.table_name
      assert event.data.metadata.table_schema == message.table_schema
      assert event.data.metadata.commit_timestamp == message.commit_timestamp
      assert event.data.metadata.database_name == consumer.postgres_database.name
      assert event.seq == message.seq
    end

    test "handles message_kind: record correctly" do
      message = ReplicationFactory.postgres_message(table_oid: 456, action: :update)
      source_table = ConsumersFactory.source_table(oid: 456, column_filters: [])
      consumer = ConsumersFactory.insert_sink_consumer!(message_kind: :record, source_tables: [source_table])
      context = %MessageHandler.Context{consumers: [consumer], replication_slot_id: UUID.uuid4()}

      {:ok, 1, _} = MessageHandler.handle_messages(context, [message])

      [record] = Consumers.list_consumer_records_for_consumer(consumer.id)
      assert record.consumer_id == consumer.id
      assert record.table_oid == 456
      assert record.commit_lsn == message.commit_lsn
      assert record.record_pks == Enum.map(message.ids, &to_string/1)
      assert record.group_id == Enum.join(message.ids, ",")
      assert record.state == :available
      assert record.seq == message.seq
    end

    test "fans out messages correctly for mixed message_kind consumers and wal_pipelines" do
      message1 = ReplicationFactory.postgres_message(table_oid: 123, action: :insert)
      message2 = ReplicationFactory.postgres_message(table_oid: 456, action: :update)

      source_table1 = ConsumersFactory.source_table(oid: 123, column_filters: [])
      source_table2 = ConsumersFactory.source_table(oid: 456, column_filters: [])

      consumer1 = ConsumersFactory.insert_sink_consumer!(message_kind: :event, source_tables: [source_table1])
      consumer2 = ConsumersFactory.insert_sink_consumer!(message_kind: :record, source_tables: [source_table2])

      consumer3 =
        ConsumersFactory.insert_sink_consumer!(message_kind: :event, source_tables: [source_table1, source_table2])

      consumer1 = Repo.preload(consumer1, :postgres_database)
      consumer2 = Repo.preload(consumer2, :postgres_database)
      consumer3 = Repo.preload(consumer3, :postgres_database)

      wal_pipeline = ReplicationFactory.insert_wal_pipeline!(source_tables: [source_table1, source_table2])

      context = %MessageHandler.Context{
        consumers: [consumer1, consumer2, consumer3],
        wal_pipelines: [wal_pipeline],
        replication_slot_id: UUID.uuid4()
      }

      {:ok, 6, _} = MessageHandler.handle_messages(context, [message1, message2])

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
      consumer1 = ConsumersFactory.insert_sink_consumer!(source_tables: [source_table])
      consumer2 = ConsumersFactory.insert_sink_consumer!(source_tables: [source_table])
      consumer1 = Repo.preload(consumer1, :postgres_database)
      consumer2 = Repo.preload(consumer2, :postgres_database)
      wal_pipeline = ReplicationFactory.insert_wal_pipeline!(source_tables: [source_table])

      context = %MessageHandler.Context{
        consumers: [consumer1, consumer2],
        wal_pipelines: [wal_pipeline],
        replication_slot_id: UUID.uuid4()
      }

      {:ok, 6, _} = MessageHandler.handle_messages(context, [message1, message2])

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
      assert Enum.any?(all_messages, &(&1.commit_lsn == message1.commit_lsn))
      assert Enum.any?(all_messages, &(&1.commit_lsn == message2.commit_lsn))
    end

    test "inserts message for consumer with matching source table and no filters" do
      message = ReplicationFactory.postgres_message(table_oid: 123, action: :insert)
      source_table = ConsumersFactory.source_table(oid: 123, column_filters: [])
      consumer = ConsumersFactory.insert_sink_consumer!(source_tables: [source_table])
      consumer = Repo.preload(consumer, :postgres_database)
      context = %MessageHandler.Context{consumers: [consumer], replication_slot_id: UUID.uuid4()}

      {:ok, 1, _} = MessageHandler.handle_messages(context, [message])

      messages = list_messages(consumer.id)
      assert length(messages) == 1
      assert hd(messages).table_oid == 123
      assert hd(messages).consumer_id == consumer.id
    end

    test "does not insert message for consumer with non-matching source table" do
      message = ReplicationFactory.postgres_message(table_oid: 123)
      source_table = ConsumersFactory.source_table(oid: 456)
      consumer = ConsumersFactory.insert_sink_consumer!(source_tables: [source_table])
      context = %MessageHandler.Context{consumers: [consumer], replication_slot_id: UUID.uuid4()}

      {:ok, 0, _} = MessageHandler.handle_messages(context, [message])

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
      consumer = ConsumersFactory.insert_sink_consumer!(source_tables: [source_table])
      consumer = Repo.preload(consumer, :postgres_database)
      test_field = ReplicationFactory.field(column_attnum: 1, value: "test")
      message = %{message | fields: [test_field | message.fields]}

      context = %MessageHandler.Context{consumers: [consumer], replication_slot_id: UUID.uuid4()}

      {:ok, 1, _} = MessageHandler.handle_messages(context, [message])

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
      consumer = ConsumersFactory.insert_sink_consumer!(source_tables: [source_table])

      # Ensure the message has a non-matching field for the filter
      message = %{message | fields: [%{column_attnum: 1, value: "not_test"} | message.fields]}

      context = %MessageHandler.Context{consumers: [consumer], replication_slot_id: UUID.uuid4()}

      {:ok, 0, _} = MessageHandler.handle_messages(context, [message])

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
      context = %MessageHandler.Context{wal_pipelines: [wal_pipeline], replication_slot_id: UUID.uuid4()}

      {:ok, 2, _} = MessageHandler.handle_messages(context, [insert_message, update_message])

      {[insert_event], [update_event]} =
        wal_pipeline.id |> Replication.list_wal_events() |> Enum.split_with(&(&1.action == :insert))

      assert insert_event.action == :insert
      assert insert_event.wal_pipeline_id == wal_pipeline.id
      assert insert_event.commit_lsn == insert_message.commit_lsn
      assert insert_event.record_pks == Enum.map(insert_message.ids, &to_string/1)
      assert insert_event.replication_message_trace_id == insert_message.trace_id
      assert insert_event.source_table_oid == insert_message.table_oid
      assert insert_event.record == fields_to_map(insert_message.fields)
      assert insert_event.changes == nil
      assert insert_event.committed_at == insert_message.commit_timestamp
      assert insert_event.seq == insert_message.seq

      assert update_event.action == :update
      assert update_event.changes == %{"name" => "old_name"}
    end

    test "inserts wal_event for wal_pipeline with matching source table and no filters" do
      message = ReplicationFactory.postgres_message(table_oid: 123)
      source_table = ConsumersFactory.source_table(oid: 123, column_filters: [])
      wal_pipeline = ReplicationFactory.insert_wal_pipeline!(source_tables: [source_table])
      context = %MessageHandler.Context{wal_pipelines: [wal_pipeline], replication_slot_id: UUID.uuid4()}

      {:ok, 1, _} = MessageHandler.handle_messages(context, [message])

      wal_events = Replication.list_wal_events(wal_pipeline.id)
      assert length(wal_events) == 1
      assert hd(wal_events).wal_pipeline_id == wal_pipeline.id
    end

    test "does not insert wal_event for wal_pipeline with non-matching source table" do
      message = ReplicationFactory.postgres_message(table_oid: 123)
      source_table = ConsumersFactory.source_table(oid: 456)
      wal_pipeline = ReplicationFactory.insert_wal_pipeline!(source_tables: [source_table])
      context = %MessageHandler.Context{wal_pipelines: [wal_pipeline], replication_slot_id: UUID.uuid4()}

      {:ok, 0, _} = MessageHandler.handle_messages(context, [message])

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

      context = %MessageHandler.Context{wal_pipelines: [wal_pipeline], replication_slot_id: UUID.uuid4()}

      {:ok, 1, _} = MessageHandler.handle_messages(context, [message])

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

      context = %MessageHandler.Context{wal_pipelines: [wal_pipeline], replication_slot_id: UUID.uuid4()}

      {:ok, 0, _} = MessageHandler.handle_messages(context, [message])

      wal_events = Replication.list_wal_events(wal_pipeline.id)
      assert Enum.empty?(wal_events)
    end

    test "sets group_id based on PKs when group_column_attnums is nil" do
      message = ReplicationFactory.postgres_message(table_oid: 123, action: :insert, ids: [1, 2])
      source_table = ConsumersFactory.source_table(oid: 123, column_filters: [], group_column_attnums: nil)
      consumer = ConsumersFactory.insert_sink_consumer!(message_kind: :record, source_tables: [source_table])
      context = %MessageHandler.Context{consumers: [consumer], replication_slot_id: UUID.uuid4()}

      {:ok, 1, _} = MessageHandler.handle_messages(context, [message])

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
      consumer = ConsumersFactory.insert_sink_consumer!(message_kind: :record, source_tables: [source_table])
      context = %MessageHandler.Context{consumers: [consumer], replication_slot_id: UUID.uuid4()}

      {:ok, 1, _} = MessageHandler.handle_messages(context, [message])

      [record] = Consumers.list_consumer_records_for_consumer(consumer.id)
      assert record.group_id == "A"
    end

    test "puts the largest sequence number into last_processed_seq" do
      # Messages should be ordered, but mess it up just in case
      messages =
        for seq <- [1, 2, 5, 3] do
          ReplicationFactory.postgres_message(table_oid: 123, action: :insert, seq: seq)
        end

      source_table = ConsumersFactory.source_table(oid: 123, column_filters: [])
      consumer = ConsumersFactory.insert_sink_consumer!(source_tables: [source_table])
      consumer = Repo.preload(consumer, :postgres_database)
      replication_slot_id = Factory.uuid()
      context = %MessageHandler.Context{consumers: [consumer], replication_slot_id: replication_slot_id}

      {:ok, 4, _} = MessageHandler.handle_messages(context, messages)

      assert {:ok, 5} = Replication.last_processed_seq(replication_slot_id)
    end

    test "updates table reader batch primary key values correctly" do
      # Create three batches with different table OIDs
      batch1 = %MessageHandler.BatchState{
        batch_id: "batch1",
        table_oid: 123,
        backfill_id: "backfill1",
        seq: 1,
        # Some existing PKs
        primary_key_values: MapSet.new([["1"], ["2"]])
      }

      batch2 = %MessageHandler.BatchState{
        batch_id: "batch2",
        table_oid: 456,
        backfill_id: "backfill1",
        seq: 2,
        # Empty initial set
        primary_key_values: MapSet.new()
      }

      batch3 = %MessageHandler.BatchState{
        batch_id: "batch3",
        table_oid: 789,
        backfill_id: "backfill2",
        seq: 3,
        # One existing PK
        primary_key_values: MapSet.new([["10"]])
      }

      context = %MessageHandler.Context{
        consumers: [],
        replication_slot_id: UUID.uuid4(),
        table_reader_batches: [batch1, batch2, batch3]
      }

      # Create messages that should match different batches
      messages = [
        # Should match batch1
        ReplicationFactory.postgres_message(table_oid: 123, ids: [3]),
        ReplicationFactory.postgres_message(table_oid: 123, ids: [4]),
        # Should match batch1 (with overlap)
        ReplicationFactory.postgres_message(table_oid: 123, ids: [4]),
        ReplicationFactory.postgres_message(table_oid: 123, ids: [5]),
        # Should match batch2
        ReplicationFactory.postgres_message(table_oid: 456, ids: [100]),
        ReplicationFactory.postgres_message(table_oid: 456, ids: [101]),
        # Should match no batch
        ReplicationFactory.postgres_message(table_oid: 999, ids: [200]),
        ReplicationFactory.postgres_message(table_oid: 999, ids: [201])
      ]

      {:ok, _count, new_context} = MessageHandler.handle_messages(context, messages)

      # Find updated batches
      updated_batch1 = Enum.find(new_context.table_reader_batches, &(&1.batch_id == "batch1"))
      updated_batch2 = Enum.find(new_context.table_reader_batches, &(&1.batch_id == "batch2"))
      updated_batch3 = Enum.find(new_context.table_reader_batches, &(&1.batch_id == "batch3"))

      # Verify batch1 has original PKs plus new ones (without duplicates)
      assert MapSet.equal?(
               updated_batch1.primary_key_values,
               MapSet.new([["1"], ["2"], ["3"], ["4"], ["5"]])
             )

      # Verify batch2 has only the new PKs
      assert MapSet.equal?(
               updated_batch2.primary_key_values,
               MapSet.new([["100"], ["101"]])
             )

      # Verify batch3 is unchanged
      assert MapSet.equal?(
               updated_batch3.primary_key_values,
               MapSet.new([["10"]])
             )
    end
  end

  describe "handle_logical_message/3" do
    test "handles low watermark message by adding new batch state while preserving existing batches" do
      # Create an existing batch in the context
      existing_batch = %MessageHandler.BatchState{
        batch_id: "existing-batch",
        table_oid: 789,
        backfill_id: "existing-backfill",
        seq: 42,
        primary_key_values: MapSet.new([["1"], ["2"]])
      }

      context = %MessageHandler.Context{
        table_reader_batches: [existing_batch]
      }

      # Create a low watermark message
      message =
        ReplicationFactory.postgres_logical_message(%{
          prefix: Constants.backfill_batch_low_watermark(),
          content:
            Jason.encode!(%{
              "batch_id" => "new-batch",
              "table_oid" => 123,
              "backfill_id" => "new-backfill"
            })
        })

      # Process the message
      new_context = MessageHandler.handle_logical_message(context, 43, message)

      # Assert we have both batches
      assert length(new_context.table_reader_batches) == 2

      # Assert the existing batch is unchanged
      [existing_batch_updated] =
        Enum.filter(new_context.table_reader_batches, &(&1.batch_id == "existing-batch"))

      assert existing_batch_updated == existing_batch

      # Assert the new batch was created correctly
      [new_batch] =
        Enum.filter(new_context.table_reader_batches, &(&1.batch_id == "new-batch"))

      assert new_batch.batch_id == "new-batch"
      assert new_batch.table_oid == 123
      assert new_batch.backfill_id == "new-backfill"
      assert new_batch.seq == 43
      assert new_batch.primary_key_values == MapSet.new()
    end

    test "handles high watermark message by discarding batch when no matching batch exists" do
      # Create a context with no matching batch
      context = %MessageHandler.Context{
        table_reader_batches: [
          %MessageHandler.BatchState{
            batch_id: "other-batch",
            table_oid: 789,
            backfill_id: "other-backfill",
            seq: 42,
            primary_key_values: MapSet.new()
          }
        ],
        table_reader_mod: TableReaderServerMock
      }

      # Create a high watermark message for a non-existent batch
      message =
        ReplicationFactory.postgres_logical_message(%{
          prefix: Constants.backfill_batch_high_watermark(),
          content:
            Jason.encode!(%{
              "batch_id" => "non-existent-batch",
              "backfill_id" => "test-backfill"
            })
        })

      expect(TableReaderServerMock, :discard_batch, 1, fn "test-backfill", "non-existent-batch" ->
        :ok
      end)

      # Process the message
      capture_log(fn ->
        MessageHandler.handle_logical_message(context, 43, message)
      end) =~ "Batch not found"
    end

    test "handles high watermark message by flushing batch and removing it from context" do
      # Create a context with a matching batch that has some PKs
      existing_batch = %MessageHandler.BatchState{
        batch_id: "matching-batch",
        table_oid: 123,
        backfill_id: "test-backfill",
        seq: 42,
        primary_key_values: MapSet.new([["1"], ["2"], ["3"]])
      }

      batch_info = %{batch_id: "matching-batch", seq: 42, drop_pks: MapSet.new([["1"], ["2"], ["3"]])}

      context = %MessageHandler.Context{
        table_reader_batches: [
          existing_batch,
          %MessageHandler.BatchState{
            batch_id: "other-batch",
            table_oid: 789,
            backfill_id: "other-backfill",
            seq: 43,
            primary_key_values: MapSet.new()
          }
        ],
        table_reader_mod: TableReaderServerMock
      }

      # Create a high watermark message matching the existing batch
      message =
        ReplicationFactory.postgres_logical_message(%{
          prefix: Constants.backfill_batch_high_watermark(),
          content:
            Jason.encode!(%{
              "batch_id" => "matching-batch",
              "backfill_id" => "test-backfill"
            })
        })

      expect(TableReaderServerMock, :flush_batch, 1, fn "test-backfill", ^batch_info ->
        :ok
      end)

      # Process the message
      new_context = MessageHandler.handle_logical_message(context, 44, message)

      assert length(new_context.table_reader_batches) == 1
      assert Enum.all?(new_context.table_reader_batches, &(&1.batch_id != "matching-batch"))
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

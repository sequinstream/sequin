defmodule Sequin.MessageHandlerTest do
  use Sequin.DataCase, async: false

  alias Sequin.Constants
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Factory
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Functions.TestMessages
  alias Sequin.Health
  alias Sequin.Replication
  alias Sequin.Runtime.MessageHandler
  alias Sequin.Runtime.MessageLedgers
  alias Sequin.Runtime.SlotMessageStore
  alias Sequin.Runtime.SlotMessageStoreSupervisor
  alias Sequin.Runtime.TableReaderServerMock
  alias Sequin.TestSupport

  setup do
    TestSupport.stub_random(fn _ -> 1 end)
    TestMessages.create_ets_table()
    :ok
  end

  describe "handle_messages/2" do
    test "handles message_kind: event correctly" do
      account = AccountsFactory.insert_account!()
      database = DatabasesFactory.insert_postgres_database!(account_id: account.id)

      field = ReplicationFactory.field()
      message = ReplicationFactory.postgres_message(table_oid: 123, action: :insert, fields: [field])

      sequence =
        DatabasesFactory.insert_sequence!(
          table_oid: 123,
          account_id: account.id,
          postgres_database_id: database.id
        )

      sequence_filter =
        ConsumersFactory.sequence_filter_attrs(
          group_column_attnums: [field.column_attnum],
          column_filters: []
        )

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          message_kind: :event,
          account_id: account.id,
          sequence_id: sequence.id,
          sequence_filter: sequence_filter,
          postgres_database_id: database.id
        )

      start_supervised!(
        {SlotMessageStoreSupervisor, [consumer_id: consumer.id, test_pid: self(), persisted_mode?: false]}
      )

      consumer = Repo.preload(consumer, [:postgres_database, :sequence, :filter])
      database = Repo.preload(database, :sequences)

      context = context(consumers: [consumer], postgres_database: database)

      now = DateTime.utc_now()
      TestSupport.expect_utc_now(3, fn -> now end)

      {:ok, 1} = MessageHandler.handle_messages(context, [message])

      [event] = list_messages(consumer)
      assert event.consumer_id == consumer.id
      assert event.table_oid == 123
      assert event.commit_lsn == message.commit_lsn
      assert event.commit_idx == message.commit_idx
      assert event.commit_timestamp == message.commit_timestamp
      assert event.record_pks == Enum.map(message.ids, &to_string/1)
      assert event.data.action == :insert
      assert event.data.record == fields_to_map(message.fields)
      assert event.data.changes == nil
      assert event.data.metadata.table_name == message.table_name
      assert event.data.metadata.table_schema == message.table_schema
      assert event.data.metadata.commit_timestamp == message.commit_timestamp
      assert event.data.metadata.database_name == consumer.postgres_database.name
      assert is_binary(event.data.metadata.idempotency_key)
      assert event.data.metadata.idempotency_key != ""

      assert {:ok, 1} = MessageLedgers.count_commit_verification_set(consumer.id)
      assert {:ok, [wal_cursor]} = MessageLedgers.list_undelivered_wal_cursors(consumer.id, DateTime.utc_now())

      assert wal_cursor == %{
               commit_lsn: message.commit_lsn,
               commit_idx: message.commit_idx,
               ingested_at: DateTime.truncate(now, :second)
             }
    end

    test "handles message_kind: record correctly" do
      account = AccountsFactory.insert_account!()
      database = DatabasesFactory.insert_postgres_database!(account_id: account.id)

      field = ReplicationFactory.field()
      message = ReplicationFactory.postgres_message(table_oid: 456, action: :update, fields: [field])

      sequence =
        DatabasesFactory.insert_sequence!(
          table_oid: 456,
          account_id: account.id,
          postgres_database_id: database.id
        )

      sequence_filter =
        ConsumersFactory.sequence_filter_attrs(
          group_column_attnums: [field.column_attnum],
          column_filters: []
        )

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          message_kind: :record,
          account_id: account.id,
          sequence_id: sequence.id,
          sequence_filter: sequence_filter
        )

      start_supervised!(
        {SlotMessageStoreSupervisor, [consumer_id: consumer.id, test_pid: self(), persisted_mode?: false]}
      )

      consumer = Repo.preload(consumer, [:postgres_database, :sequence, :filter])
      database = Repo.preload(database, :sequences)

      context = context(consumers: [consumer], postgres_database: database)

      now = DateTime.utc_now()
      TestSupport.expect_utc_now(3, fn -> now end)

      {:ok, 1} = MessageHandler.handle_messages(context, [message])

      [record] = list_messages(consumer)
      assert record.consumer_id == consumer.id
      assert record.table_oid == 456
      assert record.commit_lsn == message.commit_lsn
      assert record.commit_idx == message.commit_idx
      assert record.commit_timestamp == message.commit_timestamp
      assert record.record_pks == Enum.map(message.ids, &to_string/1)
      assert record.group_id == Enum.find(message.fields, &(&1.column_attnum == field.column_attnum)).value
      assert record.state == :available

      assert {:ok, 1} = MessageLedgers.count_commit_verification_set(consumer.id)
      assert {:ok, [wal_cursor]} = MessageLedgers.list_undelivered_wal_cursors(consumer.id, DateTime.utc_now())

      assert wal_cursor == %{
               commit_lsn: message.commit_lsn,
               commit_idx: message.commit_idx,
               ingested_at: DateTime.truncate(now, :second)
             }
    end

    test "fans out messages correctly for mixed message_kind consumers and wal_pipelines" do
      account = AccountsFactory.insert_account!()
      database = DatabasesFactory.insert_postgres_database!(account_id: account.id)

      field = ReplicationFactory.field()
      table_schema1 = Factory.postgres_object()
      table_schema2 = Factory.postgres_object()

      message1 =
        ReplicationFactory.postgres_message(table_oid: 123, action: :insert, fields: [field], table_schema: table_schema1)

      message2 =
        ReplicationFactory.postgres_message(table_oid: 456, action: :update, fields: [field], table_schema: table_schema2)

      message3 =
        ReplicationFactory.postgres_message(table_oid: 789, action: :insert, fields: [field], table_schema: table_schema1)

      sequence1 =
        DatabasesFactory.insert_sequence!(
          table_oid: 123,
          account_id: account.id,
          postgres_database_id: database.id
        )

      sequence2 =
        DatabasesFactory.insert_sequence!(
          table_oid: 456,
          account_id: account.id,
          postgres_database_id: database.id
        )

      sequence_filter1 =
        ConsumersFactory.sequence_filter_attrs(
          group_column_attnums: [field.column_attnum],
          column_filters: []
        )

      sequence_filter2 =
        ConsumersFactory.sequence_filter_attrs(
          group_column_attnums: [field.column_attnum],
          column_filters: []
        )

      consumer1 =
        ConsumersFactory.insert_sink_consumer!(
          message_kind: :event,
          account_id: account.id,
          sequence_id: sequence1.id,
          sequence_filter: sequence_filter1
        )

      consumer2 =
        ConsumersFactory.insert_sink_consumer!(
          message_kind: :record,
          account_id: account.id,
          sequence_id: sequence2.id,
          sequence_filter: sequence_filter2
        )

      schema_filter = ConsumersFactory.schema_filter_attrs(schema: table_schema1)

      consumer3 =
        ConsumersFactory.insert_sink_consumer!(
          message_kind: :event,
          account_id: account.id,
          sequence_id: nil,
          sequence_filter: nil,
          schema_filter: schema_filter
        )

      start_supervised!(
        {SlotMessageStoreSupervisor, [consumer_id: consumer1.id, test_pid: self(), persisted_mode?: false]}
      )

      start_supervised!(
        {SlotMessageStoreSupervisor, [consumer_id: consumer2.id, test_pid: self(), persisted_mode?: false]}
      )

      start_supervised!(
        {SlotMessageStoreSupervisor, [consumer_id: consumer3.id, test_pid: self(), persisted_mode?: false]}
      )

      consumer1 = Repo.preload(consumer1, [:postgres_database, :sequence, :filter])
      consumer2 = Repo.preload(consumer2, [:postgres_database, :sequence, :filter])
      consumer3 = Repo.preload(consumer3, [:postgres_database, :sequence, :filter])

      database = Repo.preload(database, :sequences)

      source_table1 = ConsumersFactory.source_table(oid: 123, column_filters: [])
      source_table2 = ConsumersFactory.source_table(oid: 456, column_filters: [])

      wal_pipeline =
        ReplicationFactory.insert_wal_pipeline!(
          account_id: account.id,
          source_tables: [source_table1, source_table2]
        )

      context =
        context(consumers: [consumer1, consumer2, consumer3], wal_pipelines: [wal_pipeline], postgres_database: database)

      {:ok, 6} = MessageHandler.handle_messages(context, [message1, message2, message3])

      consumer1_messages = list_messages(consumer1)
      consumer2_messages = list_messages(consumer2)
      consumer3_messages = list_messages(consumer3)
      wal_events = Replication.list_wal_events(wal_pipeline.id)

      assert length(consumer1_messages) == 1
      assert hd(consumer1_messages).table_oid == 123

      assert length(consumer2_messages) == 1
      assert hd(consumer2_messages).table_oid == 456

      assert length(consumer3_messages) == 2
      assert Enum.all?(consumer3_messages, &(&1.data.metadata.table_schema == table_schema1))

      assert length(wal_events) == 2
    end

    test "two messages with two consumers and one wal_pipeline are fanned out to each" do
      account = AccountsFactory.insert_account!()
      database = DatabasesFactory.insert_postgres_database!(account_id: account.id)

      field = ReplicationFactory.field()
      message1 = ReplicationFactory.postgres_message(table_oid: 123, action: :insert, fields: [field])
      message2 = ReplicationFactory.postgres_message(table_oid: 123, action: :insert, fields: [field])

      sequence =
        DatabasesFactory.insert_sequence!(table_oid: 123, account_id: account.id, postgres_database_id: database.id)

      sequence_filter =
        ConsumersFactory.sequence_filter_attrs(group_column_attnums: [field.column_attnum], column_filters: [])

      consumer1 =
        ConsumersFactory.insert_sink_consumer!(
          account_id: account.id,
          sequence_id: sequence.id,
          sequence_filter: sequence_filter
        )

      consumer2 =
        ConsumersFactory.insert_sink_consumer!(
          account_id: account.id,
          sequence_id: sequence.id,
          sequence_filter: sequence_filter
        )

      start_supervised!(
        {SlotMessageStoreSupervisor, [consumer_id: consumer1.id, test_pid: self(), persisted_mode?: false]}
      )

      start_supervised!(
        {SlotMessageStoreSupervisor, [consumer_id: consumer2.id, test_pid: self(), persisted_mode?: false]}
      )

      consumer1 = Repo.preload(consumer1, [:postgres_database, :sequence, :filter])
      consumer2 = Repo.preload(consumer2, [:postgres_database, :sequence, :filter])

      database = Repo.preload(database, :sequences)

      source_table = ConsumersFactory.source_table(oid: 123, column_filters: [])
      wal_pipeline = ReplicationFactory.insert_wal_pipeline!(account_id: account.id, source_tables: [source_table])

      context = context(consumers: [consumer1, consumer2], wal_pipelines: [wal_pipeline], postgres_database: database)

      {:ok, 6} = MessageHandler.handle_messages(context, [message1, message2])

      consumer1_messages = list_messages(consumer1)
      consumer2_messages = list_messages(consumer2)
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

    test "inserts message for consumer with matching sequence and no filters" do
      account = AccountsFactory.insert_account!()
      database = DatabasesFactory.insert_postgres_database!(account_id: account.id)

      field = ReplicationFactory.field()
      message = ReplicationFactory.postgres_message(table_oid: 123, action: :insert, fields: [field])

      sequence =
        DatabasesFactory.insert_sequence!(
          table_oid: 123,
          account_id: account.id,
          postgres_database_id: database.id
        )

      sequence_filter =
        ConsumersFactory.sequence_filter_attrs(
          group_column_attnums: [field.column_attnum],
          column_filters: []
        )

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          account_id: account.id,
          sequence_id: sequence.id,
          sequence_filter: sequence_filter
        )

      start_supervised!(
        {SlotMessageStoreSupervisor, [consumer_id: consumer.id, test_pid: self(), persisted_mode?: false]}
      )

      consumer = Repo.preload(consumer, [:postgres_database, :sequence, :filter])
      database = Repo.preload(database, :sequences)

      context = context(consumers: [consumer], postgres_database: database)

      {:ok, 1} = MessageHandler.handle_messages(context, [message])

      messages = list_messages(consumer)
      assert length(messages) == 1
      assert hd(messages).table_oid == 123
      assert hd(messages).consumer_id == consumer.id
    end

    test "inserts message for consumer with matching sequence and passing filters" do
      account = AccountsFactory.insert_account!()
      database = DatabasesFactory.insert_postgres_database!(account_id: account.id)

      field = ReplicationFactory.field(column_attnum: 1, value: "test")
      message = ReplicationFactory.postgres_message(table_oid: 123, action: :insert, fields: [field])

      column_filter =
        ConsumersFactory.column_filter(
          column_attnum: 1,
          operator: :==,
          value: %{__type__: :string, value: "test"}
        )

      sequence =
        DatabasesFactory.insert_sequence!(
          table_oid: 123,
          account_id: account.id,
          postgres_database_id: database.id
        )

      sequence_filter =
        ConsumersFactory.sequence_filter_attrs(
          group_column_attnums: [field.column_attnum],
          column_filters: [column_filter]
        )

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          account_id: account.id,
          sequence_id: sequence.id,
          sequence_filter: sequence_filter
        )

      start_supervised!(
        {SlotMessageStoreSupervisor, [consumer_id: consumer.id, test_pid: self(), persisted_mode?: false]}
      )

      consumer = Repo.preload(consumer, [:postgres_database, :sequence, :filter])
      database = Repo.preload(database, :sequences)

      test_field = ReplicationFactory.field(column_attnum: 1, value: "test")
      message = %{message | fields: [test_field | message.fields]}

      context = context(consumers: [consumer], postgres_database: database)

      {:ok, 1} = MessageHandler.handle_messages(context, [message])

      messages = list_messages(consumer)
      assert length(messages) == 1
      assert hd(messages).table_oid == 123
      assert hd(messages).consumer_id == consumer.id
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

      context = context(wal_pipelines: [wal_pipeline], postgres_database: %PostgresDatabase{sequences: []})

      {:ok, 2} = MessageHandler.handle_messages(context, [insert_message, update_message])

      {[insert_event], [update_event]} =
        wal_pipeline.id |> Replication.list_wal_events() |> Enum.split_with(&(&1.action == :insert))

      assert insert_event.action == :insert
      assert insert_event.wal_pipeline_id == wal_pipeline.id
      assert insert_message.commit_lsn == insert_event.commit_lsn
      assert insert_message.commit_idx == insert_event.commit_idx
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

      context = context(wal_pipelines: [wal_pipeline], postgres_database: %PostgresDatabase{sequences: []})

      {:ok, 1} = MessageHandler.handle_messages(context, [message])

      wal_events = Replication.list_wal_events(wal_pipeline.id)
      assert length(wal_events) == 1
      assert hd(wal_events).wal_pipeline_id == wal_pipeline.id
    end

    test "does not insert wal_event for wal_pipeline with non-matching source table" do
      message = ReplicationFactory.postgres_message(table_oid: 123)
      source_table = ConsumersFactory.source_table(oid: 456)
      wal_pipeline = ReplicationFactory.insert_wal_pipeline!(source_tables: [source_table])

      context = context(wal_pipelines: [wal_pipeline], postgres_database: %PostgresDatabase{sequences: []})

      {:ok, 0} = MessageHandler.handle_messages(context, [message])

      wal_events = Replication.list_wal_events(wal_pipeline.id)
      assert Enum.empty?(wal_events)
    end

    test "inserts wal_event for pipeline with matching source table and passing filters" do
      message = ReplicationFactory.postgres_message(action: :insert, table_oid: 123)

      column_filter =
        ConsumersFactory.column_filter(
          column_attnum: 1,
          operator: :==,
          value: %{__type__: :string, value: "test"}
        )

      source_table = ConsumersFactory.source_table(oid: 123, column_filters: [column_filter])
      wal_pipeline = ReplicationFactory.insert_wal_pipeline!(source_tables: [source_table])

      test_field = ReplicationFactory.field(column_attnum: 1, value: "test")
      message = %{message | fields: [test_field | message.fields]}

      context = context(wal_pipelines: [wal_pipeline], postgres_database: %PostgresDatabase{sequences: []})

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
      field = ReplicationFactory.field(column_attnum: 1, value: "not_test")
      message = %{message | fields: [field | message.fields]}

      context = context(wal_pipelines: [wal_pipeline], postgres_database: %PostgresDatabase{sequences: []})

      {:ok, 0} = MessageHandler.handle_messages(context, [message])

      wal_events = Replication.list_wal_events(wal_pipeline.id)
      assert Enum.empty?(wal_events)
    end

    test "sets group_id based on group_column_attnums when it's set" do
      account = AccountsFactory.insert_account!()
      database = DatabasesFactory.insert_postgres_database!(account_id: account.id)

      fields = [
        ReplicationFactory.field(column_attnum: 1, column_name: "id", value: 1),
        ReplicationFactory.field(column_attnum: 2, column_name: "group", value: "A"),
        ReplicationFactory.field(column_attnum: 3, column_name: "name", value: "Test")
      ]

      message =
        ReplicationFactory.postgres_message(
          table_oid: 123,
          action: :insert,
          ids: [1, 2],
          fields: fields
        )

      sequence =
        DatabasesFactory.insert_sequence!(
          table_oid: 123,
          account_id: account.id,
          postgres_database_id: database.id
        )

      sequence_filter =
        ConsumersFactory.sequence_filter_attrs(
          group_column_attnums: [2],
          column_filters: []
        )

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          message_kind: :record,
          account_id: account.id,
          sequence_id: sequence.id,
          sequence_filter: sequence_filter
        )

      start_supervised!(
        {SlotMessageStoreSupervisor, [consumer_id: consumer.id, test_pid: self(), persisted_mode?: false]}
      )

      consumer = Repo.preload(consumer, [:postgres_database, :sequence, :filter])
      database = Repo.preload(database, :sequences)

      context = context(consumers: [consumer], postgres_database: database)

      {:ok, 1} = MessageHandler.handle_messages(context, [message])

      [record] = list_messages(consumer)
      assert record.group_id == "A"
    end
  end

  describe "handle_logical_message/3" do
    test "handles high watermark message by flushing batch and removing it from context" do
      batch_info = %{batch_id: "matching-batch", commit_lsn: 42}
      slot_id = Factory.uuid()

      context = context(replication_slot_id: slot_id, table_reader_mod: TableReaderServerMock)

      # Create a high watermark message matching the existing batch
      message =
        ReplicationFactory.postgres_logical_message(%{
          prefix: Constants.backfill_batch_high_watermark(),
          content:
            Jason.encode!(%{
              "batch_id" => "matching-batch",
              "backfill_id" => "test-backfill",
              "replication_slot_id" => slot_id
            })
        })

      expect(TableReaderServerMock, :flush_batch, 1, fn "test-backfill", ^batch_info ->
        :ok
      end)

      # Process the message
      :ok = MessageHandler.handle_logical_message(context, 42, message)
    end
  end

  describe "before_handle_messages/2" do
    test "only processes messages for tables with running TableReaderServers" do
      account = AccountsFactory.insert_account!()
      database = DatabasesFactory.insert_postgres_database!(account_id: account.id)

      # Create two sequences with different table_oids
      sequence1 =
        DatabasesFactory.insert_sequence!(
          table_oid: 123,
          account_id: account.id,
          postgres_database_id: database.id
        )

      sequence2 =
        DatabasesFactory.insert_sequence!(
          table_oid: 456,
          account_id: account.id,
          postgres_database_id: database.id
        )

      # Create two consumers
      consumer1 =
        ConsumersFactory.insert_sink_consumer!(
          account_id: account.id,
          sequence_id: sequence1.id,
          sequence_filter: ConsumersFactory.sequence_filter_attrs()
        )

      consumer2 =
        ConsumersFactory.insert_sink_consumer!(
          account_id: account.id,
          sequence_id: sequence2.id,
          sequence_filter: ConsumersFactory.sequence_filter_attrs()
        )

      # Preload consumers with their sequences
      consumers = [
        Repo.preload(consumer1, :sequence),
        Repo.preload(consumer2, :sequence)
      ]

      # Create messages for both table_oids
      messages = [
        # composite pk for one row
        ReplicationFactory.postgres_message(table_oid: 123, ids: [1, 2]),
        # composite pk for one row
        ReplicationFactory.postgres_message(table_oid: 456, ids: [3, 4])
      ]

      context =
        context(
          consumers: consumers,
          postgres_database: %PostgresDatabase{sequences: []},
          table_reader_mod: TableReaderServerMock
        )

      # Mock that only consumer1 has a running TableReaderServer
      expect(TableReaderServerMock, :active_table_oids, 1, fn ->
        [123]
      end)

      # Expect pks_seen to be called only for consumer1's messages
      expect(TableReaderServerMock, :pks_seen, 1, fn table_oid, pks ->
        assert table_oid == 123
        # Single composite pk
        assert pks == [[1, 2]]
        :ok
      end)

      MessageHandler.before_handle_messages(context, messages)
    end

    test "handles multiple messages for the same table_oid" do
      account = AccountsFactory.insert_account!()
      database = DatabasesFactory.insert_postgres_database!(account_id: account.id)

      sequence =
        DatabasesFactory.insert_sequence!(
          table_oid: 123,
          account_id: account.id,
          postgres_database_id: database.id
        )

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          account_id: account.id,
          sequence_id: sequence.id,
          sequence_filter: ConsumersFactory.sequence_filter_attrs()
        )

      consumer = Repo.preload(consumer, [:sequence, :filter])

      # Create multiple messages for the same table_oid
      messages = [
        # First row's composite pk
        ReplicationFactory.postgres_message(table_oid: 123, ids: [1, 2]),
        # Second row's composite pk
        ReplicationFactory.postgres_message(table_oid: 123, ids: [3, 4, 5])
      ]

      context =
        context(
          consumers: [consumer],
          postgres_database: %PostgresDatabase{sequences: []},
          table_reader_mod: TableReaderServerMock
        )

      # Mock that the consumer has a running TableReaderServer
      expect(TableReaderServerMock, :active_table_oids, 1, fn ->
        [123]
      end)

      # Expect pks_seen to be called with composite pks from both messages
      expect(TableReaderServerMock, :pks_seen, 1, fn table_oid, pks ->
        assert table_oid == 123
        # Two different composite pks
        assert pks == [[1, 2], [3, 4, 5]]
        :ok
      end)

      MessageHandler.before_handle_messages(context, messages)
    end
  end

  describe "handle_messages/2 with unchanged_toast" do
    setup do
      account = AccountsFactory.insert_account!()
      database = DatabasesFactory.insert_postgres_database!(account_id: account.id)

      sequence =
        DatabasesFactory.insert_sequence!(
          table_oid: 123,
          account_id: account.id,
          postgres_database_id: database.id
        )

      sequence_filter =
        ConsumersFactory.sequence_filter_attrs(
          group_column_attnums: [1],
          column_filters: []
        )

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          account_id: account.id,
          sequence_id: sequence.id,
          sequence_filter: sequence_filter,
          postgres_database_id: database.id
        )

      consumer = Repo.preload(consumer, [:postgres_database, :sequence, :filter])

      start_supervised!(
        {SlotMessageStoreSupervisor, [consumer_id: consumer.id, test_pid: self(), persisted_mode?: false]}
      )

      database = Repo.preload(database, :sequences)

      context = context(consumers: [consumer], postgres_database: database)

      %{context: context, consumer: consumer}
    end

    test "with replica identity full, loads unchanged toast values from old fields", %{
      context: context,
      consumer: consumer
    } do
      fields = [
        ReplicationFactory.field(column_name: "id", column_attnum: 1, value: 1),
        ReplicationFactory.field(column_name: "name", value: :unchanged_toast, column_attnum: 2),
        ReplicationFactory.field(column_name: "house", value: "Gryffindor", column_attnum: 3)
      ]

      old_fields = [
        ReplicationFactory.field(column_name: "id", column_attnum: 1, value: 1),
        ReplicationFactory.field(column_name: "name", value: "Harry", column_attnum: 2),
        ReplicationFactory.field(column_name: "house", value: nil, column_attnum: 3)
      ]

      message =
        ReplicationFactory.postgres_message(action: :update, table_oid: 123, fields: fields, old_fields: old_fields)

      {:ok, 1} = MessageHandler.handle_messages(context, [message])
      [message] = list_messages(consumer)

      assert message.data.record["name"] == "Harry"
      assert message.data.record["house"] == "Gryffindor"
    end

    test "without replica identity full, emits health event", %{
      context: context,
      consumer: consumer
    } do
      fields = [
        ReplicationFactory.field(column_name: "id", column_attnum: 1, value: 1),
        ReplicationFactory.field(column_name: "name", value: :unchanged_toast, column_attnum: 2),
        ReplicationFactory.field(column_name: "house", value: "Gryffindor", column_attnum: 3)
      ]

      message = ReplicationFactory.postgres_message(action: :update, table_oid: 123, fields: fields, old_fields: nil)

      {:ok, 1} = MessageHandler.handle_messages(context, [message])
      [message] = list_messages(consumer)

      # unchanged_toast values are not loaded
      assert message.data.record["name"] == :unchanged_toast
      assert message.data.record["house"] == "Gryffindor"

      # consumer health was updated
      assert {:ok, event} = Health.get_event(consumer.id, :toast_columns_detected)
      assert event.status == :warning
    end

    test "inserts are ignored", %{context: context, consumer: consumer} do
      field = ReplicationFactory.field(column_name: "id", column_attnum: 1, value: 1)
      message = ReplicationFactory.postgres_message(action: :insert, table_oid: 123, fields: [field])

      {:ok, 1} = MessageHandler.handle_messages(context, [message])
      assert [_] = list_messages(consumer)
    end

    test "deletes are ignored", %{context: context, consumer: consumer} do
      field = ReplicationFactory.field(column_name: "id", column_attnum: 1, value: 1)
      message = ReplicationFactory.postgres_message(action: :delete, table_oid: 123, old_fields: [field])

      {:ok, count} = MessageHandler.handle_messages(context, [message])

      case consumer.message_kind do
        :record ->
          assert count == 0
          records = list_messages(consumer)
          assert length(records) == 0

        :event ->
          assert count == 1
          events = list_messages(consumer)
          assert length(events) == 1
      end
    end
  end

  defp list_messages(consumer) do
    SlotMessageStore.peek_messages(consumer, 1000)
  end

  defp fields_to_map(fields) do
    Map.new(fields, fn %{column_name: name, value: value} -> {name, value} end)
  end

  defp context(attrs) do
    attrs = Keyword.put_new(attrs, :replication_slot_id, UUID.uuid4())

    MessageHandler.Context
    |> struct!(attrs)
    |> MessageHandler.Context.set_indices()
  end
end

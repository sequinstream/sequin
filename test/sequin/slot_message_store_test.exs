defmodule Sequin.SlotMessageStoreTest do
  use Sequin.DataCase, async: true

  alias Sequin.Consumers
  alias Sequin.DatabasesRuntime.SlotMessageStore
  alias Sequin.DatabasesRuntime.SlotMessageStore.State
  alias Sequin.DatabasesRuntime.TableReaderMock
  alias Sequin.Error.InvariantError
  alias Sequin.Factory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory

  describe "SlotMessageStore with persisted messages" do
    setup do
      consumer = ConsumersFactory.insert_sink_consumer!()

      msg1 = ConsumersFactory.insert_consumer_message!(message_kind: consumer.message_kind, consumer_id: consumer.id)
      msg2 = ConsumersFactory.insert_consumer_message!(message_kind: consumer.message_kind, consumer_id: consumer.id)

      start_supervised!({SlotMessageStore, consumer_id: consumer.id, test_pid: self()})

      %{consumer: consumer, msg1: msg1, msg2: msg2}
    end

    test "messages from disk are loaded on init and then produce_messages returns them", %{
      consumer: consumer,
      msg1: msg1,
      msg2: msg2
    } do
      assert {:ok, delivered} = SlotMessageStore.produce(consumer.id, 2, self())
      assert length(delivered) == 2
      assert Enum.all?(delivered, fn msg -> msg.id in [msg1.id, msg2.id] end)

      assert SlotMessageStore.peek(consumer.id).payload_size_bytes > 0
    end

    test "producing and acking persisted messages deletes them from disk", %{consumer: consumer} do
      refute [] == Consumers.list_consumer_messages_for_consumer(consumer)

      assert {:ok, delivered} = SlotMessageStore.produce(consumer.id, 10, self())
      assert length(delivered) == 2

      ack_ids = Enum.map(delivered, & &1.ack_id)
      {:ok, 2} = SlotMessageStore.messages_succeeded(consumer.id, ack_ids)

      assert [] == Consumers.list_consumer_messages_for_consumer(consumer)
    end

    test "putting messages with persisted group_ids will upsert to postgres", %{
      consumer: consumer,
      msg1: msg1
    } do
      new_message =
        ConsumersFactory.consumer_message(
          group_id: msg1.group_id,
          message_kind: consumer.message_kind,
          consumer_id: consumer.id
        )

      :ok = SlotMessageStore.put_messages(consumer.id, [new_message])

      persisted_messages = Consumers.list_consumer_messages_for_consumer(consumer)
      assert length(persisted_messages) == 3

      assert new_message.record_pks in Enum.map(persisted_messages, & &1.record_pks)
    end
  end

  describe "SlotMessageStore message handling" do
    setup do
      consumer = ConsumersFactory.insert_sink_consumer!()
      event_consumer = ConsumersFactory.insert_sink_consumer!(message_kind: :event)
      start_supervised!({SlotMessageStore, consumer_id: consumer.id, test_pid: self()})
      start_supervised!({SlotMessageStore, consumer_id: event_consumer.id, test_pid: self()})
      %{consumer: consumer, event_consumer: event_consumer}
    end

    test "puts, delivers, and acks in-memory messages", %{consumer: consumer} do
      # Create test events
      messages = [
        ConsumersFactory.consumer_message(),
        ConsumersFactory.consumer_message()
      ]

      # Put messages in store
      :ok = SlotMessageStore.put_messages(consumer.id, messages)

      # Retrieve messages
      {:ok, delivered} = SlotMessageStore.produce(consumer.id, 2, self())
      assert length(delivered) == 2

      # For acks
      ack_ids = Enum.map(delivered, & &1.ack_id)
      {:ok, 2} = SlotMessageStore.messages_succeeded(consumer.id, ack_ids)

      # Produce messages, none should be delivered
      {:ok, []} = SlotMessageStore.produce(consumer.id, 2, self())
    end

    test "deletes failed then succeeded messages from postgres", %{consumer: consumer} do
      # Create message with a group_id
      group_id = "test-group"

      message =
        ConsumersFactory.consumer_message(
          group_id: group_id,
          message_kind: consumer.message_kind,
          consumer_id: consumer.id
        )

      # Put message in store (starts unpersisted)
      :ok = SlotMessageStore.put_messages(consumer.id, [message])

      # Deliver message
      {:ok, [delivered]} = SlotMessageStore.produce(consumer.id, 1, self())

      # Fail the message (this will persist it)
      meta = %{
        ack_id: delivered.ack_id,
        deliver_count: 1,
        last_delivered_at: DateTime.utc_now(),
        not_visible_until: DateTime.utc_now()
      }

      :ok = SlotMessageStore.messages_failed(consumer.id, [meta])

      # Verify message is persisted
      assert [_persisted] = Consumers.list_consumer_messages_for_consumer(consumer)

      # Redeliver and succeed the message
      {:ok, [redelivered]} = SlotMessageStore.produce(consumer.id, 1, self())
      {:ok, 1} = SlotMessageStore.messages_succeeded(consumer.id, [redelivered.ack_id])

      # Verify message was deleted from postgres
      assert [] = Consumers.list_consumer_messages_for_consumer(consumer)
    end

    test "deletes failed then already_succeeded messages from postgres", %{consumer: consumer} do
      # Create message with a group_id
      group_id = "test-group"

      message =
        ConsumersFactory.consumer_message(
          group_id: group_id,
          message_kind: consumer.message_kind,
          consumer_id: consumer.id
        )

      # Put message in store (starts unpersisted)
      :ok = SlotMessageStore.put_messages(consumer.id, [message])

      # Deliver message
      {:ok, [delivered]} = SlotMessageStore.produce(consumer.id, 1, self())

      # Fail the message (this will persist it)
      meta = %{
        ack_id: delivered.ack_id,
        deliver_count: 1,
        last_delivered_at: DateTime.utc_now(),
        not_visible_until: DateTime.utc_now()
      }

      :ok = SlotMessageStore.messages_failed(consumer.id, [meta])

      # Verify message is persisted
      assert [_persisted] = Consumers.list_consumer_messages_for_consumer(consumer)

      # Redeliver and succeed the message
      {:ok, [redelivered]} = SlotMessageStore.produce(consumer.id, 1, self())
      {:ok, 1} = SlotMessageStore.messages_already_succeeded(consumer.id, [redelivered.ack_id])

      # Verify message was deleted from postgres
      assert [] = Consumers.list_consumer_messages_for_consumer(consumer)
    end

    test "delivers and fails messages to disk", %{consumer: consumer} do
      messages = [
        ConsumersFactory.consumer_message(message_kind: consumer.message_kind, consumer_id: consumer.id),
        ConsumersFactory.consumer_message(message_kind: consumer.message_kind, consumer_id: consumer.id)
      ]

      # Put messages in store
      :ok = SlotMessageStore.put_messages(consumer.id, messages)

      # Produce messages
      {:ok, delivered} = SlotMessageStore.produce(consumer.id, 2, self())
      assert length(delivered) == 2

      # Fail the messages with updated metadata
      now = DateTime.utc_now()
      not_visible_until = DateTime.add(now, 60)

      metas =
        Enum.map(delivered, fn msg ->
          %{
            ack_id: msg.ack_id,
            deliver_count: 1,
            last_delivered_at: now,
            not_visible_until: not_visible_until
          }
        end)

      :ok = SlotMessageStore.messages_failed(consumer.id, metas)

      # Verify messages were persisted with updated metadata
      messages = SlotMessageStore.peek_messages(consumer.id, 2)
      assert length(messages) == 2
      assert Enum.all?(messages, fn msg -> msg.deliver_count == 1 end)
      assert Enum.all?(messages, fn msg -> msg.last_delivered_at == now end)
      assert Enum.all?(messages, fn msg -> msg.not_visible_until == not_visible_until end)
    end

    test "when a message fails, non-persisted messages of the same group_id are persisted", %{consumer: consumer} do
      # Create two messages with same group_id
      group_id = "test-group"

      message1 =
        ConsumersFactory.consumer_message(
          group_id: group_id,
          message_kind: consumer.message_kind,
          consumer_id: consumer.id
        )

      message2 =
        ConsumersFactory.consumer_message(
          group_id: group_id,
          message_kind: consumer.message_kind,
          consumer_id: consumer.id
        )

      # Put both messages in store
      :ok = SlotMessageStore.put_messages(consumer.id, [message1, message2])

      # Get and fail first message
      {:ok, [delivered]} = SlotMessageStore.produce(consumer.id, 1, self())

      meta = %{
        ack_id: delivered.ack_id,
        deliver_count: 1,
        last_delivered_at: DateTime.utc_now(),
        not_visible_until: DateTime.add(DateTime.utc_now(), 60)
      }

      :ok = SlotMessageStore.messages_failed(consumer.id, [meta])

      # Verify both messages are now persisted
      messages = SlotMessageStore.peek_messages(consumer.id, 2)
      assert length(messages) == 2
      assert Enum.all?(messages, &(&1.group_id == group_id))

      persisted_messages = Consumers.list_consumer_messages_for_consumer(consumer)
      assert length(persisted_messages) == 2
      assert Enum.all?(persisted_messages, &(&1.group_id == group_id))
    end

    test "if the pid changes between calls of produce_messages, produced_messages are available for deliver", %{
      consumer: consumer
    } do
      messages = [
        ConsumersFactory.consumer_message(),
        ConsumersFactory.consumer_message()
      ]

      # Put messages in store
      :ok = SlotMessageStore.put_messages(consumer.id, messages)

      # Produce messages, none should be delivered
      {:ok, delivered} = SlotMessageStore.produce(consumer.id, 2, self())
      assert length(delivered) == 2
      {:ok, []} = SlotMessageStore.produce(consumer.id, 2, self())

      {:ok, delivered} = SlotMessageStore.produce(consumer.id, 2, Factory.pid())
      assert length(delivered) == 2
    end

    test "messages with nil group_ids are not blocked when other nil group_id messages are persisted", %{
      # Only event_consumer has nil group_ids
      event_consumer: consumer
    } do
      # Create a message with nil group_id that will be persisted
      message1 =
        ConsumersFactory.consumer_message(
          group_id: nil,
          message_kind: consumer.message_kind,
          consumer_id: consumer.id,
          commit_lsn: 1
        )

      # Create another message with nil group_id that should not be blocked
      message2 =
        ConsumersFactory.consumer_message(
          group_id: nil,
          message_kind: consumer.message_kind,
          consumer_id: consumer.id,
          commit_lsn: 2
        )

      # Put first message and fail it to persist it
      :ok = SlotMessageStore.put_messages(consumer.id, [message1])
      {:ok, [delivered]} = SlotMessageStore.produce(consumer.id, 1, self())

      meta = %{
        ack_id: delivered.ack_id,
        deliver_count: 1,
        last_delivered_at: DateTime.utc_now(),
        not_visible_until: DateTime.add(DateTime.utc_now(), 60)
      }

      :ok = SlotMessageStore.messages_failed(consumer.id, [meta])

      # Put second message - it should not be blocked or persisted
      :ok = SlotMessageStore.put_messages(consumer.id, [message2])
      [persisted_msg] = Consumers.list_consumer_messages_for_consumer(consumer)
      assert persisted_msg.commit_lsn == message1.commit_lsn
      {:ok, [message]} = SlotMessageStore.produce(consumer.id, 1, self())
      assert message.commit_lsn == message2.commit_lsn
    end

    test "is_message_group_persisted? returns false for nil group_ids", %{event_consumer: consumer} do
      # Create and persist a message with nil group_id
      message =
        ConsumersFactory.consumer_message(
          group_id: nil,
          message_kind: consumer.message_kind,
          consumer_id: consumer.id
        )

      :ok = SlotMessageStore.put_messages(consumer.id, [message])
      {:ok, [delivered]} = SlotMessageStore.produce(consumer.id, 1, self())

      meta = %{
        ack_id: delivered.ack_id,
        deliver_count: 1,
        last_delivered_at: DateTime.utc_now(),
        not_visible_until: DateTime.add(DateTime.utc_now(), 60)
      }

      :ok = SlotMessageStore.messages_failed(consumer.id, [meta])

      # Peek at state to verify is_message_group_persisted? returns false
      state = SlotMessageStore.peek(consumer.id)
      refute State.is_message_group_persisted?(state, nil)
    end

    test "pop_blocked_messages does not block messages with nil group_ids", %{event_consumer: consumer} do
      # Create messages - one with group_id and one without
      message1 =
        ConsumersFactory.consumer_message(
          group_id: "test-group",
          message_kind: consumer.message_kind,
          consumer_id: consumer.id
        )

      message2 =
        ConsumersFactory.consumer_message(
          group_id: nil,
          message_kind: consumer.message_kind,
          consumer_id: consumer.id
        )

      # Put and fail first message to persist it
      :ok = SlotMessageStore.put_messages(consumer.id, [message1])
      {:ok, [delivered]} = SlotMessageStore.produce(consumer.id, 1, self())

      meta = %{
        ack_id: delivered.ack_id,
        deliver_count: 1,
        last_delivered_at: DateTime.utc_now(),
        not_visible_until: DateTime.add(DateTime.utc_now(), 60)
      }

      :ok = SlotMessageStore.messages_failed(consumer.id, [meta])

      # Put second message (nil group_id)
      :ok = SlotMessageStore.put_messages(consumer.id, [message2])

      # Verify only messages with non-nil group_ids are blocked
      state = SlotMessageStore.peek(consumer.id)
      {blocked_messages, _state} = State.pop_blocked_messages(state)

      assert length(blocked_messages) == 0
    end

    test "persists all messages when consumer is disabled", %{consumer: consumer} do
      # Put initial message in store while active
      initial_message =
        ConsumersFactory.consumer_message(
          message_kind: consumer.message_kind,
          consumer_id: consumer.id
        )

      :ok = SlotMessageStore.put_messages(consumer.id, [initial_message])

      # Verify message is not persisted yet
      assert [] == Consumers.list_consumer_messages_for_consumer(consumer)

      # Update consumer to disabled state
      disabled_consumer = %{consumer | status: :disabled}
      :ok = SlotMessageStore.consumer_updated(disabled_consumer)

      # This will cause an async flush, wait for it by making any other call
      SlotMessageStore.peek_messages(consumer.id, 1)

      # Verify existing message was persisted
      persisted_messages = Consumers.list_consumer_messages_for_consumer(disabled_consumer)
      assert length(persisted_messages) == 1

      # Put new messages in store while disabled
      new_messages = [
        ConsumersFactory.consumer_message(message_kind: consumer.message_kind, consumer_id: consumer.id),
        ConsumersFactory.consumer_message(message_kind: consumer.message_kind, consumer_id: consumer.id)
      ]

      :ok = SlotMessageStore.put_messages(consumer.id, new_messages)

      # Verify all messages are persisted
      persisted_messages = Consumers.list_consumer_messages_for_consumer(disabled_consumer)
      assert length(persisted_messages) == 3
    end
  end

  describe "SlotMessageStore table reader batch handling" do
    setup do
      consumer = ConsumersFactory.insert_sink_consumer!()

      start_supervised!({SlotMessageStore, consumer_id: consumer.id, test_pid: self()})

      %{consumer: consumer}
    end

    test "puts batch and reports on batch progress", %{consumer: consumer} do
      consumer_id = consumer.id
      :syn.join(:consumers, {:table_reader_batch_complete, consumer_id}, self())
      # Create test events
      messages = [
        ConsumersFactory.consumer_message(),
        ConsumersFactory.consumer_message()
      ]

      # Put messages in store
      :ok = SlotMessageStore.put_table_reader_batch(consumer_id, messages, "test-batch-id")

      # Retrieve messages
      {:ok, [delivered]} = SlotMessageStore.produce(consumer_id, 1, self())

      assert {:ok, :in_progress} == SlotMessageStore.batch_progress(consumer_id, "test-batch-id")

      # For acks
      {:ok, 1} = SlotMessageStore.messages_succeeded(consumer_id, [delivered.ack_id])

      # Produce messages, none should be delivered
      {:ok, [delivered]} = SlotMessageStore.produce(consumer_id, 1, self())

      # Delivered messages don't "complete" a batch
      assert {:ok, :in_progress} == SlotMessageStore.batch_progress(consumer_id, "test-batch-id")

      {:ok, 1} = SlotMessageStore.messages_succeeded(consumer_id, [delivered.ack_id])

      assert_received {:table_reader_batch_complete, "test-batch-id"}

      assert {:ok, :completed} == SlotMessageStore.batch_progress(consumer_id, "test-batch-id")
    end
  end

  describe "SlotMessageStore wal cursors" do
    setup do
      consumer = ConsumersFactory.insert_sink_consumer!()

      start_supervised!({SlotMessageStore, consumer_id: consumer.id, test_pid: self()})

      %{consumer: consumer}
    end

    @tag :capture_log
    test "min_wal_cursor raises on ref mismatch", %{consumer: consumer} do
      pid = GenServer.whereis(SlotMessageStore.via_tuple(consumer.id))
      ref = make_ref()
      :ok = SlotMessageStore.set_monitor_ref(pid, ref)

      assert SlotMessageStore.min_unpersisted_wal_cursor(consumer.id, ref) == nil

      assert_raise(InvariantError, fn ->
        SlotMessageStore.min_unpersisted_wal_cursor(consumer.id, make_ref())
      end)
    end
  end

  describe "backfills, in isolation" do
    setup do
      table = DatabasesFactory.table()
      db = DatabasesFactory.insert_postgres_database!(tables: [table])
      consumer = ConsumersFactory.insert_sink_consumer!(account_id: db.account_id, postgres_database: db)

      # Create a backfill
      backfill = ConsumersFactory.insert_active_backfill!(account_id: consumer.account_id, sink_consumer_id: consumer.id)

      %{consumer: consumer, backfill: backfill}
    end

    test "when current backfill cursor is nil, calls fetch with initial_min_cursor", %{
      consumer: consumer,
      backfill: backfill
    } do
      test_pid = self()
      stub(TableReaderMock, :cursor, fn _backfill_id -> nil end)

      stub(TableReaderMock, :fetch_batch_primary_keys, fn _db_or_conn, _table, min_cursor ->
        send(test_pid, {:cursor, min_cursor})
        # Don't return a result to the SMS
        :sys.suspend(self())
      end)

      start_supervised!({SlotMessageStore, consumer_id: consumer.id, test_pid: self(), table_reader_mod: TableReaderMock})

      assert_receive {:cursor, min_cursor}
      [{key, value}] = Enum.to_list(min_cursor)
      [{expected_key, expected_value}] = Enum.to_list(backfill.initial_min_cursor)
      assert key == expected_key
      assert Sequin.Time.parse_timestamp!(value) == expected_value
    end

    test "when there's a saved cursor, calls fetch with that cursor", %{consumer: consumer} do
      test_pid = self()
      cursor = %{5 => DateTime.utc_now()}
      stub(TableReaderMock, :cursor, fn _backfill_id -> cursor end)

      stub(TableReaderMock, :fetch_batch_primary_keys, fn _db_or_conn, _table, cursor ->
        send(test_pid, {:cursor, cursor})
        # Don't return a result to the SMS
        :sys.suspend(self())
      end)

      start_supervised!({SlotMessageStore, consumer_id: consumer.id, test_pid: self(), table_reader_mod: TableReaderMock})

      assert_receive {:cursor, received_cursor}
      assert received_cursor == cursor
    end

    test "when backfill_fetch_ids returns an empty list, backfill marked completed", %{
      consumer: consumer,
      backfill: backfill
    } do
      assert backfill.state == :active
      stub(TableReaderMock, :cursor, fn _backfill_id -> nil end)
      stub(TableReaderMock, :fetch_batch_primary_keys, fn _db_or_conn, _table, _min_cursor -> {:ok, []} end)

      start_supervised!({SlotMessageStore, consumer_id: consumer.id, test_pid: self(), table_reader_mod: TableReaderMock})

      assert_receive {SlotMessageStore, :backfill_completed}

      assert Repo.reload(backfill).state == :completed
    end

    test "when backfill_fetch_ids returns a list of ids, calls fetch_batch_by_primary_keys and emits logical message", %{
      consumer: consumer
    } do
      test_pid = self()
      pks = [["1"], ["2"], ["3"]]
      next_cursor = %{5 => DateTime.utc_now()}
      records = for pk <- pks, do: ConsumersFactory.consumer_record(record_pks: pk)

      expect(TableReaderMock, :cursor, fn _backfill_id -> nil end)
      expect(TableReaderMock, :fetch_batch_primary_keys, fn _db_or_conn, _table, _min_cursor -> {:ok, pks} end)

      expect(TableReaderMock, :fetch_batch_by_primary_keys, fn _db, _consumer_id, _table, ^pks ->
        {:ok, %{messages: records, next_cursor: next_cursor}}
      end)

      expect(TableReaderMock, :emit_logic_message, fn _db, _consumer_id ->
        send(test_pid, :emit_logic_message)
        {:ok, 1}
      end)

      start_supervised!({SlotMessageStore, consumer_id: consumer.id, test_pid: self(), table_reader_mod: TableReaderMock})

      assert_receive :emit_logic_message
    end

    test "when backfill_fetch_ids returns an error, retries"

    test "when backfill_fetch_messages returns before the logical message, awaits the logical message before flushing", %{
      consumer: consumer
    } do
      test_pid = self()
      pks = [["1"], ["2"], ["3"]]
      next_cursor = %{5 => DateTime.utc_now()}
      records = for pk <- pks, do: ConsumersFactory.consumer_record(record_pks: pk)
      watermark_lsn = 100

      stub(TableReaderMock, :cursor, fn _backfill_id -> nil end)
      stub(TableReaderMock, :fetch_batch_primary_keys, fn _db_or_conn, _table, _min_cursor -> {:ok, pks} end)

      stub(TableReaderMock, :fetch_batch_by_primary_keys, fn _db, _consumer_id, _table, ^pks ->
        {:ok, %{messages: records, next_cursor: next_cursor}}
      end)

      stub(TableReaderMock, :emit_logic_message, fn _db, _consumer_id ->
        send(test_pid, :emit_logic_message)
        {:ok, watermark_lsn}
      end)

      start_supervised!({SlotMessageStore, consumer_id: consumer.id, test_pid: self(), table_reader_mod: TableReaderMock})

      assert_receive :emit_logic_message

      {:ok, []} = SlotMessageStore.produce(consumer.id, 1, self())

      :ok = SlotMessageStore.backfill_logical_message_received(consumer.id, watermark_lsn)

      assert_receive {SlotMessageStore, :backfill_flush_messages}

      {:ok, [msg1, msg2, msg3]} = SlotMessageStore.produce(consumer.id, 3, self())

      assert msg1.commit_lsn == watermark_lsn
      assert msg2.commit_lsn == watermark_lsn
      assert msg3.commit_lsn == watermark_lsn
      assert msg1.record_pks == ["1"]
      assert msg2.record_pks == ["2"]
      assert msg3.record_pks == ["3"]
    end

    test "when backfill_flush_messages returns after the logical message, flushes right away", %{consumer: consumer} do
      pks = [["1"], ["2"], ["3"]]
      next_cursor = %{5 => DateTime.utc_now()}
      records = for pk <- pks, do: ConsumersFactory.consumer_record(record_pks: pk)
      watermark_lsn = 100

      stub(TableReaderMock, :cursor, fn _backfill_id -> nil end)
      stub(TableReaderMock, :fetch_batch_primary_keys, fn _db_or_conn, _table, _min_cursor -> {:ok, pks} end)

      stub(TableReaderMock, :fetch_batch_by_primary_keys, fn _db, _consumer_id, _table, ^pks ->
        {:ok, %{messages: records, next_cursor: next_cursor}}
      end)

      stub(TableReaderMock, :emit_logic_message, fn _db, _consumer_id ->
        # Call in here, so we know the message is received first
        :ok = SlotMessageStore.backfill_logical_message_received(consumer.id, watermark_lsn)
        {:ok, watermark_lsn}
      end)

      start_supervised!({SlotMessageStore, consumer_id: consumer.id, test_pid: self(), table_reader_mod: TableReaderMock})

      assert_receive {SlotMessageStore, :backfill_flush_messages}
    end

    test "when backfill_flush_messages errors, retries"

    # test "backfill end-to-end", %{consumer: consumer, backfill: backfill} do
    # stub(TableReaderMock, :cursor, fn _backfill_id -> nil end)
    # stub(TableReaderMock, :fetch_batch_primary_keys, fn _db_or_conn, _table, _min_cursor -> {:ok, []} end)

    # stub(TableReaderMock, :fetch_batch_by_primary_keys, fn _db_or_conn, _consumer, _table, _primary_keys ->
    #   {:ok, %{messages: [], next_cursor: nil}}
    # end)

    # stub(TableReaderMock, :emit_logic_message, fn _db, _consumer_id -> :ok end)

    # start_supervised!({SlotMessageStore, consumer_id: consumer.id, test_pid: self(), table_reader_mod: TableReaderMock})

    # Process.sleep(100)
    # assert true
    # end
  end
end

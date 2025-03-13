defmodule Sequin.SlotMessageStoreTest do
  use Sequin.DataCase, async: true

  alias Sequin.Consumers
  alias Sequin.Error.InvariantError
  alias Sequin.Factory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Runtime.SlotMessageStore
  alias Sequin.Runtime.SlotMessageStore.State
  alias Sequin.Runtime.SlotMessageStoreSupervisor

  @moduletag :capture_log

  describe "SlotMessageStore with persisted messages" do
    setup do
      consumer = ConsumersFactory.insert_sink_consumer!()

      msg1 = ConsumersFactory.insert_consumer_message!(message_kind: consumer.message_kind, consumer_id: consumer.id)
      msg2 = ConsumersFactory.insert_consumer_message!(message_kind: consumer.message_kind, consumer_id: consumer.id)

      start_supervised!({SlotMessageStoreSupervisor, consumer: consumer, test_pid: self()})

      %{consumer: consumer, msg1: msg1, msg2: msg2}
    end

    test "messages from disk are loaded on init and then produce_messages returns them", %{
      consumer: consumer,
      msg1: msg1,
      msg2: msg2
    } do
      assert {:ok, delivered} = SlotMessageStore.produce(consumer, 2, self())
      assert length(delivered) == 2
      assert Enum.all?(delivered, fn msg -> msg.id in [msg1.id, msg2.id] end)

      assert Enum.any?(SlotMessageStore.peek(consumer), fn state ->
               state.payload_size_bytes > 0
             end)
    end

    test "producing and acking persisted messages deletes them from disk", %{consumer: consumer} do
      refute [] == Consumers.list_consumer_messages_for_consumer(consumer)

      assert {:ok, delivered} = SlotMessageStore.produce(consumer, 10, self())
      assert length(delivered) == 2

      ack_ids = Enum.map(delivered, & &1.ack_id)
      {:ok, 2} = SlotMessageStore.messages_succeeded(consumer, ack_ids)

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

      :ok = SlotMessageStore.put_messages(consumer, [new_message])

      consumer_id = consumer.id
      assert_receive {:put_messages_done, ^consumer_id}, 1000

      persisted_messages = Consumers.list_consumer_messages_for_consumer(consumer)
      assert length(persisted_messages) == 3

      assert new_message.record_pks in Enum.map(persisted_messages, & &1.record_pks)
    end
  end

  describe "SlotMessageStore message handling" do
    setup do
      consumer = ConsumersFactory.insert_sink_consumer!()
      event_consumer = ConsumersFactory.insert_sink_consumer!(message_kind: :event)
      start_supervised!({SlotMessageStoreSupervisor, consumer: consumer, test_pid: self()})
      start_supervised!({SlotMessageStoreSupervisor, consumer: event_consumer, test_pid: self()})
      %{consumer: consumer, event_consumer: event_consumer}
    end

    test "puts, delivers, and acks in-memory messages", %{consumer: consumer} do
      # Create test events
      messages = [
        ConsumersFactory.consumer_message(),
        ConsumersFactory.consumer_message()
      ]

      # Put messages in store
      :ok = SlotMessageStore.put_messages(consumer, messages)

      # Retrieve messages
      {:ok, delivered} = SlotMessageStore.produce(consumer, 2, self())
      assert length(delivered) == 2

      # For acks
      ack_ids = Enum.map(delivered, & &1.ack_id)
      {:ok, 2} = SlotMessageStore.messages_succeeded(consumer, ack_ids)

      # Produce messages, none should be delivered
      {:ok, []} = SlotMessageStore.produce(consumer, 2, self())
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
      :ok = SlotMessageStore.put_messages(consumer, [message])

      # Deliver message
      {:ok, [delivered]} = SlotMessageStore.produce(consumer, 1, self())

      # Fail the message (this will persist it)
      meta = %{
        ack_id: delivered.ack_id,
        deliver_count: 1,
        last_delivered_at: DateTime.utc_now(),
        not_visible_until: DateTime.utc_now(),
        group_id: delivered.group_id
      }

      :ok = SlotMessageStore.messages_failed(consumer, [meta])

      # Verify message is persisted
      assert [_persisted] = Consumers.list_consumer_messages_for_consumer(consumer)

      # Redeliver and succeed the message
      {:ok, [redelivered]} = SlotMessageStore.produce(consumer, 1, self())
      {:ok, 1} = SlotMessageStore.messages_succeeded(consumer, [redelivered.ack_id])

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
      :ok = SlotMessageStore.put_messages(consumer, [message])

      # Deliver message
      {:ok, [delivered]} = SlotMessageStore.produce(consumer, 1, self())

      # Fail the message (this will persist it)
      meta = %{
        ack_id: delivered.ack_id,
        deliver_count: 1,
        last_delivered_at: DateTime.utc_now(),
        not_visible_until: DateTime.utc_now(),
        group_id: delivered.group_id
      }

      :ok = SlotMessageStore.messages_failed(consumer, [meta])

      # Verify message is persisted
      assert [_persisted] = Consumers.list_consumer_messages_for_consumer(consumer)

      # Redeliver and succeed the message
      {:ok, [redelivered]} = SlotMessageStore.produce(consumer, 1, self())
      {:ok, 1} = SlotMessageStore.messages_already_succeeded(consumer, [redelivered.ack_id])

      # Verify message was deleted from postgres
      assert [] = Consumers.list_consumer_messages_for_consumer(consumer)
    end

    test "delivers and fails messages to disk", %{consumer: consumer} do
      messages = [
        ConsumersFactory.consumer_message(message_kind: consumer.message_kind, consumer_id: consumer.id),
        ConsumersFactory.consumer_message(message_kind: consumer.message_kind, consumer_id: consumer.id)
      ]

      # Put messages in store
      :ok = SlotMessageStore.put_messages(consumer, messages)

      # Produce messages
      {:ok, delivered} = SlotMessageStore.produce(consumer, 2, self())
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
            not_visible_until: not_visible_until,
            group_id: msg.group_id
          }
        end)

      :ok = SlotMessageStore.messages_failed(consumer, metas)

      # Verify messages were persisted with updated metadata
      messages = SlotMessageStore.peek_messages(consumer, 2)
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
      :ok = SlotMessageStore.put_messages(consumer, [message1, message2])

      # Get and fail first message
      {:ok, [delivered]} = SlotMessageStore.produce(consumer, 1, self())

      meta = %{
        ack_id: delivered.ack_id,
        deliver_count: 1,
        group_id: delivered.group_id,
        last_delivered_at: DateTime.utc_now(),
        not_visible_until: DateTime.add(DateTime.utc_now(), 60)
      }

      :ok = SlotMessageStore.messages_failed(consumer, [meta])

      # Verify both messages are now persisted
      messages = SlotMessageStore.peek_messages(consumer, 2)
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
      :ok = SlotMessageStore.put_messages(consumer, messages)

      # Produce messages, none should be delivered
      {:ok, delivered} = SlotMessageStore.produce(consumer, 2, self())
      assert length(delivered) == 2
      {:ok, []} = SlotMessageStore.produce(consumer, 2, self())

      {:ok, delivered} = SlotMessageStore.produce(consumer, 2, Factory.pid())
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
      :ok = SlotMessageStore.put_messages(consumer, [message1])
      {:ok, [delivered]} = SlotMessageStore.produce(consumer, 1, self())

      meta = %{
        ack_id: delivered.ack_id,
        deliver_count: 1,
        group_id: nil,
        last_delivered_at: DateTime.utc_now(),
        not_visible_until: DateTime.add(DateTime.utc_now(), 60)
      }

      :ok = SlotMessageStore.messages_failed(consumer, [meta])

      # Put second message - it should not be blocked or persisted
      :ok = SlotMessageStore.put_messages(consumer, [message2])
      [persisted_msg] = Consumers.list_consumer_messages_for_consumer(consumer)
      assert persisted_msg.commit_lsn == message1.commit_lsn
      {:ok, [message]} = SlotMessageStore.produce(consumer, 1, self())
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

      :ok = SlotMessageStore.put_messages(consumer, [message])
      {:ok, [delivered]} = SlotMessageStore.produce(consumer, 1, self())

      meta = %{
        ack_id: delivered.ack_id,
        deliver_count: 1,
        group_id: nil,
        last_delivered_at: DateTime.utc_now(),
        not_visible_until: DateTime.add(DateTime.utc_now(), 60)
      }

      :ok = SlotMessageStore.messages_failed(consumer, [meta])

      # Peek at state to verify is_message_group_persisted? returns false
      Enum.each(SlotMessageStore.peek(consumer), fn state ->
        refute State.is_message_group_persisted?(state, nil)
      end)
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
      :ok = SlotMessageStore.put_messages(consumer, [message1])
      {:ok, [delivered]} = SlotMessageStore.produce(consumer, 1, self())

      meta = %{
        ack_id: delivered.ack_id,
        deliver_count: 1,
        group_id: delivered.group_id,
        last_delivered_at: DateTime.utc_now(),
        not_visible_until: DateTime.add(DateTime.utc_now(), 60)
      }

      :ok = SlotMessageStore.messages_failed(consumer, [meta])

      # Put second message (nil group_id)
      :ok = SlotMessageStore.put_messages(consumer, [message2])

      # Verify only messages with non-nil group_ids are blocked
      Enum.each(SlotMessageStore.peek(consumer), fn state ->
        {blocked_messages, _state} = State.pop_blocked_messages(state)
        assert length(blocked_messages) == 0
      end)
    end

    test "duplicate messages don't accumulate payload size", %{consumer: consumer} do
      # Create a message
      message =
        ConsumersFactory.consumer_message(
          message_kind: consumer.message_kind,
          consumer_id: consumer.id,
          commit_lsn: 1,
          commit_idx: 1
        )

      # Put the same message multiple times
      :ok = SlotMessageStore.put_messages(consumer, [message])
      :ok = SlotMessageStore.put_messages(consumer, [message])
      :ok = SlotMessageStore.put_messages(consumer, [message])

      # Verify we can only produce it once
      {:ok, [delivered]} = SlotMessageStore.produce(consumer, 2, self())
      assert length([delivered]) == 1
      assert delivered.commit_lsn == 1
      assert delivered.commit_idx == 1

      # Ack the message
      {:ok, 1} = SlotMessageStore.messages_succeeded(consumer, [delivered.ack_id])

      # Verify no bytes are accumulated
      Enum.each(SlotMessageStore.peek(consumer), fn state ->
        assert state.payload_size_bytes == 0
      end)
    end

    test "messages_succeeded_returning_messages returns the acked messages", %{consumer: consumer} do
      # Create test messages
      messages = [
        ConsumersFactory.consumer_message(),
        ConsumersFactory.consumer_message()
      ]

      # Put messages in store
      :ok = SlotMessageStore.put_messages(consumer, messages)

      # Retrieve messages
      {:ok, delivered} = SlotMessageStore.produce(consumer, 2, self())
      assert length(delivered) == 2

      # Get ack_ids and call messages_succeeded_returning_messages
      ack_ids = Enum.map(delivered, & &1.ack_id)
      {:ok, returned_messages} = SlotMessageStore.messages_succeeded_returning_messages(consumer, ack_ids)

      # Verify returned messages match what was delivered
      assert length(returned_messages) == 2
      assert_lists_equal(Enum.map(returned_messages, & &1.ack_id), ack_ids)

      # Verify messages were removed from store
      {:ok, []} = SlotMessageStore.produce(consumer, 2, self())
    end
  end

  describe "SlotMessageStore table reader batch handling" do
    setup do
      consumer = ConsumersFactory.insert_sink_consumer!()

      start_supervised!({SlotMessageStoreSupervisor, consumer: consumer, test_pid: self()})

      %{consumer: consumer}
    end

    test "puts batch and reports on batch progress", %{consumer: consumer} do
      consumer_id = consumer.id
      :syn.join(:consumers, {:table_reader_batches_changed, consumer_id}, self())

      # Create test events
      messages = [
        ConsumersFactory.consumer_message(),
        ConsumersFactory.consumer_message()
      ]

      # Put messages in store
      :ok = SlotMessageStore.put_table_reader_batch(consumer, messages, "test-batch-id")

      # Retrieve messages
      {:ok, [delivered]} = SlotMessageStore.produce(consumer, 1, self())

      assert {:ok, ["test-batch-id"]} == SlotMessageStore.unpersisted_table_reader_batch_ids(consumer)

      # For acks
      {:ok, 1} = SlotMessageStore.messages_succeeded(consumer, [delivered.ack_id])

      # Produce messages, none should be delivered
      {:ok, [delivered]} = SlotMessageStore.produce(consumer, 1, self())

      # Delivered messages don't "complete" a batch
      assert {:ok, ["test-batch-id"]} == SlotMessageStore.unpersisted_table_reader_batch_ids(consumer)

      {:ok, 1} = SlotMessageStore.messages_succeeded(consumer, [delivered.ack_id])

      assert_received :table_reader_batches_changed

      assert {:ok, []} == SlotMessageStore.unpersisted_table_reader_batch_ids(consumer)
    end
  end

  describe "SlotMessageStore wal cursors" do
    setup do
      consumer = ConsumersFactory.insert_sink_consumer!()

      start_supervised!({SlotMessageStoreSupervisor, consumer: consumer, test_pid: self()})

      %{consumer: consumer}
    end

    test "min_wal_cursor raises on ref mismatch", %{consumer: consumer} do
      ref = make_ref()
      :ok = SlotMessageStore.set_monitor_ref(consumer, ref)

      assert SlotMessageStore.min_unpersisted_wal_cursors(consumer, ref) == []

      assert_raise(InvariantError, fn ->
        SlotMessageStore.min_unpersisted_wal_cursors(consumer, make_ref())
      end)
    end
  end

  describe "SlotMessageStore flush behavior" do
    setup do
      consumer = ConsumersFactory.insert_sink_consumer!()

      start_supervised!(
        {SlotMessageStoreSupervisor,
         consumer: consumer, test_pid: self(), flush_interval: 100, message_age_before_flush_ms: 100}
      )

      %{consumer: consumer}
    end

    test "identifies messages for flushing after age threshold", %{consumer: consumer} do
      consumer_id = consumer.id

      ref = make_ref()
      :ok = SlotMessageStore.set_monitor_ref(consumer, ref)

      # Create a message that will be old enough to flush
      message =
        ConsumersFactory.consumer_message(
          message_kind: consumer.message_kind,
          consumer_id: consumer.id
        )

      # Put message in store
      :ok = SlotMessageStore.put_messages(consumer, [message])

      # Initially we have one unpersisted commit tuple
      assert SlotMessageStore.min_unpersisted_wal_cursors(consumer, ref) == [
               %{commit_lsn: message.commit_lsn, commit_idx: message.commit_idx}
             ]

      # Verify we receive flush_messages_done
      assert_receive {:flush_messages_done, ^consumer_id}, 1000

      persisted_messages = Consumers.list_consumer_messages_for_consumer(consumer)
      assert length(persisted_messages) == 1

      # This is the point- the wal cursor is now persisted!
      assert SlotMessageStore.min_unpersisted_wal_cursors(consumer, ref) == []
    end

    test "persisted messages are not identified for flushing regardless of age", %{consumer: consumer} do
      # Create a message that will be persisted
      message =
        ConsumersFactory.consumer_message(
          message_kind: consumer.message_kind,
          consumer_id: consumer.id
        )

      # Put message and fail it to persist it
      :ok = SlotMessageStore.put_messages(consumer, [message])
      {:ok, [delivered]} = SlotMessageStore.produce(consumer, 1, self())

      meta = %{
        ack_id: delivered.ack_id,
        deliver_count: 1,
        group_id: delivered.group_id,
        last_delivered_at: DateTime.utc_now(),
        not_visible_until: DateTime.add(DateTime.utc_now(), 60)
      }

      :ok = SlotMessageStore.messages_failed(consumer, [meta])

      refute_receive {:flush_messages_done, _}, 100
    end
  end
end

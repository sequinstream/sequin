defmodule Sequin.DatabasesRuntime.SlotMessageStoreStateTest do
  use Sequin.DataCase, async: true

  alias Sequin.DatabasesRuntime.SlotMessageStore.State
  alias Sequin.Factory.ConsumersFactory

  describe "put_messages/2 with :event messages" do
    setup do
      consumer = ConsumersFactory.sink_consumer(message_kind: :event)

      state = %State{
        consumer: consumer,
        messages: %{},
        flush_interval: 1000,
        flush_batch_size: 100,
        flush_wait_ms: 1
      }

      {:ok, %{state: state}}
    end

    test "merges new event messages into empty state", %{state: state} do
      event1 = ConsumersFactory.consumer_event()
      event2 = ConsumersFactory.consumer_event()
      expect_uuid4(fn -> event1.ack_id end)
      expect_uuid4(fn -> event2.ack_id end)
      state = State.put_messages(state, [event1, event2])

      assert map_size(state.messages) == 2
      assert Map.has_key?(state.messages, event1.ack_id)
      assert Map.has_key?(state.messages, event2.ack_id)
    end

    test "merges new event messages with existing messages", %{state: state} do
      # Add initial message
      event1 = ConsumersFactory.consumer_event()
      expect_uuid4(fn -> event1.ack_id end)
      state = State.put_messages(state, [event1])

      # Add new message
      event2 = ConsumersFactory.consumer_event()
      expect_uuid4(fn -> event2.ack_id end)
      updated_state = State.put_messages(state, [event2])

      assert map_size(updated_state.messages) == 2
      assert Map.has_key?(updated_state.messages, event1.ack_id)
      assert Map.has_key?(updated_state.messages, event2.ack_id)
    end
  end

  describe "put_messages/2 with :record messages" do
    setup do
      consumer = ConsumersFactory.sink_consumer(message_kind: :record)

      state = %State{
        consumer: consumer,
        messages: %{},
        flush_interval: 1000,
        flush_batch_size: 100,
        flush_wait_ms: 1
      }

      {:ok, %{state: state}}
    end

    test "merges new record messages into empty state", %{state: state} do
      record1 = ConsumersFactory.consumer_record()
      record2 = ConsumersFactory.consumer_record()
      expect_uuid4(fn -> record1.ack_id end)
      expect_uuid4(fn -> record2.ack_id end)

      state = State.put_messages(state, [record1, record2])

      assert map_size(state.messages) == 2
      assert Map.has_key?(state.messages, record1.ack_id)
      assert Map.has_key?(state.messages, record2.ack_id)
    end

    test "merges new record messages with existing messages", %{state: state} do
      # Add initial message
      record1 = ConsumersFactory.consumer_record()
      expect_uuid4(fn -> record1.ack_id end)
      state = State.put_messages(state, [record1])

      # Add new message
      record2 = ConsumersFactory.consumer_record()
      expect_uuid4(fn -> record2.ack_id end)
      updated_state = State.put_messages(state, [record2])

      assert map_size(updated_state.messages) == 2
      assert Map.has_key?(updated_state.messages, record1.ack_id)
      assert Map.has_key?(updated_state.messages, record2.ack_id)
    end
  end

  describe "deliverable_messages/2" do
    setup do
      consumer = ConsumersFactory.sink_consumer(message_kind: :record)

      state = %State{
        consumer: consumer,
        messages: %{},
        flush_interval: 1000,
        flush_batch_size: 100,
        flush_wait_ms: 1
      }

      {:ok, %{state: state}}
    end

    test "excludes messages that are not yet visible", %{state: state} do
      future_time = DateTime.add(DateTime.utc_now(), 30, :second)

      visible_msg =
        ConsumersFactory.consumer_record(
          seq: 1,
          not_visible_until: nil
        )

      invisible_msg =
        ConsumersFactory.consumer_record(
          seq: 2,
          not_visible_until: future_time
        )

      messages = Map.new([visible_msg, invisible_msg], &{&1.record_pks, &1})
      state = %{state | messages: messages}

      deliverable = State.deliverable_messages(state, 10)

      assert length(deliverable) == 1
      assert hd(deliverable).seq == visible_msg.seq
    end

    test "respects message group visibility", %{state: state} do
      group_id = "group1"
      future_time = DateTime.add(DateTime.utc_now(), 30, :second)

      # Create three messages in same group, one pending
      msg1 = ConsumersFactory.consumer_record(seq: 1, group_id: group_id)
      msg2 = ConsumersFactory.consumer_record(seq: 2, group_id: group_id, not_visible_until: future_time)
      msg3 = ConsumersFactory.consumer_record(seq: 3, group_id: group_id)

      messages = Map.new([msg1, msg2, msg3], &{&1.record_pks, &1})
      state = %{state | messages: messages}

      deliverable = State.deliverable_messages(state, 10)

      assert Enum.empty?(deliverable),
             "Should not deliver any messages when group has pending message"
    end
  end

  describe "deliver_messages/2" do
    setup do
      consumer = ConsumersFactory.sink_consumer(message_kind: :record, ack_wait_ms: 30_000)

      state = %State{
        consumer: consumer,
        messages: %{},
        flush_interval: 1000,
        flush_batch_size: 100,
        flush_wait_ms: 1
      }

      {:ok, %{state: state, consumer: consumer}}
    end

    test "updates message delivery state and metadata", %{state: state, consumer: consumer} do
      now = DateTime.utc_now()
      not_visible_until = DateTime.add(now, consumer.ack_wait_ms, :millisecond)
      # Create messages in available state
      msg1 =
        ConsumersFactory.consumer_record(
          deliver_count: 0,
          last_delivered_at: nil,
          not_visible_until: nil,
          state: :available
        )

      msg2 =
        ConsumersFactory.consumer_record(
          deliver_count: 1,
          last_delivered_at: DateTime.add(DateTime.utc_now(), -60, :second),
          not_visible_until: nil,
          state: :available
        )

      messages = [msg1, msg2]
      state = %{state | messages: Map.new(messages, &{&1.ack_id, &1})}

      now = DateTime.utc_now()
      {updated_state, delivered_messages} = State.deliver_messages(state, messages)

      # Check first message
      delivered1 = updated_state.messages[msg1.ack_id]
      assert delivered1.deliver_count == 1
      assert DateTime.compare(delivered1.last_delivered_at, now) in [:eq, :gt]
      assert DateTime.compare(delivered1.not_visible_until, not_visible_until) in [:eq, :gt]
      assert delivered1.state == :delivered
      assert delivered1.dirty == true

      # Check second message
      delivered2 = updated_state.messages[msg2.ack_id]
      assert delivered2.deliver_count == 2
      assert DateTime.compare(delivered2.last_delivered_at, now) in [:eq, :gt]
      assert DateTime.compare(delivered2.not_visible_until, not_visible_until) in [:eq, :gt]
      assert delivered2.state == :delivered
      assert delivered2.dirty == true

      # Verify returned messages match state
      assert delivered_messages == [delivered1, delivered2]
    end
  end

  describe "flush_messages/1" do
    setup do
      consumer = ConsumersFactory.insert_sink_consumer!(message_kind: :record)

      state = %State{
        consumer: consumer,
        messages: %{},
        flush_interval: 1000,
        # Small batch size for testing
        flush_batch_size: 3,
        flush_wait_ms: :timer.seconds(60)
      }

      {:ok, %{state: state, two_minutes_ago: DateTime.add(DateTime.utc_now(), -120, :second)}}
    end

    test "prioritizes never-flushed messages", %{state: state, two_minutes_ago: two_minutes_ago} do
      never_flushed1 =
        ConsumersFactory.consumer_record(
          record_pks: [1],
          consumer_id: state.consumer.id,
          flushed_at: nil,
          dirty: true,
          ingested_at: two_minutes_ago
        )

      never_flushed2 =
        ConsumersFactory.consumer_record(
          record_pks: [2],
          consumer_id: state.consumer.id,
          flushed_at: nil,
          dirty: true,
          ingested_at: two_minutes_ago
        )

      previously_flushed1 =
        ConsumersFactory.consumer_record(
          record_pks: [3],
          consumer_id: state.consumer.id,
          flushed_at: DateTime.add(DateTime.utc_now(), -60, :second),
          dirty: true,
          ingested_at: two_minutes_ago
        )

      previously_flushed2 =
        ConsumersFactory.consumer_record(
          record_pks: [4],
          consumer_id: state.consumer.id,
          flushed_at: DateTime.add(DateTime.utc_now(), -60, :second),
          dirty: true,
          ingested_at: two_minutes_ago
        )

      messages = Map.new([never_flushed1, never_flushed2, previously_flushed1, previously_flushed2], &{&1.ack_id, &1})
      state = %{state | messages: messages}

      pre_flush_datetime = DateTime.utc_now()
      {state, more?} = State.flush_messages(state)
      assert more?, "Should be more messages to flush"
      messages = Map.values(state.messages)

      # The unflushed message should be one of the previously flushed messages
      assert Enum.find(messages, &DateTime.before?(&1.flushed_at, pre_flush_datetime)).record_pks in [["3"], ["4"]]
    end

    test "respects flush_batch_size limit", %{state: state, two_minutes_ago: two_minutes_ago} do
      messages =
        for _i <- 1..5 do
          ConsumersFactory.consumer_record(
            consumer_id: state.consumer.id,
            flushed_at: nil,
            dirty: true,
            ingested_at: two_minutes_ago
          )
        end

      state = %{state | messages: Map.new(messages, &{&1.ack_id, &1})}

      {state, more?} = State.flush_messages(state)
      assert more?, "Should be more messages to flush"
      messages = Map.values(state.messages)

      # 3 messages should have been flushed, matching flush_batch_size
      assert Enum.count(messages, &(&1.flushed_at != nil)) == 3
    end

    test "excludes non-dirty messages", %{state: state, two_minutes_ago: two_minutes_ago} do
      dirty_msg =
        ConsumersFactory.consumer_record(
          record_pks: [1],
          consumer_id: state.consumer.id,
          flushed_at: DateTime.utc_now(),
          dirty: true,
          ingested_at: two_minutes_ago
        )

      clean_msg =
        ConsumersFactory.consumer_record(
          record_pks: [2],
          consumer_id: state.consumer.id,
          flushed_at: DateTime.utc_now(),
          dirty: false,
          ingested_at: two_minutes_ago
        )

      messages = Map.new([dirty_msg, clean_msg], &{&1.ack_id, &1})
      state = %{state | messages: messages}

      pre_flush_datetime = DateTime.utc_now()
      {state, more?} = State.flush_messages(state)
      refute more?, "Should not be more messages to flush"
      messages = Map.values(state.messages)

      assert Enum.find(messages, &DateTime.before?(&1.flushed_at, pre_flush_datetime)).record_pks == ["2"]
    end

    test "excludes messages that are less than flush_wait_ms old", %{state: state} do
      recent_message =
        ConsumersFactory.consumer_record(
          record_pks: [1],
          consumer_id: state.consumer.id,
          dirty: true,
          ingested_at: DateTime.utc_now()
        )

      old_message =
        ConsumersFactory.consumer_record(
          record_pks: [2],
          consumer_id: state.consumer.id,
          dirty: true,
          ingested_at: DateTime.add(DateTime.utc_now(), -state.flush_wait_ms, :millisecond)
        )

      state = %{state | messages: Map.new([recent_message, old_message], &{&1.ack_id, &1})}

      {state, more?} = State.flush_messages(state)
      refute more?, "Should not be more messages to flush"
      messages = Map.values(state.messages)

      assert Enum.find(messages, &(&1.flushed_at == nil)).record_pks == ["1"]
    end
  end

  describe "min_unflushed_commit_lsn/1" do
    setup do
      consumer = ConsumersFactory.sink_consumer()

      state = %State{
        consumer: consumer,
        messages: %{},
        flush_interval: 1000,
        flush_batch_size: 100,
        slot_processor_monitor_ref: make_ref()
      }

      {:ok, %{state: state, message_kind: consumer.message_kind}}
    end

    test "returns nil when there are no records or events", %{state: state} do
      assert State.min_unflushed_commit_lsn(state, state.slot_processor_monitor_ref) == nil
    end

    test "returns nil when all messages are flushed", %{state: state, message_kind: message_kind} do
      messages = [
        ConsumersFactory.consumer_message(message_kind: message_kind, commit_lsn: 100, flushed_at: DateTime.utc_now()),
        ConsumersFactory.consumer_message(message_kind: message_kind, commit_lsn: 200, flushed_at: DateTime.utc_now())
      ]

      state = State.put_messages(state, messages)

      assert State.min_unflushed_commit_lsn(state, state.slot_processor_monitor_ref) == nil
    end

    test "returns lowest commit_lsn from unflushed messages", %{state: state, message_kind: message_kind} do
      messages =
        Enum.shuffle([
          ConsumersFactory.consumer_message(message_kind: message_kind, commit_lsn: 300, flushed_at: DateTime.utc_now()),
          ConsumersFactory.consumer_message(message_kind: message_kind, commit_lsn: 100, flushed_at: nil),
          ConsumersFactory.consumer_message(message_kind: message_kind, commit_lsn: 200, flushed_at: nil)
        ])

      state = State.put_messages(state, messages)

      assert State.min_unflushed_commit_lsn(state, state.slot_processor_monitor_ref) == 100
    end

    test "raises when monitor ref mismatch", %{state: state} do
      assert_raise RuntimeError, fn ->
        State.min_unflushed_commit_lsn(state, make_ref())
      end
    end
  end

  describe "peek_messages/2" do
    test "returns messages in seq order" do
      state = %State{
        consumer: ConsumersFactory.sink_consumer(),
        messages: %{}
      }

      messages =
        Enum.shuffle([
          ConsumersFactory.consumer_message(seq: 1),
          ConsumersFactory.consumer_message(seq: 2),
          ConsumersFactory.consumer_message(seq: 3)
        ])

      state = State.put_messages(state, messages)

      assert peeked_messages = State.peek_messages(state, 3)
      assert Enum.map(peeked_messages, & &1.seq) == [1, 2, 3]
    end
  end
end

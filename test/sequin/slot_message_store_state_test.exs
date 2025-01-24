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
          commit_lsn: 1,
          commit_idx: 0,
          not_visible_until: nil
        )

      invisible_msg =
        ConsumersFactory.consumer_record(
          commit_lsn: 1,
          commit_idx: 1,
          not_visible_until: future_time
        )

      messages = Map.new([visible_msg, invisible_msg], &{&1.record_pks, &1})
      state = %{state | messages: messages}

      deliverable = State.deliverable_messages(state, 10)

      assert length(deliverable) == 1
      assert hd(deliverable).commit_lsn == visible_msg.commit_lsn
      assert hd(deliverable).commit_idx == visible_msg.commit_idx
    end

    test "respects message group visibility", %{state: state} do
      group_id = "group1"
      future_time = DateTime.add(DateTime.utc_now(), 30, :second)

      # Create three messages in same group, one pending
      msg1 = ConsumersFactory.consumer_record(commit_lsn: 1, commit_idx: 0, group_id: group_id)

      msg2 =
        ConsumersFactory.consumer_record(commit_lsn: 1, commit_idx: 1, group_id: group_id, not_visible_until: future_time)

      msg3 = ConsumersFactory.consumer_record(commit_lsn: 1, commit_idx: 2, group_id: group_id)

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

  describe "messages_to_flush/1" do
    setup do
      consumer = ConsumersFactory.sink_consumer(message_kind: :record)

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
      never_flushed1 = ConsumersFactory.consumer_record(flushed_at: nil, dirty: true, ingested_at: two_minutes_ago)
      never_flushed2 = ConsumersFactory.consumer_record(flushed_at: nil, dirty: true, ingested_at: two_minutes_ago)

      previously_flushed =
        ConsumersFactory.consumer_record(
          flushed_at: DateTime.add(DateTime.utc_now(), -60, :second),
          dirty: true,
          ingested_at: two_minutes_ago
        )

      messages = Map.new([never_flushed1, never_flushed2, previously_flushed], &{&1.record_pks, &1})
      state = %{state | messages: messages}

      to_flush = State.messages_to_flush(state)

      assert length(to_flush) == 3
      # Never flushed messages should come first
      assert to_flush |> Enum.take(2) |> Enum.all?(&is_nil(&1.flushed_at))
    end

    test "respects flush_batch_size limit", %{state: state, two_minutes_ago: two_minutes_ago} do
      messages =
        for _i <- 1..5 do
          ConsumersFactory.consumer_record(flushed_at: nil, dirty: true, ingested_at: two_minutes_ago)
        end

      state = %{state | messages: Map.new(messages, &{&1.record_pks, &1})}

      to_flush = State.messages_to_flush(state)

      # matches flush_batch_size
      assert length(to_flush) == 3
    end

    test "sorts previously flushed messages by flushed_at", %{state: state, two_minutes_ago: two_minutes_ago} do
      now = DateTime.utc_now()

      msg1 =
        ConsumersFactory.consumer_record(
          flushed_at: DateTime.add(now, -30, :second),
          dirty: true,
          ingested_at: two_minutes_ago
        )

      msg2 =
        ConsumersFactory.consumer_record(
          flushed_at: DateTime.add(now, -60, :second),
          dirty: true,
          ingested_at: two_minutes_ago
        )

      msg3 =
        ConsumersFactory.consumer_record(
          flushed_at: DateTime.add(now, -15, :second),
          dirty: true,
          ingested_at: two_minutes_ago
        )

      messages = Map.new([msg1, msg2, msg3], &{&1.ack_id, &1})
      state = %{state | messages: messages}

      to_flush = State.messages_to_flush(state)

      flushed_ats = Enum.map(to_flush, & &1.flushed_at)
      assert flushed_ats == Enum.sort(flushed_ats, DateTime)
    end

    test "excludes non-dirty messages", %{state: state, two_minutes_ago: two_minutes_ago} do
      dirty_msg = ConsumersFactory.consumer_record(flushed_at: nil, dirty: true, ingested_at: two_minutes_ago)
      clean_msg = ConsumersFactory.consumer_record(flushed_at: nil, dirty: false, ingested_at: two_minutes_ago)

      messages = Map.new([dirty_msg, clean_msg], &{&1.ack_id, &1})
      state = %{state | messages: messages}

      to_flush = State.messages_to_flush(state)

      assert length(to_flush) == 1
      assert hd(to_flush).record_pks == dirty_msg.record_pks
    end

    test "excludes messages that are less than 60 seconds old", %{state: state, two_minutes_ago: two_minutes_ago} do
      recent_message =
        ConsumersFactory.consumer_record(
          last_delivered_at: DateTime.utc_now(),
          dirty: true,
          ingested_at: DateTime.utc_now()
        )

      old_message =
        ConsumersFactory.consumer_record(
          last_delivered_at: DateTime.add(DateTime.utc_now(), -60, :second),
          dirty: true,
          ingested_at: two_minutes_ago
        )

      state = %{state | messages: Map.new([recent_message, old_message], &{&1.ack_id, &1})}

      to_flush = State.messages_to_flush(state)

      assert length(to_flush) == 1
      assert hd(to_flush).record_pks == old_message.record_pks
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
    test "returns messages in commit_lsn, commit_idx order" do
      state = %State{
        consumer: ConsumersFactory.sink_consumer(),
        messages: %{}
      }

      messages =
        Enum.shuffle([
          ConsumersFactory.consumer_message(commit_lsn: 1, commit_idx: 0),
          ConsumersFactory.consumer_message(commit_lsn: 1, commit_idx: 1),
          ConsumersFactory.consumer_message(commit_lsn: 1, commit_idx: 2)
        ])

      state = State.put_messages(state, messages)

      assert peeked_messages = State.peek_messages(state, 3)
      assert Enum.map(peeked_messages, & &1.commit_lsn) == [1, 1, 1]
      assert Enum.map(peeked_messages, & &1.commit_idx) == [0, 1, 2]
    end
  end
end

defmodule Sequin.ConsumersRuntime.ConsumerMessageStoreStateTest do
  use Sequin.DataCase, async: true

  alias Sequin.ConsumersRuntime.ConsumerMessageStore.State
  alias Sequin.Factory.ConsumersFactory

  describe "put_messages/2 with :event messages" do
    setup do
      consumer = ConsumersFactory.sink_consumer(message_kind: :event)

      state = %State{
        consumer: consumer,
        messages: %{},
        flush_interval: 1000,
        flush_batch_size: 100
      }

      {:ok, %{state: state}}
    end

    test "merges new event messages into empty state", %{state: state} do
      event1 = ConsumersFactory.consumer_event()
      event2 = ConsumersFactory.consumer_event()
      messages = Map.new([event1, event2], &{&1.record_pks, &1})

      updated_state = State.put_messages(state, messages)

      assert map_size(updated_state.messages) == 2
      assert Map.has_key?(updated_state.messages, event1.record_pks)
      assert Map.has_key?(updated_state.messages, event2.record_pks)
    end

    test "merges new event messages with existing messages", %{state: state} do
      # Add initial message
      event1 = ConsumersFactory.consumer_event()
      initial_messages = Map.new([event1], &{&1.record_pks, &1})
      state = %{state | messages: initial_messages}

      # Add new message
      event2 = ConsumersFactory.consumer_event()
      new_messages = Map.new([event2], &{&1.record_pks, &1})

      updated_state = State.put_messages(state, new_messages)

      assert map_size(updated_state.messages) == 2
      assert Map.has_key?(updated_state.messages, event1.record_pks)
      assert Map.has_key?(updated_state.messages, event2.record_pks)
    end
  end

  describe "put_messages/2 with :record messages" do
    setup do
      consumer = ConsumersFactory.sink_consumer(message_kind: :record)

      state = %State{
        consumer: consumer,
        messages: %{},
        flush_interval: 1000,
        flush_batch_size: 100
      }

      {:ok, %{state: state}}
    end

    test "preserves delivery state for existing records", %{state: state} do
      # Create initial record in delivered state
      initial_record =
        ConsumersFactory.consumer_record(
          state: :delivered,
          deliver_count: 2,
          last_delivered_at: DateTime.utc_now(),
          not_visible_until: DateTime.add(DateTime.utc_now(), 30, :second)
        )

      state = %{state | messages: Map.new([{initial_record.record_pks, initial_record}])}

      # Create updated version of same record
      updated_record = %{
        initial_record
        | # This should be preserved as :pending_redelivery
          state: :available,
          # This should be preserved from original
          deliver_count: 0,
          # This should be preserved from original
          last_delivered_at: nil
      }

      updated_state = State.put_messages(state, Map.new([{updated_record.record_pks, updated_record}]))

      result_record = updated_state.messages[initial_record.record_pks]
      assert result_record.state == :pending_redelivery
      assert result_record.deliver_count == initial_record.deliver_count
      assert result_record.last_delivered_at == initial_record.last_delivered_at
      assert result_record.not_visible_until == initial_record.not_visible_until
      # Should be marked dirty for next flush
      assert result_record.dirty == true
    end
  end

  describe "deliverable_messages/2" do
    setup do
      consumer = ConsumersFactory.sink_consumer(message_kind: :record)

      state = %State{
        consumer: consumer,
        messages: %{},
        flush_interval: 1000,
        flush_batch_size: 100
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
        flush_batch_size: 100
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
      state = %{state | messages: Map.new(messages, &{&1.record_pks, &1})}

      now = DateTime.utc_now()
      {updated_state, delivered_messages} = State.deliver_messages(state, messages)

      # Check first message
      delivered1 = updated_state.messages[msg1.record_pks]
      assert delivered1.deliver_count == 1
      assert DateTime.compare(delivered1.last_delivered_at, now) in [:eq, :gt]
      assert DateTime.compare(delivered1.not_visible_until, not_visible_until) in [:eq, :gt]
      assert delivered1.state == :delivered
      assert delivered1.dirty == true

      # Check second message
      delivered2 = updated_state.messages[msg2.record_pks]
      assert delivered2.deliver_count == 2
      assert DateTime.compare(delivered2.last_delivered_at, now) in [:eq, :gt]
      assert DateTime.compare(delivered2.not_visible_until, not_visible_until) in [:eq, :gt]
      assert delivered2.state == :delivered
      assert delivered2.dirty == true

      # Verify returned messages match state
      assert delivered_messages == [delivered1, delivered2]
    end
  end

  describe "update_messages/2" do
    setup do
      consumer = ConsumersFactory.sink_consumer(message_kind: :record)

      state = %State{
        consumer: consumer,
        messages: %{},
        flush_interval: 1000,
        flush_batch_size: 100
      }

      {:ok, %{state: state}}
    end

    test "merges updated messages into state", %{state: state} do
      # Create initial messages
      msg1 = ConsumersFactory.consumer_record()
      msg2 = ConsumersFactory.consumer_record()
      initial_messages = Map.new([msg1, msg2], &{&1.record_pks, &1})
      state = %{state | messages: initial_messages}

      # Update one existing message and add one new message
      updated_msg1 = %{msg1 | state: :delivered}
      msg3 = ConsumersFactory.consumer_record()
      updates = [updated_msg1, msg3]

      updated_state = State.update_messages(state, updates)

      assert map_size(updated_state.messages) == 3
      assert updated_state.messages[msg1.record_pks].state == :delivered
      assert updated_state.messages[msg2.record_pks] == msg2
      assert Map.has_key?(updated_state.messages, msg3.record_pks)
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
        flush_batch_size: 3
      }

      {:ok, %{state: state}}
    end

    test "prioritizes never-flushed messages", %{state: state} do
      never_flushed1 = ConsumersFactory.consumer_record(flushed_at: nil, dirty: true)
      never_flushed2 = ConsumersFactory.consumer_record(flushed_at: nil, dirty: true)

      previously_flushed =
        ConsumersFactory.consumer_record(
          flushed_at: DateTime.add(DateTime.utc_now(), -60, :second),
          dirty: true
        )

      messages = Map.new([never_flushed1, never_flushed2, previously_flushed], &{&1.record_pks, &1})
      state = %{state | messages: messages}

      to_flush = State.messages_to_flush(state)

      assert length(to_flush) == 3
      # Never flushed messages should come first
      assert to_flush |> Enum.take(2) |> Enum.all?(&is_nil(&1.flushed_at))
    end

    test "respects flush_batch_size limit", %{state: state} do
      messages =
        for _i <- 1..5 do
          ConsumersFactory.consumer_record(flushed_at: nil, dirty: true)
        end

      state = %{state | messages: Map.new(messages, &{&1.record_pks, &1})}

      to_flush = State.messages_to_flush(state)

      # matches flush_batch_size
      assert length(to_flush) == 3
    end

    test "sorts previously flushed messages by flushed_at", %{state: state} do
      now = DateTime.utc_now()
      msg1 = ConsumersFactory.consumer_record(flushed_at: DateTime.add(now, -30, :second), dirty: true)
      msg2 = ConsumersFactory.consumer_record(flushed_at: DateTime.add(now, -60, :second), dirty: true)
      msg3 = ConsumersFactory.consumer_record(flushed_at: DateTime.add(now, -15, :second), dirty: true)

      messages = Map.new([msg1, msg2, msg3], &{&1.record_pks, &1})
      state = %{state | messages: messages}

      to_flush = State.messages_to_flush(state)

      flushed_ats = Enum.map(to_flush, & &1.flushed_at)
      assert flushed_ats == Enum.sort(flushed_ats, DateTime)
    end

    test "excludes non-dirty messages", %{state: state} do
      dirty_msg = ConsumersFactory.consumer_record(flushed_at: nil, dirty: true)
      clean_msg = ConsumersFactory.consumer_record(flushed_at: nil, dirty: false)

      messages = Map.new([dirty_msg, clean_msg], &{&1.record_pks, &1})
      state = %{state | messages: messages}

      to_flush = State.messages_to_flush(state)

      assert length(to_flush) == 1
      assert hd(to_flush).record_pks == dirty_msg.record_pks
    end
  end
end

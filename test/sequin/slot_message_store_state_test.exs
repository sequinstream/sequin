defmodule Sequin.DatabasesRuntime.SlotMessageStoreStateTest do
  use Sequin.DataCase, async: true

  alias Sequin.Consumers
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.DatabasesRuntime.SlotMessageStore.State
  alias Sequin.Factory.ConsumersFactory

  describe "put_messages/2" do
    setup do
      consumer = ConsumersFactory.insert_sink_consumer!()

      state = %State{
        consumer: consumer,
        flush_interval: 1000,
        flush_batch_size: 100,
        flush_wait_ms: 1
      }

      {:ok, %{state: state}}
    end

    test "merges new messages into empty state", %{state: state} do
      message1 = ConsumersFactory.consumer_message(message_kind: state.consumer.message_kind)
      message2 = ConsumersFactory.consumer_message(message_kind: state.consumer.message_kind)

      state = State.put_messages(state, [message1, message2])

      messages = State.peek_messages(state, 1000)
      assert_same_messages(messages, [message1, message2])
    end

    test "merges new messages with existing messages", %{state: state} do
      # Add initial message
      message1 = ConsumersFactory.consumer_message(message_kind: state.consumer.message_kind)
      state = State.put_messages(state, [message1])

      # Add new message
      message2 = ConsumersFactory.consumer_message(message_kind: state.consumer.message_kind)
      updated_state = State.put_messages(state, [message2])

      assert_same_messages(State.peek_messages(updated_state, 1000), [message1, message2])
    end
  end

  describe "deliver_messages/2" do
    setup do
      consumer = ConsumersFactory.insert_sink_consumer!()

      state = %State{
        consumer: consumer,
        flush_interval: 1000,
        flush_batch_size: 100,
        flush_wait_ms: 1
      }

      {:ok, %{state: state}}
    end

    test "excludes messages that are not yet visible", %{state: state} do
      future_time = DateTime.add(DateTime.utc_now(), 30, :second)

      visible_msg =
        ConsumersFactory.consumer_message(
          seq: 1,
          not_visible_until: nil
        )

      invisible_msg =
        ConsumersFactory.consumer_message(
          seq: 2,
          not_visible_until: future_time
        )

      messages = Map.new([visible_msg, invisible_msg], &{&1.ack_id, &1})
      state = %{state | messages: messages}

      {_state, delivered} = State.deliver_messages(state, 10)

      assert length(delivered) == 1
      assert hd(delivered).seq == visible_msg.seq
    end

    test "respects message group visibility", %{state: state} do
      group_id = "group1"
      future_time = DateTime.add(DateTime.utc_now(), 30, :second)

      # Create three messages in same group, one pending
      msg1 = ConsumersFactory.consumer_message(seq: 1, group_id: group_id)
      msg2 = ConsumersFactory.consumer_message(seq: 2, group_id: group_id, not_visible_until: future_time)
      msg3 = ConsumersFactory.consumer_message(seq: 3, group_id: group_id)

      messages = Map.new([msg1, msg2, msg3], &{&1.ack_id, &1})
      state = %{state | messages: messages}

      {_state, delivered} = State.deliver_messages(state, 10)

      assert Enum.empty?(delivered),
             "Should not deliver any messages when group has pending message"
    end

    test "updates message delivery state and metadata", %{state: state} do
      now = DateTime.utc_now()
      not_visible_until = DateTime.add(now, state.consumer.ack_wait_ms, :millisecond)
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
      {updated_state, delivered_messages} = State.deliver_messages(state, 2)

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

  describe "disk_overflow_mode?" do
    setup do
      consumer = ConsumersFactory.insert_sink_consumer!()

      state = %State{
        consumer: consumer,
        consumer_id: consumer.id,
        flush_batch_size: 2,
        disk_overflow_mode?: false,
        max_messages_in_memory: 3
      }

      {:ok, %{state: state}}
    end

    test "init_from_postgres/1 enters disk_overflow_mode? when messages on disk exceeds max_messages_in_memory", %{
      state: state
    } do
      message_count = state.max_messages_in_memory + 10

      messages =
        for i <- 1..message_count do
          ConsumersFactory.insert_consumer_message!(
            seq: i,
            consumer_id: state.consumer.id,
            message_kind: state.consumer.message_kind
          )
        end

      messages = Enum.sort_by(messages, & &1.seq)

      assert {:ok, state} = State.init_from_postgres(state)
      assert state.disk_overflow_mode?

      # Loads the lowest seq messages into memory
      expected_loaded = Enum.take(messages, state.max_messages_in_memory)
      assert_same_messages(State.peek_messages(state, 1000), expected_loaded)
    end

    test "put_messages/2 flushes incoming message to disk when disk_overflow_mode? is true", %{state: state} do
      state = %{state | disk_overflow_mode?: true}

      message =
        ConsumersFactory.consumer_message(consumer_id: state.consumer.id, message_kind: state.consumer.message_kind)

      state = State.put_messages(state, [message])

      assert State.peek_messages(state, 1000) == []
      assert_same_messages(list_db_messages(state.consumer), [message])
    end

    test "put_messages/2 enters disk_overflow_mode? when max_messages_in_memory is reached", %{state: state} do
      message_count = state.max_messages_in_memory + 10

      messages =
        for _ <- 1..message_count do
          ConsumersFactory.consumer_message(consumer_id: state.consumer.id, message_kind: state.consumer.message_kind)
        end

      state = State.put_messages(state, messages)
      assert state.disk_overflow_mode?

      assert State.peek_messages(state, 1000) == []
      assert_same_messages(list_db_messages(state.consumer), messages)
    end

    test "put_table_reader_batch/3 flushes incoming message to disk when disk_overflow_mode? is true", %{state: state} do
      state = %{state | disk_overflow_mode?: true}

      message =
        ConsumersFactory.consumer_message(consumer_id: state.consumer.id, message_kind: state.consumer.message_kind)

      state = State.put_table_reader_batch(state, [message], "batch-1")
      assert state.table_reader_batch_id == "batch-1"

      assert State.peek_messages(state, 1000) == []
      assert_same_messages(list_db_messages(state.consumer), [message])

      assert State.batch_progress(state, "batch-1") == {:ok, :completed}
    end

    test "put_table_reader_batch/3 enters disk_overflow_mode? when max_messages_in_memory is reached", %{state: state} do
      message_count = state.max_messages_in_memory + 10

      messages =
        for _ <- 1..message_count do
          ConsumersFactory.consumer_message(consumer_id: state.consumer.id, message_kind: state.consumer.message_kind)
        end

      state = State.put_table_reader_batch(state, messages, "batch-1")
      assert state.disk_overflow_mode?
      assert state.table_reader_batch_id == "batch-1"

      assert State.peek_messages(state, 1000) == []
      assert_same_messages(list_db_messages(state.consumer), messages)

      assert State.batch_progress(state, "batch-1") == {:ok, :completed}
    end

    test "deliver_messages/2 loads messages with lowest seqs from db when messages in memory is low", %{state: state} do
      state = %{state | disk_overflow_mode?: true}
      message_count = state.max_messages_in_memory * 3

      messages =
        for i <- 1..message_count do
          ConsumersFactory.insert_consumer_message!(
            seq: i,
            consumer_id: state.consumer.id,
            message_kind: state.consumer.message_kind
          )
        end

      messages = Enum.sort_by(messages, & &1.seq)
      assert State.peek_messages(state, 1000) == []

      {state, delivered} = State.deliver_messages(state, 2)
      expected_delivered = Enum.take(messages, 2)
      assert_same_messages(delivered, expected_delivered)

      # Messages were loaded into memory
      expected_loaded = Enum.take(messages, state.max_messages_in_memory)
      assert_same_messages(State.peek_messages(state, 1000), expected_loaded)
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

  defp assert_same_messages(messages1, messages2) do
    assert_lists_equal(
      messages1,
      messages2,
      &assert_maps_equal(&1, &2, [
        :consumer_id,
        :commit_lsn,
        :seq,
        :record_pks,
        :group_id,
        :table_oid,
        :data
      ])
    )
  end

  defp list_db_messages(%SinkConsumer{id: consumer_id, message_kind: :event}) do
    Consumers.list_consumer_events_for_consumer(consumer_id)
  end

  defp list_db_messages(%SinkConsumer{id: consumer_id, message_kind: :record}) do
    Consumers.list_consumer_records_for_consumer(consumer_id)
  end
end

defmodule Sequin.Runtime.SlotMessageStoreStateTest do
  use Sequin.Case, async: true

  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Error
  alias Sequin.Factory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Multiset
  alias Sequin.Runtime.SlotMessageStore.State
  alias Sequin.Size

  setup do
    state = %State{
      consumer: %SinkConsumer{seq: Factory.unique_integer()},
      partition: 0,
      max_time_since_delivered_ms: 1000
    }

    State.setup_ets(state)
    {:ok, %{state: state}}
  end

  describe "put_messages/2 with messages" do
    test "merges new messages into empty state", %{state: state} do
      msg1 = ConsumersFactory.consumer_message()
      msg2 = ConsumersFactory.consumer_message()
      {:ok, state} = State.put_messages(state, [msg1, msg2])

      assert map_size(state.messages) == 2
      assert_message_in_state(msg1, state)
      assert_message_in_state(msg2, state)
    end

    test "merges new messages with existing messages", %{state: state} do
      # Add initial message
      msg1 = ConsumersFactory.consumer_message()
      {:ok, state} = State.put_messages(state, [msg1])

      # Add new message
      msg2 = ConsumersFactory.consumer_message()
      {:ok, updated_state} = State.put_messages(state, [msg2])

      assert map_size(updated_state.messages) == 2
      assert_message_in_state(msg1, updated_state)
      assert_message_in_state(msg2, updated_state)
    end

    test "updates payload_size_bytes in state", %{state: state} do
      msg1 = ConsumersFactory.consumer_message(payload_size_bytes: 100)

      msg2 = ConsumersFactory.consumer_message(payload_size_bytes: 200)

      {:ok, state} = State.put_messages(state, [msg1, msg2])

      assert state.payload_size_bytes == 300

      msg3 = ConsumersFactory.consumer_message(payload_size_bytes: 100)

      {:ok, updated_state} = State.put_messages(state, [msg3])

      assert updated_state.payload_size_bytes == 400
    end

    test "returns error when exceeding setting_max_accumulated_payload_bytes", %{state: state} do
      state = %{state | max_memory_bytes: Size.mb(100)}

      msg1 = ConsumersFactory.consumer_message(payload_size_bytes: Size.mb(100))

      msg2 = ConsumersFactory.consumer_message(payload_size_bytes: Size.mb(100))

      assert {:error, %Error.InvariantError{}} = State.put_messages(state, [msg1, msg2])
    end
  end

  describe "validate_put_messages/2" do
    test "returns payload size if under memory limit", %{state: state} do
      msg1 = ConsumersFactory.consumer_message(payload_size_bytes: 100)
      msg2 = ConsumersFactory.consumer_message(payload_size_bytes: 200)

      {:ok, payload_size} = State.validate_put_messages(state, [msg1, msg2])

      assert payload_size == 300
    end

    test "returns error when exceeding setting_max_accumulated_payload_bytes", %{state: state} do
      state = %{state | max_memory_bytes: Size.mb(100)}

      msg1 = ConsumersFactory.consumer_message(payload_size_bytes: Size.mb(100))
      msg2 = ConsumersFactory.consumer_message(payload_size_bytes: Size.mb(100))

      assert {:error, %Error.InvariantError{}} = State.validate_put_messages(state, [msg1, msg2])
    end
  end

  describe "put_persisted_messages/2" do
    test "updates persisted_message_groups", %{state: state} do
      table_oid = 1
      msg1 = ConsumersFactory.consumer_message(table_oid: table_oid, group_id: "group1")
      msg2 = ConsumersFactory.consumer_message(table_oid: table_oid, group_id: "group1")

      state = State.put_persisted_messages(state, [msg1, msg2])

      assert_message_in_state(msg1, state)
      assert_message_in_state(msg2, state)

      # Verify persisted_message_groups tracking
      assert Multiset.value_member?(
               state.persisted_message_groups,
               {msg1.table_oid, "group1"},
               {msg1.commit_lsn, msg1.commit_idx}
             )

      assert Multiset.value_member?(
               state.persisted_message_groups,
               {msg2.table_oid, "group1"},
               {msg2.commit_lsn, msg2.commit_idx}
             )

      assert Multiset.count(state.persisted_message_groups, {msg1.table_oid, "group1"}) == 2
    end

    test "preserves existing messages when adding new persisted messages", %{state: state} do
      # Add initial message
      msg1 = ConsumersFactory.consumer_message(group_id: "group1")
      {:ok, state} = State.put_messages(state, [msg1])

      # Add persisted message
      msg2 = ConsumersFactory.consumer_message(group_id: "group2")
      state = State.put_persisted_messages(state, [msg2])

      # Verify both messages exist
      assert map_size(state.messages) == 2
      assert_message_in_state(msg1, state)
      assert_message_in_state(msg2, state)

      # Only persisted message should be in persisted_message_groups
      assert Multiset.count(state.persisted_message_groups, {msg1.table_oid, "group1"}) == 0
      assert Multiset.count(state.persisted_message_groups, {msg2.table_oid, "group2"}) == 1
    end

    test "handles multiple groups in persisted_message_groups", %{state: state} do
      table_oid = 1
      msg1 = ConsumersFactory.consumer_message(table_oid: table_oid, group_id: "group1")
      msg2 = ConsumersFactory.consumer_message(table_oid: table_oid, group_id: "group2")
      msg3 = ConsumersFactory.consumer_message(table_oid: table_oid, group_id: "group1")

      state = State.put_persisted_messages(state, [msg1, msg2, msg3])

      assert Multiset.count(state.persisted_message_groups, {msg1.table_oid, "group1"}) == 2
      assert Multiset.count(state.persisted_message_groups, {msg2.table_oid, "group2"}) == 1
    end
  end

  describe "put_table_reader_batch/3" do
    test "sets messages and table_reader_batch_id", %{state: state} do
      batch_id = "test-batch-123"

      msg1 = ConsumersFactory.consumer_message()
      msg2 = ConsumersFactory.consumer_message()

      {:ok, state} = State.put_table_reader_batch(state, [msg1, msg2], batch_id)

      assert map_size(state.messages) == 2
      assert state.table_reader_batch_id == batch_id

      # Verify all messages have the batch_id set
      Enum.each(Map.values(state.messages), fn msg ->
        assert msg.table_reader_batch_id == batch_id
      end)
    end
  end

  describe "pop_messages/2" do
    test "when a message is popped, its payload size bytes are removed from state", %{state: state} do
      msg1 = ConsumersFactory.consumer_message(commit_lsn: 1, commit_idx: 0, payload_size_bytes: 100)
      msg2 = ConsumersFactory.consumer_message(commit_lsn: 1, commit_idx: 1, payload_size_bytes: 200)

      {:ok, state} = State.put_messages(state, [msg1, msg2])

      assert {[_msg1], state} = State.pop_messages(state, [{msg1.commit_lsn, msg1.commit_idx}])

      assert state.payload_size_bytes == 200
    end

    test "when a produced message is popped, its group_id is removed from produced_message_groups", %{state: state} do
      msg = ConsumersFactory.consumer_message(group_id: "group1")
      {:ok, state} = State.put_messages(state, [msg])

      # Produce the message to add it to produced_message_groups
      assert {[produced_msg], state} = State.produce_messages(state, 1)
      assert Multiset.count(state.produced_message_groups, {msg.table_oid, "group1"}) == 1

      # Pop the message and verify it's removed from produced_message_groups
      assert {[_msg], state} = State.pop_messages(state, [{produced_msg.commit_lsn, produced_msg.commit_idx}])
      assert Multiset.count(state.produced_message_groups, {msg.table_oid, "group1"}) == 0
    end

    test "when a persisted message is popped, its group_id is removed from persisted_message_groups", %{state: state} do
      msg = ConsumersFactory.consumer_message(group_id: "group1")
      state = State.put_persisted_messages(state, [msg])

      # Verify message is in persisted_message_groups
      assert Multiset.count(state.persisted_message_groups, {msg.table_oid, "group1"}) == 1

      # Pop the message and verify it's removed from persisted_message_groups
      assert {[_msg], state} = State.pop_messages(state, [{msg.commit_lsn, msg.commit_idx}])
      assert Multiset.count(state.persisted_message_groups, {msg.table_oid, "group1"}) == 0
    end
  end

  describe "end to end test with put and pop" do
    test "popped messages are removed from state", %{state: state} do
      now = DateTime.utc_now()
      expect_utc_now(6, fn -> now end)

      msg1 = ConsumersFactory.consumer_message(commit_lsn: 1, commit_idx: 0)
      msg2 = ConsumersFactory.consumer_message(commit_lsn: 1, commit_idx: 1)
      msg3 = ConsumersFactory.consumer_message(commit_lsn: 1, commit_idx: 2)

      {:ok, state} = State.put_messages(state, [msg1, msg2, msg3])
      assert {[produced1, produced2], state} = State.produce_messages(state, 2)

      assert {[_msg1, _msg2], state} =
               State.pop_messages(state, [
                 {produced1.commit_lsn, produced1.commit_idx},
                 {produced2.commit_lsn, produced2.commit_idx}
               ])

      assert {[produced3], state} = State.produce_messages(state, 1)
      assert {[_msg3], state} = State.pop_messages(state, [{produced3.commit_lsn, produced3.commit_idx}])

      assert {[], state} = State.produce_messages(state, 1)
      assert state.payload_size_bytes == 0
    end

    test "when a message is popped, messages for that group_id are available for delivery", %{state: state} do
      table_oid = 1
      group_id = "group1"
      msg1 = ConsumersFactory.consumer_message(table_oid: table_oid, commit_lsn: 1, commit_idx: 0, group_id: group_id)
      msg2 = ConsumersFactory.consumer_message(table_oid: table_oid, commit_lsn: 1, commit_idx: 1, group_id: group_id)

      {:ok, state} = State.put_messages(state, [msg1, msg2])
      assert {[produced1], state} = State.produce_messages(state, 1)
      assert {[], state} = State.produce_messages(state, 1)

      assert {[_msg1], state} = State.pop_messages(state, [{produced1.commit_lsn, produced1.commit_idx}])

      assert {[produced2], _state} = State.produce_messages(state, 1)

      assert produced1.commit_idx == msg1.commit_idx
      assert produced2.commit_idx == msg2.commit_idx
    end

    test "mixed order of operations", %{state: state} do
      msg1 = ConsumersFactory.consumer_message(commit_lsn: 1, commit_idx: 0)
      msg2 = ConsumersFactory.consumer_message(commit_lsn: 1, commit_idx: 1)
      msg3 = ConsumersFactory.consumer_message(commit_lsn: 2, commit_idx: 0)
      msg4 = ConsumersFactory.consumer_message(commit_lsn: 3, commit_idx: 0)

      {:ok, state} = State.put_messages(state, [msg1, msg2])
      assert {[produced1, produced2], state} = State.produce_messages(state, 2)
      assert {[], state} = State.produce_messages(state, 2)

      {:ok, state} = State.put_messages(state, [msg3, msg4])

      assert {[acked1], state} = State.pop_messages(state, [{produced1.commit_lsn, produced1.commit_idx}])

      assert {[], state} = State.pop_messages(state, [Factory.uuid()])

      assert {[produced3, produced4], state} = State.produce_messages(state, 2)

      assert {acked_34, state} =
               State.pop_messages(state, [
                 {produced3.commit_lsn, produced3.commit_idx},
                 {produced4.commit_lsn, produced4.commit_idx}
               ])

      assert {[], state} = State.produce_messages(state, 1)
      assert {[acked2], _state} = State.pop_messages(state, [{produced2.commit_lsn, produced2.commit_idx}])

      assert_cursor_tuple_matches(msg1, produced1)
      assert_cursor_tuple_matches(msg1, acked1)

      assert_cursor_tuple_matches(msg2, produced2)
      assert_cursor_tuple_matches(msg2, acked2)

      assert_cursor_tuple_matches(msg3, produced3)
      assert_cursor_tuple_matches(msg4, produced4)

      # These acked messages can arrive in any order
      assert length(acked_34) == 2
      acked3 = Enum.find(acked_34, fn msg -> produced3.ack_id == msg.ack_id end)
      acked4 = Enum.find(acked_34, fn msg -> produced4.ack_id == msg.ack_id end)

      assert_cursor_tuple_matches(msg3, acked3)
      assert_cursor_tuple_matches(msg4, acked4)
    end
  end

  describe "message_group_persisted?/2" do
    test "returns true if the message group is persisted", %{state: state} do
      # Add a persisted message to establish a blocked group
      table_oid = 1
      persisted_msg = ConsumersFactory.consumer_message(table_oid: table_oid, group_id: "group1")
      other_msg = ConsumersFactory.consumer_message(table_oid: table_oid, group_id: "group2")
      state = State.put_persisted_messages(state, [persisted_msg])
      {:ok, state} = State.put_messages(state, [other_msg])

      assert State.message_group_persisted?(state, table_oid, "group1")
      refute State.message_group_persisted?(state, table_oid, "group2")
    end
  end

  describe "nack_produced_messages/1" do
    test "removes all messages from produced_message_groups", %{state: state} do
      msg1 = ConsumersFactory.consumer_message()
      msg2 = ConsumersFactory.consumer_message()

      {:ok, state} = State.put_messages(state, [msg1, msg2])
      assert {[_msg1, _msg2], state} = State.produce_messages(state, 2)
      assert {[], state} = State.produce_messages(state, 2)

      assert {state, 2} = State.nack_produced_messages(state)

      assert {[_msg1, _msg2], _state} = State.produce_messages(state, 2)
    end
  end

  describe "min_unpersisted_wal_cursor/1" do
    test "returns nil when there are no messages", %{state: state} do
      assert State.min_unpersisted_wal_cursor(state) == nil
    end

    test "returns lowest wal_cursor from messages", %{state: state} do
      messages = [
        ConsumersFactory.consumer_message(commit_lsn: 100, commit_idx: 4),
        ConsumersFactory.consumer_message(commit_lsn: 200, commit_idx: 1)
      ]

      {:ok, state} = State.put_messages(state, messages)

      assert State.min_unpersisted_wal_cursor(state) == %{commit_lsn: 100, commit_idx: 4}
    end

    test "returns lowest wal_cursor from messages with the same commit_lsn", %{state: state} do
      messages = [
        ConsumersFactory.consumer_message(commit_lsn: 100, commit_idx: 4),
        ConsumersFactory.consumer_message(commit_lsn: 100, commit_idx: 1)
      ]

      {:ok, state} = State.put_messages(state, messages)

      assert State.min_unpersisted_wal_cursor(state) == %{commit_lsn: 100, commit_idx: 1}
    end

    test "returns lowest wal_cursor including produced messages", %{state: state} do
      messages = [
        ConsumersFactory.consumer_message(commit_lsn: 100, commit_idx: 4),
        ConsumersFactory.consumer_message(commit_lsn: 200, commit_idx: 1)
      ]

      {:ok, state} = State.put_messages(state, messages)
      assert {[_msg1, _msg2], state} = State.produce_messages(state, 2)

      assert State.min_unpersisted_wal_cursor(state) == %{commit_lsn: 100, commit_idx: 4}
    end

    test "ignores persisted messages when calculating min_unpersisted_wal_cursor", %{state: state} do
      # Add a persisted message with lower LSN/idx
      persisted_msg = ConsumersFactory.consumer_message(commit_lsn: 50, commit_idx: 1)
      state = State.put_persisted_messages(state, [persisted_msg])

      # Add regular messages with higher LSN/idx
      messages = [
        ConsumersFactory.consumer_message(commit_lsn: 100, commit_idx: 4),
        ConsumersFactory.consumer_message(commit_lsn: 200, commit_idx: 1)
      ]

      {:ok, state} = State.put_messages(state, messages)

      # Should return lowest cursor from regular messages, ignoring persisted message
      assert State.min_unpersisted_wal_cursor(state) == %{commit_lsn: 100, commit_idx: 4}
    end
  end

  describe "produce_messages/2" do
    test "delivers messages, excluding messages that are already produced", %{state: state} do
      now = DateTime.utc_now()
      expect_utc_now(6, fn -> now end)

      msg1 = ConsumersFactory.consumer_message(commit_lsn: 1, commit_idx: 0)
      msg2 = ConsumersFactory.consumer_message(commit_lsn: 1, commit_idx: 1)

      {:ok, state} = State.put_messages(state, [msg1, msg2])
      assert {[produced1], state} = State.produce_messages(state, 1)
      assert {[produced2], state} = State.produce_messages(state, 1)

      assert produced1.commit_idx == msg1.commit_idx
      assert produced2.commit_idx == msg2.commit_idx

      assert produced1.last_delivered_at == now

      assert {[], _state} = State.produce_messages(state, 1)
    end

    test "respects group_id for produced messages", %{state: state} do
      table_oid = 1
      group_id = "group1"
      msg1 = ConsumersFactory.consumer_message(table_oid: table_oid, commit_lsn: 1, commit_idx: 0, group_id: group_id)
      msg2 = ConsumersFactory.consumer_message(table_oid: table_oid, commit_lsn: 1, commit_idx: 1, group_id: group_id)

      {:ok, state} = State.put_messages(state, [msg1, msg2])
      assert {[produced1], state} = State.produce_messages(state, 1)
      assert {[], _state} = State.produce_messages(state, 1)

      assert produced1.commit_idx == msg1.commit_idx
    end

    test "only returns one message per group_id regardless of requested count", %{state: state} do
      # Create 3 messages, where 2 share the same group_id
      table_oid = 1
      msg1 = ConsumersFactory.consumer_message(table_oid: table_oid, commit_lsn: 1, commit_idx: 0, group_id: "group1")
      msg2 = ConsumersFactory.consumer_message(table_oid: table_oid, commit_lsn: 1, commit_idx: 1, group_id: "group1")
      msg3 = ConsumersFactory.consumer_message(table_oid: table_oid, commit_lsn: 1, commit_idx: 2, group_id: "group2")

      {:ok, state} = State.put_messages(state, [msg1, msg2, msg3])

      # Request more messages than exist
      assert {produced_messages, _state} = State.produce_messages(state, 10)

      # Should only get 2 messages back (one per unique group_id)
      assert length(produced_messages) == 2

      # Verify we got one message from each group
      group_ids = produced_messages |> Enum.map(& &1.group_id) |> Enum.sort()
      assert group_ids == ["group1", "group2"]

      # Verify we got the first message from group1 (lower commit_idx)
      group1_msg = Enum.find(produced_messages, &(&1.group_id == "group1"))
      assert group1_msg.commit_idx == 0
    end

    test "delivers multiple messages without group_id at the same time", %{state: state} do
      # This test ensures backwards compatibility with messages that don't have a group_id
      # Create messages with nil group_ids
      msg1 = ConsumersFactory.consumer_message(group_id: nil, commit_lsn: 1)
      msg2 = ConsumersFactory.consumer_message(group_id: nil, commit_lsn: 2)
      msg3 = ConsumersFactory.consumer_message(group_id: nil, commit_lsn: 3)
      msg4 = ConsumersFactory.consumer_message(group_id: "group1", commit_lsn: 4)
      _ = ConsumersFactory.consumer_message(group_id: "group1", commit_lsn: 5)

      {:ok, state} = State.put_messages(state, [msg1, msg2, msg3, msg4])

      # Should be able to produce all messages with nil group_id, and only one message with a group_id "group1"
      assert {produced, _state} = State.produce_messages(state, 100)
      assert length(produced) == 4
      assert Enum.map(produced, & &1.commit_lsn) == [1, 2, 3, 4]
    end
  end

  describe "peek_messages/2" do
    test "returns messages in commit_lsn, commit_idx order", %{state: state} do
      messages =
        Enum.shuffle([
          ConsumersFactory.consumer_message(commit_lsn: 1, commit_idx: 0),
          ConsumersFactory.consumer_message(commit_lsn: 1, commit_idx: 1),
          ConsumersFactory.consumer_message(commit_lsn: 1, commit_idx: 2),
          ConsumersFactory.consumer_message(commit_lsn: 2, commit_idx: 0),
          ConsumersFactory.consumer_message(commit_lsn: 3, commit_idx: 0)
        ])

      {:ok, state} = State.put_messages(state, messages)
      assert {_msgs, state} = State.produce_messages(state, Enum.random(1..length(messages)))

      assert peeked_messages = State.peek_messages(state, 3)
      assert Enum.map(peeked_messages, & &1.commit_lsn) == [1, 1, 1]
      assert Enum.map(peeked_messages, & &1.commit_idx) == [0, 1, 2]
    end
  end

  describe "nack_stale_produced_messages/1" do
    test "removes stale messages from produced_message_groups", %{state: state} do
      # Set up test time and ack_wait_ms
      now = DateTime.utc_now()
      past = DateTime.add(now, -2000, :millisecond)
      state = put_in(state.consumer.ack_wait_ms, 1000)

      # Create messages with different delivery times
      msg1 = ConsumersFactory.consumer_message(group_id: "group1")
      msg2 = ConsumersFactory.consumer_message(group_id: "group2")

      {:ok, state} = State.put_messages(state, [msg1, msg2])

      # Produce messages and simulate different delivery times
      expect_utc_now(2, fn -> past end)
      assert {[produced1], state} = State.produce_messages(state, 1)

      stub_utc_now(fn -> now end)
      assert {[_produced2], state} = State.produce_messages(state, 1)

      assert {[], state} = State.produce_messages(state, 2)

      # Run nack_stale_produced_messages
      state = State.nack_stale_produced_messages(state)

      # Verify stale message can be produced again
      assert {[reproduced], _state} = State.produce_messages(state, 2)
      assert reproduced.id == produced1.id
    end

    test "handles messages with no last_delivered_at", %{state: state} do
      msg = ConsumersFactory.consumer_message()
      {:ok, state} = State.put_messages(state, [msg])

      # Message exists but hasn't been produced yet
      state = State.nack_stale_produced_messages(state)

      # Should still be able to produce the message
      assert {[produced], _state} = State.produce_messages(state, 1)
      assert produced.id == msg.id
    end

    test "does nothing when no messages are stale", %{state: state} do
      state = put_in(state.consumer.ack_wait_ms, 1000)

      msg = ConsumersFactory.consumer_message()
      {:ok, state} = State.put_messages(state, [msg])

      # Produce message with current timestamp
      assert {[_produced], state} = State.produce_messages(state, 1)

      # Run nack_stale_produced_messages with same timestamp
      state = State.nack_stale_produced_messages(state)

      # Message should still be in produced_message_groups
      assert Multiset.count(state.produced_message_groups, {msg.table_oid, msg.group_id}) == 1

      # Message should not be available for production
      assert {[], _state} = State.produce_messages(state, 1)
    end
  end

  describe "reset_message_visibilities/2" do
    test "allows redelivery of messages after resetting visibility", %{state: state} do
      # Set up test messages
      msg1 = ConsumersFactory.consumer_message()
      msg2 = ConsumersFactory.consumer_message()
      {:ok, state} = State.put_messages(state, [msg1, msg2])

      # Deliver messages
      assert {[produced1, produced2], state} = State.produce_messages(state, 2)
      # Verify no more messages available for delivery
      assert {[], state} = State.produce_messages(state, 2)

      # Reset visibility
      {state, reset_messages} = State.reset_message_visibilities(state, [produced1.ack_id, produced2.ack_id])

      # Verify reset messages
      assert length(reset_messages) == 2
      assert Enum.all?(reset_messages, &(&1.not_visible_until == nil))
      assert Enum.all?(reset_messages, &(&1.id in [msg1.id, msg2.id]))

      # Messages should be available for delivery again
      assert {[redelivered1, redelivered2], _state} = State.produce_messages(state, 2)
      assert_cursor_tuple_matches(produced1, redelivered1)
      assert_cursor_tuple_matches(produced2, redelivered2)
    end

    test "only resets visibility for specified messages", %{state: state} do
      # Set up test messages
      msg1 = ConsumersFactory.consumer_message()
      msg2 = ConsumersFactory.consumer_message()
      msg3 = ConsumersFactory.consumer_message()
      {:ok, state} = State.put_messages(state, [msg1, msg2, msg3])

      # Deliver all messages
      assert {[produced1, produced2, _produced3], state} = State.produce_messages(state, 3)
      assert {[], state} = State.produce_messages(state, 3)

      # Reset visibility only for msg1 and msg2
      {state, reset_messages} = State.reset_message_visibilities(state, [produced1.ack_id, produced2.ack_id])

      # Verify reset messages
      assert length(reset_messages) == 2
      assert Enum.all?(reset_messages, &(&1.not_visible_until == nil))
      assert Enum.all?(reset_messages, &(&1.id in [msg1.id, msg2.id]))

      # Only msg1 and msg2 should be available for redelivery
      assert {[redelivered1, redelivered2], state} = State.produce_messages(state, 3)
      assert_cursor_tuple_matches(produced1, redelivered1)
      assert_cursor_tuple_matches(produced2, redelivered2)

      # No more messages should be available
      assert {[], _state} = State.produce_messages(state, 1)
    end

    test "handles non-existent ack_ids gracefully", %{state: state} do
      # Set up test message
      msg1 = ConsumersFactory.consumer_message()
      {:ok, state} = State.put_messages(state, [msg1])

      # Deliver message
      assert {[produced1], state} = State.produce_messages(state, 1)
      assert {[], state} = State.produce_messages(state, 1)

      # Reset visibility with mix of valid and invalid ack_ids
      {state, reset_messages} = State.reset_message_visibilities(state, [produced1.ack_id, Factory.uuid()])

      # Verify reset messages
      assert length(reset_messages) == 1
      assert hd(reset_messages).id == msg1.id
      assert hd(reset_messages).not_visible_until == nil

      # Should still work for the valid message
      assert {[redelivered1], _state} = State.produce_messages(state, 1)
      assert_cursor_tuple_matches(produced1, redelivered1)
    end
  end

  describe "reset_all_message_visibilities/1" do
    test "handles empty state gracefully", %{state: state} do
      assert State.reset_all_message_visibilities(state)
    end

    test "resets message visibilities for all produced messages", %{state: state} do
      # Set up test messages
      msg1 = ConsumersFactory.consumer_message()
      msg2 = ConsumersFactory.consumer_message()
      {:ok, state} = State.put_messages(state, [msg1, msg2])

      # Deliver messages
      assert {[produced1, produced2], state} = State.produce_messages(state, 2)
      assert {[], state} = State.produce_messages(state, 2)

      # Reset all visibilities
      state = State.reset_all_message_visibilities(state)

      # Should be able to produce all messages again
      assert {[redelivered1, redelivered2], _state} = State.produce_messages(state, 2)
      assert_cursor_tuple_matches(produced1, redelivered1)
      assert_cursor_tuple_matches(produced2, redelivered2)
    end
  end

  describe "unpersisted_table_reader_batch_ids/1" do
    test "returns batch IDs of unpersisted messages from table reader batches", %{state: state} do
      batch_id = "test-batch-123"
      msg1 = ConsumersFactory.consumer_message()
      msg2 = ConsumersFactory.consumer_message()

      # Put messages in batch
      {:ok, state} = State.put_table_reader_batch(state, [msg1, msg2], batch_id)
      assert batch_id in State.unpersisted_table_reader_batch_ids(state)

      # Produce two messages
      {[msg1, msg2], state} = State.produce_messages(state, 2)
      assert batch_id in State.unpersisted_table_reader_batch_ids(state)

      # Pop one message
      {[_msg], state} = State.pop_messages(state, [{msg2.commit_lsn, msg2.commit_idx}])
      assert batch_id in State.unpersisted_table_reader_batch_ids(state)

      # Pop and put persisted the other message
      {[msg1], state} = State.pop_messages(state, [{msg1.commit_lsn, msg1.commit_idx}])
      state = State.put_persisted_messages(state, [msg1])
      assert State.unpersisted_table_reader_batch_ids(state) == []
    end

    test "returns empty list when there are no table reader batch messages", %{state: state} do
      # Add regular messages (not from table reader batch)
      msg1 = ConsumersFactory.consumer_message()
      msg2 = ConsumersFactory.consumer_message()
      {:ok, state} = State.put_messages(state, [msg1, msg2])

      assert State.unpersisted_table_reader_batch_ids(state) == []
    end

    test "returns empty list after all batch messages are removed", %{state: state} do
      batch_id = "test-batch-123"
      msg = ConsumersFactory.consumer_message()

      # Put message in batch
      {:ok, state} = State.put_table_reader_batch(state, [msg], batch_id)
      assert batch_id in State.unpersisted_table_reader_batch_ids(state)

      # Pop the message
      {[_msg], state} = State.pop_messages(state, [{msg.commit_lsn, msg.commit_idx}])
      assert State.unpersisted_table_reader_batch_ids(state) == []
    end

    test "handles multiple batch IDs correctly", %{state: state} do
      batch_id1 = "test-batch-123"
      batch_id2 = "test-batch-456"

      msg1 = ConsumersFactory.consumer_message()
      msg2 = ConsumersFactory.consumer_message()

      # Put messages in different batches
      {:ok, state} = State.put_table_reader_batch(state, [msg1], batch_id1)
      {:ok, state} = State.put_table_reader_batch(state, [msg2], batch_id2)

      batch_ids = State.unpersisted_table_reader_batch_ids(state)
      assert batch_id1 in batch_ids
      assert batch_id2 in batch_ids
      assert length(batch_ids) == 2

      # Pop one message
      {[_msg], state} = State.pop_messages(state, [{msg1.commit_lsn, msg1.commit_idx}])
      assert State.unpersisted_table_reader_batch_ids(state) == [batch_id2]
    end

    test "correctly tracks multiple in-flight batches with partial persistence", %{state: state} do
      # Set up three different batches
      batch_id1 = "batch-1"
      batch_id2 = "batch-2"
      batch_id3 = "batch-3"

      # Create messages for each batch
      msgs_batch1 = [
        ConsumersFactory.consumer_message(group_id: "group1"),
        ConsumersFactory.consumer_message(group_id: "group1")
      ]

      msgs_batch2 = [
        ConsumersFactory.consumer_message(group_id: "group2")
      ]

      msgs_batch3 = [
        ConsumersFactory.consumer_message(group_id: "group3"),
        ConsumersFactory.consumer_message(group_id: "group3")
      ]

      # Put all batches into state
      {:ok, state} = State.put_table_reader_batch(state, msgs_batch1, batch_id1)
      {:ok, state} = State.put_table_reader_batch(state, msgs_batch2, batch_id2)
      {:ok, state} = State.put_table_reader_batch(state, msgs_batch3, batch_id3)

      # Initially all batch IDs should be present
      batch_ids = State.unpersisted_table_reader_batch_ids(state)
      assert length(batch_ids) == 3
      assert batch_id1 in batch_ids
      assert batch_id2 in batch_ids
      assert batch_id3 in batch_ids

      # Persist all messages from batch1
      state = State.put_persisted_messages(state, msgs_batch1)

      # Pop the persisted messages
      cursor_tuples = Enum.map(msgs_batch1, &{&1.commit_lsn, &1.commit_idx})
      {_popped, state} = State.pop_messages(state, cursor_tuples)

      # batch1 should no longer be in unpersisted batches
      batch_ids = State.unpersisted_table_reader_batch_ids(state)
      assert length(batch_ids) == 2
      refute batch_id1 in batch_ids
      assert batch_id2 in batch_ids
      assert batch_id3 in batch_ids

      # Persist one message from batch3
      [msg_batch3 | _] = msgs_batch3
      state = State.put_persisted_messages(state, [msg_batch3])
      {_popped, state} = State.pop_messages(state, [{msg_batch3.commit_lsn, msg_batch3.commit_idx}])

      # batch3 should still be present as it has unpersisted messages
      batch_ids = State.unpersisted_table_reader_batch_ids(state)
      assert length(batch_ids) == 2
      assert batch_id2 in batch_ids
      assert batch_id3 in batch_ids

      # Persist and pop remaining messages
      remaining_messages = [List.last(msgs_batch3) | msgs_batch2]
      state = State.put_persisted_messages(state, remaining_messages)
      cursor_tuples = Enum.map(remaining_messages, &{&1.commit_lsn, &1.commit_idx})
      {_popped, state} = State.pop_messages(state, cursor_tuples)

      # No batches should remain unpersisted
      assert State.unpersisted_table_reader_batch_ids(state) == []
    end
  end

  describe "messages_to_flush/1" do
    test "returns old unpersisted messages", %{state: state} do
      now = DateTime.utc_now()
      past = DateTime.add(now, -2000, :millisecond)

      # Configure flush threshold to 1000ms
      state = %{state | message_age_before_flush_ms: 1000}

      # Create one old message and one new message
      old_msg = ConsumersFactory.consumer_message(ingested_at: past)
      new_msg = ConsumersFactory.consumer_message(ingested_at: now)

      {:ok, state} = State.put_messages(state, [old_msg, new_msg])

      # Stub current time
      stub_utc_now(fn -> now end)

      # Only old message should be returned
      messages_to_flush = State.messages_to_flush(state)
      assert length(messages_to_flush) == 1
      [msg] = messages_to_flush
      assert {msg.commit_lsn, msg.commit_idx} == {old_msg.commit_lsn, old_msg.commit_idx}
    end

    test "excludes persisted messages regardless of age", %{state: state} do
      now = DateTime.utc_now()
      past = DateTime.add(now, -2000, :millisecond)

      # Configure flush threshold to 1000ms
      state = %{state | message_age_before_flush_ms: 1000}

      # Create old messages, one persisted and one unpersisted
      old_persisted_msg = ConsumersFactory.consumer_message(ingested_at: past)
      old_unpersisted_msg = ConsumersFactory.consumer_message(ingested_at: past)

      # Put the persisted message through persisted path
      state = State.put_persisted_messages(state, [old_persisted_msg])
      # Put unpersisted message normally
      {:ok, state} = State.put_messages(state, [old_unpersisted_msg])

      # Stub current time
      stub_utc_now(fn -> now end)

      # Only unpersisted old message should be returned
      messages_to_flush = State.messages_to_flush(state)
      assert length(messages_to_flush) == 1
      [msg] = messages_to_flush
      assert {msg.commit_lsn, msg.commit_idx} == {old_unpersisted_msg.commit_lsn, old_unpersisted_msg.commit_idx}
    end

    test "returns messages in order", %{state: state} do
      state = %{state | message_age_before_flush_ms: 0}

      msgs =
        Enum.shuffle([
          ConsumersFactory.consumer_message(commit_lsn: 1, commit_idx: 0),
          ConsumersFactory.consumer_message(commit_lsn: 1, commit_idx: 1),
          ConsumersFactory.consumer_message(commit_lsn: 2, commit_idx: 3),
          ConsumersFactory.consumer_message(commit_lsn: 4, commit_idx: 0),
          ConsumersFactory.consumer_message(commit_lsn: 8, commit_idx: 8),
          ConsumersFactory.consumer_message(commit_lsn: 9, commit_idx: 0)
        ])

      {:ok, state} = State.put_messages(state, msgs)
      sorted = Enum.sort_by(msgs, fn m -> {m.commit_lsn, m.commit_idx} end)

      # Messages returned in order
      msgs1 = State.messages_to_flush(state, 1)
      assert [Enum.at(sorted, 0)] == msgs1
      state = State.put_persisted_messages(state, msgs1)

      msgs2 = State.messages_to_flush(state, 1)
      assert [Enum.at(sorted, 1)] == msgs2
      state = State.put_persisted_messages(state, msgs2)

      assert Enum.drop(sorted, 2) == State.messages_to_flush(state)
    end
  end

  describe "messages_to_make_visible/1" do
    test "returns messages that are stuck in produced_message_groups", %{state: state} do
      state = %{state | max_time_since_delivered_ms: 200}

      now = DateTime.utc_now()
      past = DateTime.add(now, -2000, :millisecond)

      # Create messages with different states
      msg1 = ConsumersFactory.consumer_message()
      msg2 = ConsumersFactory.consumer_message()
      msg3 = ConsumersFactory.consumer_message()

      {:ok, state} = State.put_messages(state, [msg1, msg2, msg3])

      # Produce messages with different timestamps
      expect_utc_now(2, fn -> past end)
      assert {[produced1], state} = State.produce_messages(state, 1)

      stub_utc_now(fn -> now end)
      assert {[_produced2, _produced3], state} = State.produce_messages(state, 2)

      # Only message with old last_delivered_at should be returned
      messages_to_make_visible = State.messages_to_make_visible(state)
      assert length(messages_to_make_visible) == 1
      [msg] = messages_to_make_visible
      assert {msg.commit_lsn, msg.commit_idx} == {produced1.commit_lsn, produced1.commit_idx}
    end

    test "returns empty list when no messages are stuck", %{state: state} do
      # Create messages
      msg1 = ConsumersFactory.consumer_message()
      msg2 = ConsumersFactory.consumer_message()

      {:ok, state} = State.put_messages(state, [msg1, msg2])

      # Produce messages with current timestamp
      assert {[_produced1, _produced2], state} = State.produce_messages(state, 2)

      # No messages should be returned as they were just delivered
      assert State.messages_to_make_visible(state) == []
    end

    test "handles messages without last_delivered_at", %{state: state} do
      # Create message
      msg = ConsumersFactory.consumer_message()
      {:ok, state} = State.put_messages(state, [msg])

      # Message should not be returned as it hasn't been delivered
      assert State.messages_to_make_visible(state) == []
    end
  end

  defp assert_cursor_tuple_matches(msg1, msg2) do
    assert msg1.commit_lsn == msg2.commit_lsn
    assert msg1.commit_idx == msg2.commit_idx
  end

  defp assert_message_in_state(message, state) do
    assert Enum.find(state.messages, fn {_cursor_tuple, msg} -> msg.id == message.id end)
  end
end

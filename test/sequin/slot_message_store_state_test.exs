defmodule Sequin.DatabasesRuntime.SlotMessageStoreStateTest do
  use Sequin.Case, async: true

  alias Sequin.Consumers.SinkConsumer
  alias Sequin.DatabasesRuntime.SlotMessageStore.State
  alias Sequin.Error
  alias Sequin.Factory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Multiset

  setup do
    state = %State{consumer: %SinkConsumer{seq: Factory.unique_integer()}}
    State.setup_ets(state.consumer)
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
      state = %{state | setting_max_accumulated_payload_bytes: 150}

      msg1 = ConsumersFactory.consumer_message(payload_size_bytes: 100)

      msg2 = ConsumersFactory.consumer_message(payload_size_bytes: 100)

      assert {:error, %Error.InvariantError{}} = State.put_messages(state, [msg1, msg2])
    end
  end

  describe "put_persisted_messages/2" do
    test "updates persisted_message_groups", %{state: state} do
      msg1 = ConsumersFactory.consumer_message(group_id: "group1")
      msg2 = ConsumersFactory.consumer_message(group_id: "group1")

      {:ok, state} = State.put_persisted_messages(state, [msg1, msg2])

      assert_message_in_state(msg1, state)
      assert_message_in_state(msg2, state)

      # Verify persisted_message_groups tracking
      assert Multiset.value_member?(state.persisted_message_groups, "group1", msg1.ack_id)
      assert Multiset.value_member?(state.persisted_message_groups, "group1", msg2.ack_id)
      assert Multiset.count(state.persisted_message_groups, "group1") == 2
    end

    test "preserves existing messages when adding new persisted messages", %{state: state} do
      # Add initial message
      msg1 = ConsumersFactory.consumer_message(group_id: "group1")
      {:ok, state} = State.put_messages(state, [msg1])

      # Add persisted message
      msg2 = ConsumersFactory.consumer_message(group_id: "group2")
      {:ok, state} = State.put_persisted_messages(state, [msg2])

      # Verify both messages exist
      assert map_size(state.messages) == 2
      assert_message_in_state(msg1, state)
      assert_message_in_state(msg2, state)

      # Only persisted message should be in persisted_message_groups
      assert Multiset.count(state.persisted_message_groups, "group1") == 0
      assert Multiset.count(state.persisted_message_groups, "group2") == 1
    end

    test "handles multiple groups in persisted_message_groups", %{state: state} do
      msg1 = ConsumersFactory.consumer_message(group_id: "group1")
      msg2 = ConsumersFactory.consumer_message(group_id: "group2")
      msg3 = ConsumersFactory.consumer_message(group_id: "group1")

      {:ok, state} = State.put_persisted_messages(state, [msg1, msg2, msg3])

      assert Multiset.count(state.persisted_message_groups, "group1") == 2
      assert Multiset.count(state.persisted_message_groups, "group2") == 1
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

      assert {[_msg1], state} = State.pop_messages(state, [msg1.ack_id])

      assert state.payload_size_bytes == 200
    end

    test "when a produced message is popped, its group_id is removed from produced_message_groups", %{state: state} do
      msg = ConsumersFactory.consumer_message(group_id: "group1")
      {:ok, state} = State.put_messages(state, [msg])

      # Produce the message to add it to produced_message_groups
      assert {[produced_msg], state} = State.produce_messages(state, 1)
      assert Multiset.count(state.produced_message_groups, "group1") == 1

      # Pop the message and verify it's removed from produced_message_groups
      assert {[_msg], state} = State.pop_messages(state, [produced_msg.ack_id])
      assert Multiset.count(state.produced_message_groups, "group1") == 0
    end

    test "when a persisted message is popped, its group_id is removed from persisted_message_groups", %{state: state} do
      msg = ConsumersFactory.consumer_message(group_id: "group1")
      {:ok, state} = State.put_persisted_messages(state, [msg])

      # Verify message is in persisted_message_groups
      assert Multiset.count(state.persisted_message_groups, "group1") == 1

      # Pop the message and verify it's removed from persisted_message_groups
      assert {[_msg], state} = State.pop_messages(state, [msg.ack_id])
      assert Multiset.count(state.persisted_message_groups, "group1") == 0
    end
  end

  describe "pop_blocked_messages/2" do
    test "pops messages that should be moved to persisted_message_groups in persisted_message_groups", %{state: state} do
      # Add two messages with different group_ids
      msg1 = ConsumersFactory.consumer_message(group_id: "group1")
      msg2 = ConsumersFactory.consumer_message(group_id: "group2")

      {:ok, state} = State.put_messages(state, [msg1, msg2])

      # Add a persisted message that shares group_id with msg1
      persisted_msg = ConsumersFactory.consumer_message(group_id: "group1")
      {:ok, state} = State.put_persisted_messages(state, [persisted_msg])

      # Call pop_blocked_messages/2
      {popped_messages, updated_state} = State.pop_blocked_messages(state)

      # Verify we get back only msg1 which shares group_id with the persisted message
      assert length(popped_messages) == 1
      [popped_msg] = popped_messages
      assert popped_msg.ack_id == msg1.ack_id
      assert popped_msg.group_id == "group1"

      # Verify msg2 is still in state since it wasn't blocked
      assert_message_in_state(msg2, updated_state)
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

      assert {[_msg1, _msg2], state} = State.pop_messages(state, [produced1.ack_id, produced2.ack_id])

      assert {[produced3], state} = State.produce_messages(state, 1)
      assert {[_msg3], state} = State.pop_messages(state, [produced3.ack_id])

      assert {[], state} = State.produce_messages(state, 1)
      assert state.payload_size_bytes == 0
    end

    test "when a message is popped, messages for that group_id are available for delivery", %{state: state} do
      group_id = "group1"
      msg1 = ConsumersFactory.consumer_message(commit_lsn: 1, commit_idx: 0, group_id: group_id)
      msg2 = ConsumersFactory.consumer_message(commit_lsn: 1, commit_idx: 1, group_id: group_id)

      {:ok, state} = State.put_messages(state, [msg1, msg2])
      assert {[produced1], state} = State.produce_messages(state, 1)
      assert {[], state} = State.produce_messages(state, 1)

      assert {[_msg1], state} = State.pop_messages(state, [produced1.ack_id])

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

      assert {[acked1], state} = State.pop_messages(state, [produced1.ack_id])

      assert {[], state} = State.pop_messages(state, [Factory.uuid()])

      assert {[produced3, produced4], state} = State.produce_messages(state, 2)

      assert {acked_34, state} = State.pop_messages(state, [produced3.ack_id, produced4.ack_id])
      assert {[], state} = State.produce_messages(state, 1)
      assert {[acked2], _state} = State.pop_messages(state, [produced2.ack_id])

      assert_commit_tuple_matches(msg1, produced1)
      assert_commit_tuple_matches(msg1, acked1)

      assert_commit_tuple_matches(msg2, produced2)
      assert_commit_tuple_matches(msg2, acked2)

      assert_commit_tuple_matches(msg3, produced3)
      assert_commit_tuple_matches(msg4, produced4)

      # These acked messages can arrive in any order
      assert length(acked_34) == 2
      acked3 = Enum.find(acked_34, fn msg -> produced3.ack_id == msg.ack_id end)
      acked4 = Enum.find(acked_34, fn msg -> produced4.ack_id == msg.ack_id end)

      assert_commit_tuple_matches(msg3, acked3)
      assert_commit_tuple_matches(msg4, acked4)
    end
  end

  describe "is_message_group_persisted?/2" do
    test "returns true if the message group is persisted", %{state: state} do
      # Add a persisted message to establish a blocked group
      persisted_msg = ConsumersFactory.consumer_message(group_id: "group1")
      other_msg = ConsumersFactory.consumer_message(group_id: "group2")
      {:ok, state} = State.put_persisted_messages(state, [persisted_msg])
      {:ok, state} = State.put_messages(state, [other_msg])

      assert State.is_message_group_persisted?(state, "group1")
      refute State.is_message_group_persisted?(state, "group2")
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
      {:ok, state} = State.put_persisted_messages(state, [persisted_msg])

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
      group_id = "group1"
      msg1 = ConsumersFactory.consumer_message(commit_lsn: 1, commit_idx: 0, group_id: group_id)
      msg2 = ConsumersFactory.consumer_message(commit_lsn: 1, commit_idx: 1, group_id: group_id)

      {:ok, state} = State.put_messages(state, [msg1, msg2])
      assert {[produced1], state} = State.produce_messages(state, 1)
      assert {[], _state} = State.produce_messages(state, 1)

      assert produced1.commit_idx == msg1.commit_idx
    end

    test "only returns one message per group_id regardless of requested count", %{state: state} do
      # Create 3 messages, where 2 share the same group_id
      msg1 = ConsumersFactory.consumer_message(commit_lsn: 1, commit_idx: 0, group_id: "group1")
      msg2 = ConsumersFactory.consumer_message(commit_lsn: 1, commit_idx: 1, group_id: "group1")
      msg3 = ConsumersFactory.consumer_message(commit_lsn: 1, commit_idx: 2, group_id: "group2")

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

    test "delivers messages without group_ids independently", %{state: state} do
      # This test ensures backwards compatibility with messages that don't have a group_id
      # Create messages with nil group_ids
      msg1 = ConsumersFactory.consumer_message(group_id: nil, commit_lsn: 1)
      msg2 = ConsumersFactory.consumer_message(group_id: nil, commit_lsn: 2)
      msg3 = ConsumersFactory.consumer_message(group_id: nil, commit_lsn: 3)
      msg4 = ConsumersFactory.consumer_message(group_id: "group1", commit_lsn: 4)

      {:ok, state} = State.put_messages(state, [msg1, msg2, msg3, msg4])

      # Should be able to produce first message
      assert {[produced1, produced2], state} = State.produce_messages(state, 2)
      assert produced1.commit_lsn == msg1.commit_lsn
      assert produced2.commit_lsn == msg2.commit_lsn

      # Should be able to produce third message
      assert {[produced3], state} = State.produce_messages(state, 1)
      assert produced3.commit_lsn == msg3.commit_lsn

      # Should be able to produce fourth message
      assert {[produced4], _state} = State.produce_messages(state, 1)
      assert produced4.commit_lsn == msg4.commit_lsn
    end
  end

  describe "batch_progress/2" do
    @tag capture_log: true
    test "returns error when no batch is in progress", %{state: state} do
      assert {:error, %Error.InvariantError{message: "No batch in progress"}} =
               State.batch_progress(state, "any-batch-id")
    end

    test "tracks batch progress through message lifecycle", %{state: state} do
      batch_id = "test-batch-123"
      msg1 = ConsumersFactory.consumer_message()
      msg2 = ConsumersFactory.consumer_message()

      # Put messages in batch and verify in_progress
      {:ok, state} = State.put_table_reader_batch(state, [msg1, msg2], batch_id)
      assert {:ok, :in_progress} = State.batch_progress(state, batch_id)

      # Produce messages and verify still in_progress
      assert {[produced1, produced2], state} = State.produce_messages(state, 2)
      assert {:ok, :in_progress} = State.batch_progress(state, batch_id)

      # Ack messages and verify completed
      assert {[_msg1, _msg2], state} = State.pop_messages(state, [produced1.ack_id, produced2.ack_id])
      assert {:ok, :completed} = State.batch_progress(state, batch_id)
    end

    test "when a message is persisted, it is not included in the batch progress", %{state: state} do
      batch_id = "test-batch-123"
      msg1 = ConsumersFactory.consumer_message()
      msg2 = ConsumersFactory.consumer_message()

      # Put messages in batch
      {:ok, state} = State.put_table_reader_batch(state, [msg1, msg2], batch_id)
      assert {:ok, :in_progress} = State.batch_progress(state, batch_id)

      # Persist one message
      {:ok, state} = State.put_persisted_messages(state, [msg1])

      # Verify batch is still in progress since msg2 is not persisted
      assert {:ok, :in_progress} = State.batch_progress(state, batch_id)

      # Persist the second message
      assert {[_msg2], state} = State.pop_messages(state, [msg2.ack_id])
      {:ok, state} = State.put_persisted_messages(state, [msg2])

      # Verify batch is now completed since all messages are persisted
      assert {:ok, :completed} = State.batch_progress(state, batch_id)
    end

    @tag capture_log: true
    test "returns error when batch_id doesn't match", %{state: state} do
      {:ok, state} = State.put_table_reader_batch(state, [ConsumersFactory.consumer_message()], "batch-1")

      assert {:error, %Error.InvariantError{message: "Batch mismatch" <> _}} =
               State.batch_progress(state, "different-batch")
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
      assert Multiset.count(state.produced_message_groups, msg.group_id) == 1

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
      state = State.reset_message_visibilities(state, [produced1.ack_id, produced2.ack_id])

      # Messages should be available for delivery again
      assert {[redelivered1, redelivered2], _state} = State.produce_messages(state, 2)
      assert_commit_tuple_matches(produced1, redelivered1)
      assert_commit_tuple_matches(produced2, redelivered2)
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
      state = State.reset_message_visibilities(state, [produced1.ack_id, produced2.ack_id])

      # Only msg1 and msg2 should be available for redelivery
      assert {[redelivered1, redelivered2], state} = State.produce_messages(state, 3)
      assert_commit_tuple_matches(produced1, redelivered1)
      assert_commit_tuple_matches(produced2, redelivered2)

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
      non_existent_ack_id = "non-existent-ack-id"
      state = State.reset_message_visibilities(state, [produced1.ack_id, non_existent_ack_id])

      # Should still work for the valid message
      assert {[redelivered1], _state} = State.produce_messages(state, 1)
      assert_commit_tuple_matches(produced1, redelivered1)
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
      assert_commit_tuple_matches(produced1, redelivered1)
      assert_commit_tuple_matches(produced2, redelivered2)
    end
  end

  defp assert_commit_tuple_matches(msg1, msg2) do
    assert msg1.commit_lsn == msg2.commit_lsn
    assert msg1.commit_idx == msg2.commit_idx
  end

  defp assert_message_in_state(message, state) do
    assert Enum.find(state.messages, fn {_ack_id, msg} -> msg.id == message.id end)
  end
end

defmodule Sequin.DatabasesRuntime.SlotMessageStoreStateTest do
  use Sequin.Case, async: true

  alias Sequin.DatabasesRuntime.SlotMessageStore.State
  alias Sequin.Error
  alias Sequin.Factory
  alias Sequin.Factory.ConsumersFactory

  setup do
    state = %State{}
    {:ok, %{state: state}}
  end

  describe "put_messages/2 with messages" do
    test "merges new messages into empty state", %{state: state} do
      msg1 = ConsumersFactory.consumer_message()
      msg2 = ConsumersFactory.consumer_message()
      expect_uuid4(fn -> msg1.ack_id end)
      expect_uuid4(fn -> msg2.ack_id end)
      {:ok, state} = State.put_messages(state, [msg1, msg2])

      assert map_size(state.messages) == 2
      assert Map.has_key?(state.messages, msg1.ack_id)
      assert Map.has_key?(state.messages, msg2.ack_id)
      assert map_size(state.produced_messages) == 0
    end

    test "merges new messages with existing messages", %{state: state} do
      # Add initial message
      msg1 = ConsumersFactory.consumer_message()
      expect_uuid4(fn -> msg1.ack_id end)
      {:ok, state} = State.put_messages(state, [msg1])

      # Add new message
      msg2 = ConsumersFactory.consumer_message()
      expect_uuid4(fn -> msg2.ack_id end)
      {:ok, updated_state} = State.put_messages(state, [msg2])

      assert map_size(updated_state.messages) == 2
      assert Map.has_key?(updated_state.messages, msg1.ack_id)
      assert Map.has_key?(updated_state.messages, msg2.ack_id)
      assert map_size(updated_state.produced_messages) == 0
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

  describe "end to end test with ack/2" do
    test "ack'd messages are removed from state", %{state: state} do
      now = DateTime.utc_now()
      expect_utc_now(3, fn -> now end)

      msg1 = ConsumersFactory.consumer_message(commit_lsn: 1, commit_idx: 0)
      msg2 = ConsumersFactory.consumer_message(commit_lsn: 1, commit_idx: 1)
      msg3 = ConsumersFactory.consumer_message(commit_lsn: 1, commit_idx: 2)

      {:ok, state} = State.put_messages(state, [msg1, msg2, msg3])
      assert {state, [produced1, produced2]} = State.produce_messages(state, 2)

      assert {state, [_msg1, _msg2], 2} = State.ack(state, [produced1.ack_id, produced2.ack_id])

      assert {state, [produced3]} = State.produce_messages(state, 2)
      assert {state, [_msg3], 1} = State.ack(state, [produced3.ack_id])

      assert {state, []} = State.produce_messages(state, 1)
      assert state.payload_size_bytes == 0
    end

    test "when a message is ack'd, messages for that group_id are available for delivery", %{state: state} do
      group_id = "group1"
      msg1 = ConsumersFactory.consumer_message(commit_lsn: 1, commit_idx: 0, group_id: group_id)
      msg2 = ConsumersFactory.consumer_message(commit_lsn: 1, commit_idx: 1, group_id: group_id)

      {:ok, state} = State.put_messages(state, [msg1, msg2])
      assert {state, [produced1]} = State.produce_messages(state, 1)
      assert {state, []} = State.produce_messages(state, 1)

      assert {state, [_msg1], 1} = State.ack(state, [produced1.ack_id])

      assert {_state, [produced2]} = State.produce_messages(state, 1)

      assert produced1.commit_idx == msg1.commit_idx
      assert produced2.commit_idx == msg2.commit_idx
    end

    test "when a message is ack'd, its payload size bytes are removed from state", %{state: state} do
      msg1 = ConsumersFactory.consumer_message(commit_lsn: 1, commit_idx: 0, payload_size_bytes: 100)
      msg2 = ConsumersFactory.consumer_message(commit_lsn: 1, commit_idx: 1, payload_size_bytes: 200)

      {:ok, state} = State.put_messages(state, [msg1, msg2])

      assert {state, [produced1]} = State.produce_messages(state, 1)
      assert {state, [_msg1], 1} = State.ack(state, [produced1.ack_id])

      assert state.payload_size_bytes == 200
    end

    test "mixed order of operations", %{state: state} do
      msg1 = ConsumersFactory.consumer_message(commit_lsn: 1, commit_idx: 0)
      msg2 = ConsumersFactory.consumer_message(commit_lsn: 1, commit_idx: 1)
      msg3 = ConsumersFactory.consumer_message(commit_lsn: 2, commit_idx: 0)
      msg4 = ConsumersFactory.consumer_message(commit_lsn: 3, commit_idx: 0)

      {:ok, state} = State.put_messages(state, [msg1, msg2])
      assert {state, [produced1, produced2]} = State.produce_messages(state, 2)
      assert {state, []} = State.produce_messages(state, 2)

      {:ok, state} = State.put_messages(state, [msg3, msg4])

      assert {state, [acked1], 1} = State.ack(state, [produced1.ack_id])

      assert {state, [], 0} = State.ack(state, [Factory.uuid()])

      assert {state, [produced3, produced4]} = State.produce_messages(state, 4)

      assert {state, acked_34, 2} = State.ack(state, [produced3.ack_id, produced4.ack_id])
      assert {state, []} = State.produce_messages(state, 1)
      assert {_state, [acked2], 1} = State.ack(state, [produced2.ack_id])

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

  describe "nack_produced_messages/1" do
    test "moves all messages from produced_messages to messages", %{state: state} do
      msg1 = ConsumersFactory.consumer_message()
      msg2 = ConsumersFactory.consumer_message()

      {:ok, state} = State.put_messages(state, [msg1, msg2])
      assert {state, [_msg1, _msg2]} = State.produce_messages(state, 2)
      assert {state, []} = State.produce_messages(state, 2)

      assert {state, 2} = State.nack_produced_messages(state)

      assert {_state, [_msg1, _msg2]} = State.produce_messages(state, 2)
    end
  end

  describe "min_wal_cursor/1" do
    test "returns nil when there are no messages", %{state: state} do
      assert State.min_wal_cursor(state) == nil
    end

    test "returns lowest wal_cursor from messages", %{state: state} do
      messages = [
        ConsumersFactory.consumer_message(commit_lsn: 100, commit_idx: 4),
        ConsumersFactory.consumer_message(commit_lsn: 200, commit_idx: 1)
      ]

      {:ok, state} = State.put_messages(state, messages)

      assert State.min_wal_cursor(state) == %{commit_lsn: 100, commit_idx: 4}
    end

    test "returns lowest wal_cursor from messages with the same commit_lsn", %{state: state} do
      messages = [
        ConsumersFactory.consumer_message(commit_lsn: 100, commit_idx: 4),
        ConsumersFactory.consumer_message(commit_lsn: 100, commit_idx: 1)
      ]

      {:ok, state} = State.put_messages(state, messages)

      assert State.min_wal_cursor(state) == %{commit_lsn: 100, commit_idx: 1}
    end

    test "returns lowest wal_cursor from produced messages", %{state: state} do
      messages = [
        ConsumersFactory.consumer_message(commit_lsn: 100, commit_idx: 4),
        ConsumersFactory.consumer_message(commit_lsn: 200, commit_idx: 1)
      ]

      {:ok, state} = State.put_messages(state, messages)
      assert {state, [_msg1, _msg2]} = State.produce_messages(state, 2)

      assert State.min_wal_cursor(state) == %{commit_lsn: 100, commit_idx: 4}
    end

    test "returns lowest wal_cursor, factoring in both messages and produced_messages", %{state: state} do
      msg1 = ConsumersFactory.consumer_message(commit_lsn: 100, commit_idx: 1)
      msg2 = ConsumersFactory.consumer_message(commit_lsn: 100, commit_idx: 4)

      {:ok, state} = State.put_messages(state, [msg1, msg2])
      assert {state, [produced1]} = State.produce_messages(state, 1)

      assert State.min_wal_cursor(state) == %{commit_lsn: 100, commit_idx: 1}

      assert {state, [_msg], 1} = State.ack(state, [produced1.ack_id])

      assert State.min_wal_cursor(state) == %{commit_lsn: 100, commit_idx: 4}
    end
  end

  describe "produce_messages/2" do
    test "delivers messages, excluding messages that are inflight", %{state: state} do
      now = DateTime.utc_now()
      expect_utc_now(3, fn -> now end)

      msg1 = ConsumersFactory.consumer_message(commit_lsn: 1, commit_idx: 0)
      msg2 = ConsumersFactory.consumer_message(commit_lsn: 1, commit_idx: 1)

      {:ok, state} = State.put_messages(state, [msg1, msg2])
      assert {state, [produced1]} = State.produce_messages(state, 1)
      assert {state, [produced2]} = State.produce_messages(state, 1)

      assert produced1.commit_idx == msg1.commit_idx
      assert produced2.commit_idx == msg2.commit_idx

      assert produced1.last_delivered_at == now

      assert {_state, []} = State.produce_messages(state, 1)
    end

    test "respects group_id for inflight messages", %{state: state} do
      group_id = "group1"
      msg1 = ConsumersFactory.consumer_message(commit_lsn: 1, commit_idx: 0, group_id: group_id)
      msg2 = ConsumersFactory.consumer_message(commit_lsn: 1, commit_idx: 1, group_id: group_id)

      {:ok, state} = State.put_messages(state, [msg1, msg2])
      assert {state, [produced1]} = State.produce_messages(state, 1)
      assert {_state, []} = State.produce_messages(state, 1)

      assert produced1.commit_idx == msg1.commit_idx
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
      assert {state, [produced1, produced2]} = State.produce_messages(state, 2)
      assert {:ok, :in_progress} = State.batch_progress(state, batch_id)

      # Ack messages and verify completed
      assert {state, [_msg1, _msg2], 2} = State.ack(state, [produced1.ack_id, produced2.ack_id])
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
      assert {state, _msgs} = State.produce_messages(state, Enum.random(1..length(messages)))

      assert peeked_messages = State.peek_messages(state, 3)
      assert Enum.map(peeked_messages, & &1.commit_lsn) == [1, 1, 1]
      assert Enum.map(peeked_messages, & &1.commit_idx) == [0, 1, 2]
    end
  end

  defp assert_commit_tuple_matches(msg1, msg2) do
    assert msg1.commit_lsn == msg2.commit_lsn
    assert msg1.commit_idx == msg2.commit_idx
  end
end

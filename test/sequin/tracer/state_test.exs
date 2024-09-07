defmodule Sequin.Tracer.StateTest do
  use Sequin.DataCase, async: true

  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Tracer.State

  describe "new/1" do
    test "creates a new state with the given account_id" do
      state = State.new("account123")
      assert state.account_id == "account123"
      assert state.message_traces == []
    end
  end

  describe "message_replicated/2" do
    test "adds a new trace message with empty consumer_traces" do
      state = State.new("account123")
      database = DatabasesFactory.postgres_database()
      message = ReplicationFactory.postgres_message()

      updated_state = State.message_replicated(state, database, message)

      assert length(updated_state.message_traces) == 1
      [message_trace] = updated_state.message_traces
      assert message_trace.message == message
      assert message_trace.consumer_traces == []
    end
  end

  describe "message_filtered/3" do
    test "adds a filtered event to an existing trace message for a specific consumer" do
      state = State.new("account123")
      database = DatabasesFactory.postgres_database()
      consumer = ConsumersFactory.consumer(id: "consumer1")
      message = ReplicationFactory.postgres_message()

      state = State.message_replicated(state, database, message)
      updated_state = State.message_filtered(state, consumer.id, message)

      [message_trace] = updated_state.message_traces
      assert length(message_trace.consumer_traces) == 1
      [consumer_trace] = message_trace.consumer_traces
      assert consumer_trace.consumer_id == "consumer1"
      assert length(consumer_trace.spans) == 1
      [filtered_event] = consumer_trace.spans
      assert filtered_event.type == :filtered
    end
  end

  describe "messages_ingested/3" do
    test "adds ingested spans to existing trace messages for a specific consumer" do
      state = State.new("account123")
      database = DatabasesFactory.postgres_database()
      message1 = ReplicationFactory.postgres_message()
      message2 = ReplicationFactory.postgres_message()
      state = State.message_replicated(state, database, message1)
      state = State.message_replicated(state, database, message2)

      spans = [
        ConsumersFactory.consumer_event(replication_message_trace_id: message1.trace_id),
        ConsumersFactory.consumer_event(replication_message_trace_id: message2.trace_id)
      ]

      updated_state = State.messages_ingested(state, "consumer1", spans)

      assert length(updated_state.message_traces) == 2

      Enum.each(updated_state.message_traces, fn tm ->
        assert length(tm.consumer_traces) == 1
        [consumer_trace] = tm.consumer_traces
        assert consumer_trace.consumer_id == "consumer1"
        assert length(consumer_trace.spans) == 1
        [ingested_event] = consumer_trace.spans
        assert ingested_event.type == :ingested
      end)
    end
  end

  describe "messages_received/3" do
    test "adds received spans to existing trace messages for a specific consumer" do
      state = State.new("account123")
      database = DatabasesFactory.postgres_database()
      message1 = ReplicationFactory.postgres_message()
      message2 = ReplicationFactory.postgres_message()

      state = State.message_replicated(state, database, message1)
      state = State.message_replicated(state, database, message2)

      spans = [
        ConsumersFactory.consumer_event(replication_message_trace_id: message1.trace_id),
        ConsumersFactory.consumer_event(replication_message_trace_id: message2.trace_id)
      ]

      updated_state = State.messages_received(state, "consumer1", spans)

      assert length(updated_state.message_traces) == 2

      Enum.each(updated_state.message_traces, fn tm ->
        assert length(tm.consumer_traces) == 1
        [consumer_trace] = tm.consumer_traces
        assert consumer_trace.consumer_id == "consumer1"
        assert length(consumer_trace.spans) == 1
        [received_event] = consumer_trace.spans
        assert received_event.type == :received
      end)
    end

    test "adds multiple received spans to an existing trace message for a specific consumer" do
      state = State.new("account123")
      database = DatabasesFactory.postgres_database()
      message = ReplicationFactory.postgres_message()
      state = State.message_replicated(state, database, message)

      event1 = ConsumersFactory.consumer_event(replication_message_trace_id: message.trace_id)
      event2 = ConsumersFactory.consumer_event(replication_message_trace_id: message.trace_id)

      state = State.messages_received(state, "consumer1", [event1])
      state = State.messages_received(state, "consumer1", [event2])

      assert length(state.message_traces) == 1
      [message_trace] = state.message_traces
      assert length(message_trace.consumer_traces) == 1
      [consumer_trace] = message_trace.consumer_traces
      assert consumer_trace.consumer_id == "consumer1"
      assert length(consumer_trace.spans) == 2
      [received_event1, received_event2] = consumer_trace.spans
      assert received_event1.type == :received
      assert received_event2.type == :received
    end
  end

  describe "messages_acked/3" do
    test "adds acked spans to existing trace messages for a specific consumer" do
      state = State.new("account123")
      database = DatabasesFactory.postgres_database()
      message = ReplicationFactory.postgres_message()
      state = State.message_replicated(state, database, message)

      event =
        ConsumersFactory.consumer_event(
          replication_message_trace_id: message.trace_id,
          ack_id: "ack123"
        )

      state = State.messages_ingested(state, "consumer1", [event])
      state = State.messages_received(state, "consumer1", [event])

      updated_state = State.messages_acked(state, "consumer1", ["ack123"])

      assert length(updated_state.message_traces) == 1
      [message_trace] = updated_state.message_traces
      assert length(message_trace.consumer_traces) == 1
      [consumer_trace] = message_trace.consumer_traces
      assert consumer_trace.consumer_id == "consumer1"
      assert length(consumer_trace.spans) == 3
      [acked_event, received_event, ingested_event] = consumer_trace.spans
      assert acked_event.type == :acked
      assert received_event.type == :received
      assert ingested_event.type == :ingested
    end

    @tag capture_log: true
    test "handles non-existent ack_ids gracefully for a specific consumer" do
      state = State.new("account123")
      database = DatabasesFactory.postgres_database()
      message = ReplicationFactory.postgres_message()
      state = State.message_replicated(state, database, message)

      event = ConsumersFactory.consumer_event(replication_message_trace_id: message.trace_id)
      state = State.messages_ingested(state, "consumer1", [event])

      updated_state = State.messages_acked(state, "consumer1", ["non_existent_ack_id"])

      assert updated_state == state
    end
  end

  test "handles multiple consumers for a single message" do
    state = State.new("account123")
    database = DatabasesFactory.postgres_database()
    message = ReplicationFactory.postgres_message()
    state = State.message_replicated(state, database, message)

    event = ConsumersFactory.consumer_event(replication_message_trace_id: message.trace_id)

    state = State.messages_ingested(state, "consumer1", [event])
    state = State.messages_received(state, "consumer1", [event])
    state = State.messages_ingested(state, "consumer2", [event])
    state = State.messages_acked(state, "consumer1", [event.ack_id])

    [message_trace] = state.message_traces
    assert length(message_trace.consumer_traces) == 2

    consumer1_trace = Enum.find(message_trace.consumer_traces, &(&1.consumer_id == "consumer1"))
    consumer2_trace = Enum.find(message_trace.consumer_traces, &(&1.consumer_id == "consumer2"))

    assert length(consumer1_trace.spans) == 3
    assert length(consumer2_trace.spans) == 1

    [acked, received, ingested] = consumer1_trace.spans
    assert acked.type == :acked
    assert received.type == :received
    assert ingested.type == :ingested

    [ingested] = consumer2_trace.spans
    assert ingested.type == :ingested
  end
end

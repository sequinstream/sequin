defmodule Sequin.Consumers.AcknowledgedMessagesTest do
  use ExUnit.Case, async: true

  alias Sequin.Consumers.AcknowledgedMessages
  alias Sequin.Factory.ConsumersFactory

  setup do
    # Clear Redis before each test
    Redix.command(:redix, ["FLUSHALL"])
    :ok
  end

  describe "store_messages/3" do
    test "stores messages and updates existing ones" do
      consumer_id = "test_consumer"

      message1 =
        ConsumersFactory.consumer_record(
          consumer_id: consumer_id,
          id: 1,
          ack_id: "ack1",
          replication_message_trace_id: "trace1"
        )

      message2 =
        ConsumersFactory.consumer_record(
          consumer_id: consumer_id,
          id: 2,
          ack_id: "ack2",
          replication_message_trace_id: "trace2"
        )

      assert :ok = AcknowledgedMessages.store_messages(consumer_id, [message1, message2])

      {:ok, stored_messages} = AcknowledgedMessages.fetch_messages(consumer_id)
      dbg(stored_messages)
      assert length(stored_messages) == 2
      assert Enum.map(stored_messages, & &1.id) == ["2", "1"]

      # Update message1 with a new delivery count
      updated_message1 = %{message1 | deliver_count: message1.deliver_count + 1}
      assert :ok = AcknowledgedMessages.store_messages(consumer_id, [updated_message1])

      {:ok, stored_messages} = AcknowledgedMessages.fetch_messages(consumer_id)
      assert length(stored_messages) == 2
      assert Enum.find(stored_messages, &(&1.id == "1")).deliver_count == message1.deliver_count + 1
    end

    test "trims to max_messages" do
      consumer_id = "test_consumer"

      messages =
        Enum.map(1..10, fn i ->
          ConsumersFactory.consumer_record(consumer_id: consumer_id, id: i)
        end)

      assert :ok = AcknowledgedMessages.store_messages(consumer_id, messages, 5)

      {:ok, stored_messages} = AcknowledgedMessages.fetch_messages(consumer_id)
      assert length(stored_messages) == 5
      assert Enum.map(stored_messages, & &1.id) == ["10", "9", "8", "7", "6"]
    end
  end

  describe "fetch_messages/3" do
    test "fetches messages with correct offset and count" do
      consumer_id = "test_consumer"

      messages =
        Enum.map(1..10, fn i ->
          ConsumersFactory.consumer_record(consumer_id: consumer_id, id: i)
        end)

      assert :ok = AcknowledgedMessages.store_messages(consumer_id, messages)

      {:ok, fetched_messages} = AcknowledgedMessages.fetch_messages(consumer_id, 3, 2)
      assert length(fetched_messages) == 3
      assert Enum.map(fetched_messages, & &1.id) == ["8", "7", "6"]
    end
  end

  describe "count_messages/1" do
    test "returns correct count of messages" do
      consumer_id = "test_consumer"

      messages =
        Enum.map(1..5, fn i ->
          ConsumersFactory.consumer_record(consumer_id: consumer_id, id: i)
        end)

      assert :ok = AcknowledgedMessages.store_messages(consumer_id, messages)

      assert {:ok, 5} = AcknowledgedMessages.count_messages(consumer_id)

      # Add more messages
      more_messages =
        Enum.map(6..8, fn i ->
          ConsumersFactory.consumer_record(consumer_id: consumer_id, id: i)
        end)

      assert :ok = AcknowledgedMessages.store_messages(consumer_id, more_messages)

      assert {:ok, 8} = AcknowledgedMessages.count_messages(consumer_id)
    end
  end
end

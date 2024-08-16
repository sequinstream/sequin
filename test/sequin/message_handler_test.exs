defmodule Sequin.MessageHandlerTest do
  use Sequin.DataCase, async: true

  alias Sequin.Consumers
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Replication.MessageHandler

  describe "handle_messages/2" do
    test "two messages with two consumers are fanned out to each consumer" do
      # Create two consumers
      consumer1 = ConsumersFactory.insert_consumer!(message_kind: :event)
      consumer2 = ConsumersFactory.insert_consumer!(message_kind: :event)

      # Create two messages
      [message1, message2] = [ReplicationFactory.postgres_message(), ReplicationFactory.postgres_message()]

      # Create context with both consumers
      context = %MessageHandler.Context{consumers: [consumer1, consumer2]}

      # Handle messages
      {:ok, 4} = MessageHandler.handle_messages(context, [message1, message2])

      # Verify consumer events for consumer1
      consumer1_events = Consumers.list_consumer_events_for_consumer(consumer1.id)
      assert length(consumer1_events) == 2
      assert Enum.all?(consumer1_events, &(&1.consumer_id == consumer1.id))

      # Verify consumer events for consumer2
      consumer2_events = Consumers.list_consumer_events_for_consumer(consumer2.id)
      assert length(consumer2_events) == 2
      assert Enum.all?(consumer2_events, &(&1.consumer_id == consumer2.id))

      # Verify that the events contain the correct data
      all_events = consumer1_events ++ consumer2_events
      assert Enum.any?(all_events, &(&1.commit_lsn == DateTime.to_unix(message1.commit_timestamp, :microsecond)))
      assert Enum.any?(all_events, &(&1.commit_lsn == DateTime.to_unix(message2.commit_timestamp, :microsecond)))
    end
  end
end

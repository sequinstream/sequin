defmodule Sequin.SlotMessageStoreTest do
  use Sequin.DataCase, async: true

  alias Sequin.DatabasesRuntime.SlotMessageStore
  alias Sequin.Factory
  alias Sequin.Factory.ConsumersFactory

  describe "SlotMessageStore message handling" do
    setup do
      consumer = ConsumersFactory.sink_consumer()

      start_supervised!({SlotMessageStore, consumer: consumer, test_pid: self()})

      %{consumer: consumer}
    end

    test "puts, delivers, and acks messages", %{consumer: consumer} do
      # Create test events
      messages = [
        ConsumersFactory.consumer_message(),
        ConsumersFactory.consumer_message()
      ]

      # Put messages in store
      :ok = SlotMessageStore.put_messages(consumer.id, messages)

      # Retrieve messages
      {:ok, delivered} = SlotMessageStore.produce(consumer.id, 2, self())
      assert length(delivered) == 2

      # For acks
      ack_ids = Enum.map(delivered, & &1.ack_id)
      {:ok, 2} = SlotMessageStore.ack(consumer.id, ack_ids)

      # Produce messages, none should be delivered
      {:ok, []} = SlotMessageStore.produce(consumer.id, 2, self())
    end

    test "if the pid changes between calls of produce_messages, produced_messages are available for deliver", %{
      consumer: consumer
    } do
      messages = [
        ConsumersFactory.consumer_message(),
        ConsumersFactory.consumer_message()
      ]

      # Put messages in store
      :ok = SlotMessageStore.put_messages(consumer.id, messages)

      # Produce messages, none should be delivered
      {:ok, delivered} = SlotMessageStore.produce(consumer.id, 2, self())
      assert length(delivered) == 2
      {:ok, []} = SlotMessageStore.produce(consumer.id, 2, self())

      {:ok, delivered} = SlotMessageStore.produce(consumer.id, 2, Factory.pid())
      assert length(delivered) == 2
    end
  end
end

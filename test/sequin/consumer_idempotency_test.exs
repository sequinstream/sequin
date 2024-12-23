defmodule Sequin.ConsumerIdempotencyTest do
  use ExUnit.Case, async: true

  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.ConsumersRuntime.ConsumerIdempotency
  alias Sequin.Factory

  setup do
    [consumer_id: Factory.uuid()]
  end

  describe "mark_messages_delivered/2" do
    test "marks messages as delivered", %{consumer_id: consumer_id} do
      messages = [
        %ConsumerRecord{seq: 1},
        %ConsumerRecord{seq: 2},
        %ConsumerRecord{seq: 3}
      ]

      assert :ok = ConsumerIdempotency.mark_messages_delivered(consumer_id, messages)

      {:ok, delivered} = ConsumerIdempotency.delivered_messages(consumer_id, messages)
      assert delivered == [1, 2, 3]
    end
  end

  describe "delivered_messages/2" do
    test "returns only delivered message sequences", %{consumer_id: consumer_id} do
      delivered_messages = [
        %ConsumerRecord{seq: 1},
        %ConsumerRecord{seq: 2}
      ]

      undelivered_message = %ConsumerRecord{seq: 3}

      :ok = ConsumerIdempotency.mark_messages_delivered(consumer_id, delivered_messages)

      {:ok, delivered} =
        ConsumerIdempotency.delivered_messages(
          consumer_id,
          delivered_messages ++ [undelivered_message]
        )

      assert delivered == [1, 2]
    end
  end

  describe "trim/2" do
    test "removes messages up to the specified sequence", %{consumer_id: consumer_id} do
      messages = [
        %ConsumerRecord{seq: 1},
        %ConsumerRecord{seq: 2},
        %ConsumerRecord{seq: 3}
      ]

      :ok = ConsumerIdempotency.mark_messages_delivered(consumer_id, messages)
      :ok = ConsumerIdempotency.trim(consumer_id, 2)

      {:ok, delivered} = ConsumerIdempotency.delivered_messages(consumer_id, messages)
      assert delivered == [3]
    end
  end
end

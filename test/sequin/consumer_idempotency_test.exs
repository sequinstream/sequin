defmodule Sequin.ConsumerIdempotencyTest do
  use ExUnit.Case, async: true

  alias Sequin.ConsumersRuntime.ConsumerIdempotency
  alias Sequin.Factory

  setup do
    [consumer_id: Factory.uuid()]
  end

  describe "mark_messages_delivered/2" do
    test "marks messages as delivered", %{consumer_id: consumer_id} do
      delivered_tuples = [{1, 0}, {1, 1}, {3, 0}, {3, 1}]

      assert :ok = ConsumerIdempotency.mark_messages_delivered(consumer_id, delivered_tuples)

      {:ok, delivered} = ConsumerIdempotency.delivered_messages(consumer_id, delivered_tuples)
      assert delivered == delivered_tuples
    end
  end

  describe "delivered_messages/2" do
    test "returns only delivered message tuples", %{consumer_id: consumer_id} do
      delivered_tuples = [{1, 0}, {1, 1}, {3, 0}]
      undelivered_tuples = [{3, 1}, {5, 0}, {5, 1}]

      :ok = ConsumerIdempotency.mark_messages_delivered(consumer_id, delivered_tuples)

      {:ok, delivered} =
        ConsumerIdempotency.delivered_messages(
          consumer_id,
          delivered_tuples ++ undelivered_tuples
        )

      assert delivered == delivered_tuples
    end
  end

  describe "trim/2" do
    test "removes messages up to the specified sequence", %{consumer_id: consumer_id} do
      delivered_tuples = [{1, 0}, {1, 1}, {3, 0}, {3, 1}]

      :ok = ConsumerIdempotency.mark_messages_delivered(consumer_id, delivered_tuples)
      assert {:ok, delivered_tuples} == ConsumerIdempotency.delivered_messages(consumer_id, delivered_tuples)

      {:ok, 2} = ConsumerIdempotency.trim(consumer_id, {3, 0})

      {:ok, delivered} = ConsumerIdempotency.delivered_messages(consumer_id, delivered_tuples)
      assert delivered == [{3, 0}, {3, 1}]
    end
  end
end

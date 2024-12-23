defmodule Sequin.ConsumerIdempotencyTest do
  use ExUnit.Case, async: true

  alias Sequin.ConsumersRuntime.ConsumerIdempotency
  alias Sequin.Factory

  setup do
    [consumer_id: Factory.uuid()]
  end

  describe "mark_messages_delivered/2" do
    test "marks messages as delivered", %{consumer_id: consumer_id} do
      delivered_seqs = [1, 2, 3]

      assert :ok = ConsumerIdempotency.mark_messages_delivered(consumer_id, delivered_seqs)

      {:ok, delivered} = ConsumerIdempotency.delivered_messages(consumer_id, delivered_seqs)
      assert delivered == delivered_seqs
    end
  end

  describe "delivered_messages/2" do
    test "returns only delivered message sequences", %{consumer_id: consumer_id} do
      delivered_seqs = [1, 2]
      undelivered_seqs = [3]

      :ok = ConsumerIdempotency.mark_messages_delivered(consumer_id, delivered_seqs)

      {:ok, delivered} =
        ConsumerIdempotency.delivered_messages(
          consumer_id,
          delivered_seqs ++ undelivered_seqs
        )

      assert delivered == delivered_seqs
    end
  end

  describe "trim/2" do
    test "removes messages up to the specified sequence", %{consumer_id: consumer_id} do
      delivered_seqs = [1, 2, 3]

      :ok = ConsumerIdempotency.mark_messages_delivered(consumer_id, delivered_seqs)
      :ok = ConsumerIdempotency.trim(consumer_id, 2)

      {:ok, delivered} = ConsumerIdempotency.delivered_messages(consumer_id, delivered_seqs)
      assert delivered == [3]
    end
  end
end

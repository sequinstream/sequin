defmodule Sequin.ConsumerTest do
  use Sequin.Case, async: true

  alias Sequin.Streams.Consumer

  describe "should_delete_acked_messages?/2" do
    test "returns false if backfill_completed_at is nil" do
      refute Consumer.should_delete_acked_messages?(%Consumer{backfill_completed_at: nil})
    end

    test "returns true if backfill_completed_at is a while ago" do
      one_day_ago = DateTime.add(DateTime.utc_now(), -24, :hour)
      assert Consumer.should_delete_acked_messages?(%Consumer{backfill_completed_at: one_day_ago})
    end
  end
end

defmodule Sequin.ConsumerTest do
  use Sequin.Case, async: true

  alias Sequin.Streams.Consumer

  describe "filter_matches_subject?/2" do
    test "exact match returns true" do
      assert Consumer.filter_matches_subject?("a.b.c", "a.b.c")
    end

    test "trailing > works for subjects longer than the filter subject" do
      assert Consumer.filter_matches_subject?("a.b.>", "a.b.c.d.e")
      assert Consumer.filter_matches_subject?("a.b.>", "a.b.c")
    end

    test "trailing > does not match shorter subjects" do
      refute Consumer.filter_matches_subject?("a.b.>", "a.b")
    end

    test "wildcard * matches any single token" do
      assert Consumer.filter_matches_subject?("a.*.c", "a.b.c")
      assert Consumer.filter_matches_subject?("*.b.*", "a.b.c")
    end

    test "multiple wildcards work" do
      assert Consumer.filter_matches_subject?("*.*.*", "a.b.c")
      assert Consumer.filter_matches_subject?("a.*.c.*", "a.b.c.d")
    end

    test "non-matching subjects return false" do
      refute Consumer.filter_matches_subject?("a.b.c", "a.b.d")
      refute Consumer.filter_matches_subject?("a.b.c", "x.y.z")
    end

    test "different length subjects without > return false" do
      refute Consumer.filter_matches_subject?("a.b", "a.b.c")
      refute Consumer.filter_matches_subject?("a.b.c.d", "a.b.c")
    end

    test "combination of wildcards and exact matches" do
      assert Consumer.filter_matches_subject?("a.*.c.>", "a.b.c.d.e")
      assert Consumer.filter_matches_subject?("*.b.*.d", "a.b.c.d")
      refute Consumer.filter_matches_subject?("a.*.c.*", "a.b.c.d.e")
    end
  end

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

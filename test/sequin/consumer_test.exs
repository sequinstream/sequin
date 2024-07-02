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
end

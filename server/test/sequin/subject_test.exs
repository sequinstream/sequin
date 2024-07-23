defmodule Sequin.KeyTest do
  use Sequin.Case, async: true

  alias Sequin.Key

  describe "matches?/2" do
    test "exact match returns true" do
      assert Key.matches?("a.b.c", "a.b.c")
    end

    test "trailing > works for keys longer than the filter key" do
      assert Key.matches?("a.b.>", "a.b.c.d.e")
      assert Key.matches?("a.b.>", "a.b.c")
    end

    test "trailing > does not match shorter keys" do
      refute Key.matches?("a.b.>", "a.b")
    end

    test "wildcard * matches any single token" do
      assert Key.matches?("a.*.c", "a.b.c")
      assert Key.matches?("*.b.*", "a.b.c")
    end

    test "multiple wildcards work" do
      assert Key.matches?("*.*.*", "a.b.c")
      assert Key.matches?("a.*.c.*", "a.b.c.d")
    end

    test "non-matching keys return false" do
      refute Key.matches?("a.b.c", "a.b.d")
      refute Key.matches?("a.b.c", "x.y.z")
    end

    test "different length keys without > return false" do
      refute Key.matches?("a.b", "a.b.c")
      refute Key.matches?("a.b.c.d", "a.b.c")
    end

    test "combination of wildcards and exact matches" do
      assert Key.matches?("a.*.c.>", "a.b.c.d.e")
      assert Key.matches?("*.b.*.d", "a.b.c.d")
      refute Key.matches?("a.*.c.*", "a.b.c.d.e")
    end
  end
end

defmodule Sequin.SubjectTest do
  use Sequin.Case, async: true

  alias Sequin.Subject

  describe "matches?/2" do
    test "exact match returns true" do
      assert Subject.matches?("a.b.c", "a.b.c")
    end

    test "trailing > works for subjects longer than the filter subject" do
      assert Subject.matches?("a.b.>", "a.b.c.d.e")
      assert Subject.matches?("a.b.>", "a.b.c")
    end

    test "trailing > does not match shorter subjects" do
      refute Subject.matches?("a.b.>", "a.b")
    end

    test "wildcard * matches any single token" do
      assert Subject.matches?("a.*.c", "a.b.c")
      assert Subject.matches?("*.b.*", "a.b.c")
    end

    test "multiple wildcards work" do
      assert Subject.matches?("*.*.*", "a.b.c")
      assert Subject.matches?("a.*.c.*", "a.b.c.d")
    end

    test "non-matching subjects return false" do
      refute Subject.matches?("a.b.c", "a.b.d")
      refute Subject.matches?("a.b.c", "x.y.z")
    end

    test "different length subjects without > return false" do
      refute Subject.matches?("a.b", "a.b.c")
      refute Subject.matches?("a.b.c.d", "a.b.c")
    end

    test "combination of wildcards and exact matches" do
      assert Subject.matches?("a.*.c.>", "a.b.c.d.e")
      assert Subject.matches?("*.b.*.d", "a.b.c.d")
      refute Subject.matches?("a.*.c.*", "a.b.c.d.e")
    end
  end
end

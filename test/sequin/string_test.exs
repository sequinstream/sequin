defmodule Sequin.StringTest do
  use Sequin.Case, async: true

  alias Sequin.String, as: S

  describe "truncate_with_hash/2" do
    test "returns nil for nil input" do
      assert S.truncate_with_hash(nil, 256) == nil
    end

    test "returns string unchanged when under limit" do
      assert S.truncate_with_hash("short", 256) == "short"
      assert S.truncate_with_hash("user:123", 256) == "user:123"
    end

    test "returns string unchanged when exactly at limit" do
      string = String.duplicate("a", 256)
      assert S.truncate_with_hash(string, 256) == string
    end

    test "returns MD5 hash when over limit" do
      string = String.duplicate("a", 257)
      result = S.truncate_with_hash(string, 256)

      # MD5 hash encoded as hex is 32 characters
      assert byte_size(result) == 32
      assert result != string
    end

    test "hash is consistent (same input produces same output)" do
      string = String.duplicate("a", 300)
      hash1 = S.truncate_with_hash(string, 256)
      hash2 = S.truncate_with_hash(string, 256)

      assert hash1 == hash2
    end

    test "different inputs produce different hashes" do
      string1 = String.duplicate("a", 300)
      string2 = String.duplicate("b", 300)

      hash1 = S.truncate_with_hash(string1, 256)
      hash2 = S.truncate_with_hash(string2, 256)

      assert hash1 != hash2
    end
  end
end

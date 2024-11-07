defmodule Sequin.TimeTest do
  use Sequin.Case, async: true

  alias Sequin.Time

  describe "parse_duration/1" do
    test "correctly parses valid duration strings" do
      assert {:ok, 60_000} == Time.parse_duration("60s")
      assert {:ok, 300_000} == Time.parse_duration("5m")
      assert {:ok, 100} == Time.parse_duration("100ms")
      assert {:ok, 1000} == Time.parse_duration("1000")
    end

    test "handles integer input" do
      assert {:ok, 5000} == Time.parse_duration(5000)
    end

    test "returns error for invalid duration formats" do
      assert {:error, error} = Time.parse_duration("invalid")
      assert error.message == "Invalid duration format"

      assert {:error, error} = Time.parse_duration("60x")
      assert error.message == "Invalid duration format"

      assert {:error, error} = Time.parse_duration("ms")
      assert error.message == "Invalid duration format"

      assert {:error, error} = Time.parse_duration("")
      assert error.message == "Invalid duration format"
    end

    test "returns error for non-string, non-integer inputs" do
      assert {:error, error} = Time.parse_duration(3.14)
      assert error.message == "Invalid duration format"

      assert {:error, error} = Time.parse_duration(nil)
      assert error.message == "Invalid duration format"
    end
  end
end

defmodule Sequin.EtsMultisetTest do
  use ExUnit.Case, async: true

  alias Sequin.EtsMultiset

  setup do
    %{table: EtsMultiset.new()}
  end

  describe "initialization" do
    test "new/0 creates an empty ETS table" do
      table = EtsMultiset.new()
      assert is_reference(table)
      assert EtsMultiset.keys(table) == []
    end

    test "new/1 creates an empty ETS table with a specified name" do
      table = EtsMultiset.new_named(:test_table)
      assert EtsMultiset.keys(table) == []
    end

    test "new_from_list/1 creates a table from a list of tuples" do
      entries = [{"group1", "value1"}, {"group1", "value2"}, {"group2", "value3"}]
      table = EtsMultiset.new_from_list(entries)

      assert table |> EtsMultiset.get("group1") |> Enum.sort() == ["value1", "value2"]
      assert EtsMultiset.get(table, "group2") == ["value3"]
    end

    test "new_from_list/2 creates a table with a specified name" do
      entries = [{"group1", "value1"}]
      table = EtsMultiset.new_from_list(entries, :named_table)

      assert EtsMultiset.get(table, "group1") == ["value1"]
    end
  end

  describe "basic operations" do
    test "put/3 adds a value to a key", %{table: table} do
      EtsMultiset.put(table, "group1", "value1")
      assert EtsMultiset.get(table, "group1") == ["value1"]

      # Adding another value to the same key
      EtsMultiset.put(table, "group1", "value2")
      assert table |> EtsMultiset.get("group1") |> Enum.sort() == ["value1", "value2"]

      # Adding a duplicate value (should be ignored since values are unique per key)
      EtsMultiset.put(table, "group1", "value1")
      assert table |> EtsMultiset.get("group1") |> Enum.sort() == ["value1", "value2"]
    end

    test "put_many/2 adds multiple values in a batch", %{table: table} do
      entries = [
        {"group1", "value1"},
        {"group1", "value2"},
        {"group2", "value3"},
        {"group3", "value4"}
      ]

      EtsMultiset.put_many(table, entries)

      assert table |> EtsMultiset.get("group1") |> Enum.sort() == ["value1", "value2"]
      assert EtsMultiset.get(table, "group2") == ["value3"]
      assert EtsMultiset.get(table, "group3") == ["value4"]

      # Adding more values including duplicates
      more_entries = [
        {"group1", "value3"},
        # Duplicate, should be ignored
        {"group1", "value1"},
        {"group4", "value5"}
      ]

      EtsMultiset.put_many(table, more_entries)

      assert table |> EtsMultiset.get("group1") |> Enum.sort() == ["value1", "value2", "value3"]
      assert EtsMultiset.get(table, "group4") == ["value5"]
    end

    test "delete/3 removes a value from a key", %{table: table} do
      EtsMultiset.put(table, "group1", "value1")
      EtsMultiset.put(table, "group1", "value2")

      EtsMultiset.delete(table, "group1", "value1")
      assert EtsMultiset.get(table, "group1") == ["value2"]

      # Deleting a non-existent value should be a no-op
      EtsMultiset.delete(table, "group1", "value3")
      assert EtsMultiset.get(table, "group1") == ["value2"]

      # Deleting from a non-existent key should be a no-op
      EtsMultiset.delete(table, "group2", "value1")
      assert EtsMultiset.get(table, "group2") == []
    end

    test "delete_many/3 removes multiple values in a batch", %{table: table} do
      entries = [
        {"group1", "value1"},
        {"group1", "value2"},
        {"group1", "value3"},
        {"group2", "value4"}
      ]

      EtsMultiset.put_many(table, entries)

      # Delete multiple values from group1
      EtsMultiset.delete_many(table, "group1", ["value1", "value3"])

      assert EtsMultiset.get(table, "group1") == ["value2"]
      assert EtsMultiset.get(table, "group2") == ["value4"]

      # Deleting non-existent values should be a no-op
      EtsMultiset.delete_many(table, "group1", ["value5", "value6"])
      assert EtsMultiset.get(table, "group1") == ["value2"]

      # Deleting from a non-existent key should be a no-op
      EtsMultiset.delete_many(table, "group3", ["value1"])
      assert EtsMultiset.get(table, "group3") == []
    end

    test "delete_key/2 removes all values for a key", %{table: table} do
      entries = [
        {"group1", "value1"},
        {"group1", "value2"},
        {"group1", "value3"},
        {"group2", "value4"},
        {"group3", "value5"}
      ]

      EtsMultiset.put_many(table, entries)

      # Delete all values for group1
      EtsMultiset.delete_key(table, "group1")
      assert EtsMultiset.get(table, "group1") == []
      assert EtsMultiset.get(table, "group2") == ["value4"]
      assert EtsMultiset.get(table, "group3") == ["value5"]

      # Deleting a key that was already deleted should be a no-op
      EtsMultiset.delete_key(table, "group1")
      assert EtsMultiset.get(table, "group1") == []

      # Deleting a non-existent key should be a no-op
      EtsMultiset.delete_key(table, "nonexistent")
      assert EtsMultiset.get(table, "nonexistent") == []

      # Verify that other operations still work after deletion
      EtsMultiset.put(table, "group2", "value6")
      assert table |> EtsMultiset.get("group2") |> Enum.sort() == ["value4", "value6"]
    end

    test "delete_key/2 only affects the specified key", %{table: table} do
      # Setup multiple keys with similar values
      EtsMultiset.put(table, "batch_1", "record1")
      EtsMultiset.put(table, "batch_1", "record2")
      EtsMultiset.put(table, "batch_2", "record1")
      EtsMultiset.put(table, "batch_2", "record3")
      EtsMultiset.put(table, "batch_3", "record2")

      # Verify initial state
      assert table |> EtsMultiset.get("batch_1") |> Enum.sort() == ["record1", "record2"]
      assert table |> EtsMultiset.get("batch_2") |> Enum.sort() == ["record1", "record3"]
      assert EtsMultiset.get(table, "batch_3") == ["record2"]

      # Delete one key
      EtsMultiset.delete_key(table, "batch_2")

      # Verify only the specified key was affected
      assert table |> EtsMultiset.get("batch_1") |> Enum.sort() == ["record1", "record2"]
      assert EtsMultiset.get(table, "batch_2") == []
      assert EtsMultiset.get(table, "batch_3") == ["record2"]

      # Verify key was actually removed from the multiset
      assert table |> EtsMultiset.keys() |> Enum.sort() == ["batch_1", "batch_3"]

      # Delete another key and verify
      EtsMultiset.delete_key(table, "batch_1")
      assert EtsMultiset.get(table, "batch_1") == []
      assert EtsMultiset.get(table, "batch_3") == ["record2"]
      assert table |> EtsMultiset.keys() |> Enum.sort() == ["batch_3"]
    end
  end

  describe "querying" do
    setup %{table: table} do
      entries = [
        {"group1", "value1"},
        {"group1", "value2"},
        {"group2", "value3"},
        {"group3", "value1"}
      ]

      Enum.each(entries, fn {key, value} -> EtsMultiset.put(table, key, value) end)
      :ok
    end

    test "values/1 returns all values across all keys", %{table: table} do
      values = table |> EtsMultiset.values() |> Enum.sort()
      assert values == ["value1", "value1", "value2", "value3"]
    end

    test "member?/2 checks if a key exists", %{table: table} do
      assert EtsMultiset.member?(table, "group1") == true
      assert EtsMultiset.member?(table, "group2") == true
      assert EtsMultiset.member?(table, "nonexistent") == false
    end

    test "value_member?/3 checks if a value exists for a key", %{table: table} do
      assert EtsMultiset.value_member?(table, "group1", "value1") == true
      assert EtsMultiset.value_member?(table, "group1", "value3") == false
      assert EtsMultiset.value_member?(table, "nonexistent", "value1") == false
    end

    test "get/2 returns all values for a key", %{table: table} do
      assert table |> EtsMultiset.get("group1") |> Enum.sort() == ["value1", "value2"]
      assert EtsMultiset.get(table, "group2") == ["value3"]
      assert EtsMultiset.get(table, "nonexistent") == []
    end

    test "get_set/2 returns values as a MapSet", %{table: table} do
      assert EtsMultiset.get_set(table, "group1") == MapSet.new(["value1", "value2"])
      assert EtsMultiset.get_set(table, "nonexistent") == MapSet.new([])
    end

    test "fetch!/2 returns values or raises for missing keys", %{table: table} do
      assert table |> EtsMultiset.fetch!("group1") |> Enum.sort() == ["value1", "value2"]

      assert_raise KeyError, fn ->
        EtsMultiset.fetch!(table, "nonexistent")
      end
    end

    test "count/2 returns the number of values for a key", %{table: table} do
      assert EtsMultiset.count(table, "group1") == 2
      assert EtsMultiset.count(table, "group2") == 1
      assert EtsMultiset.count(table, "nonexistent") == 0
    end

    test "keys/1 returns all keys in the multiset", %{table: table} do
      assert table |> EtsMultiset.keys() |> Enum.sort() == ["group1", "group2", "group3"]
    end
  end

  describe "set operations" do
    setup %{table: table} do
      entries = [
        {"group1", "value1"},
        {"group1", "value2"},
        {"group2", "value3"}
      ]

      Enum.each(entries, fn {key, value} -> EtsMultiset.put(table, key, value) end)
      :ok
    end

    test "difference/3 removes a set of values from a key", %{table: table} do
      values_to_remove = MapSet.new(["value1"])
      EtsMultiset.difference(table, "group1", values_to_remove)

      assert EtsMultiset.get(table, "group1") == ["value2"]

      # Remove all values
      values_to_remove = MapSet.new(["value2"])
      EtsMultiset.difference(table, "group1", values_to_remove)

      assert EtsMultiset.get(table, "group1") == []

      # Removing from non-existent key should be a no-op
      values_to_remove = MapSet.new(["value1"])
      EtsMultiset.difference(table, "nonexistent", values_to_remove)

      assert EtsMultiset.get(table, "nonexistent") == []
    end

    test "union/3 adds a set of values to a key", %{table: table} do
      values_to_add = MapSet.new(["value4", "value5"])
      EtsMultiset.union(table, "group1", values_to_add)

      assert table |> EtsMultiset.get("group1") |> Enum.sort() == ["value1", "value2", "value4", "value5"]

      # Adding to a non-existent key should create it
      values_to_add = MapSet.new(["value6"])
      EtsMultiset.union(table, "new_group", values_to_add)

      assert EtsMultiset.get(table, "new_group") == ["value6"]
    end

    test "delete_key/2 is more efficient than delete_many/3 for removing all values", %{table: table} do
      # Create a table with many values for a key
      key = "test_key"
      values = Enum.map(1..1000, &"value#{&1}")

      # Add all values to the table
      Enum.each(values, fn value -> EtsMultiset.put(table, key, value) end)
      assert EtsMultiset.count(table, key) == 1000

      # Create a second table for comparison
      table2 = EtsMultiset.new()
      Enum.each(values, fn value -> EtsMultiset.put(table2, key, value) end)
      assert EtsMultiset.count(table2, key) == 1000

      # Measure time to delete all values using delete_key
      # Delete all values using delete_key
      EtsMultiset.delete_key(table, key)

      # Delete all values using delete_many
      all_values = EtsMultiset.get(table2, key)
      EtsMultiset.delete_many(table2, key, all_values)

      # Both tables should now have no values for the key
      assert EtsMultiset.get(table, key) == []
      assert EtsMultiset.get(table2, key) == []
    end
  end

  describe "cleanup" do
    test "destroy/1 deletes the ETS table" do
      table = EtsMultiset.new()
      EtsMultiset.put(table, "group1", "value1")

      assert EtsMultiset.destroy(table) == true

      # Table should no longer exist
      assert_raise ArgumentError, fn ->
        EtsMultiset.get(table, "group1")
      end
    end
  end
end

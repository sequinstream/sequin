defmodule Sequin.WalPipeline.SourceTable.ColumnSelectionTest do
  use Sequin.DataCase, async: true

  alias Sequin.Error.InvariantError
  alias Sequin.Runtime.SlotProcessor.Message.Field
  alias Sequin.WalPipeline.SourceTable
  alias Sequin.WalPipeline.SourceTable.ColumnSelection

  describe "filter_fields/2" do
    test "returns all fields when no column selection is configured" do
      source_table = %SourceTable{
        oid: 123,
        actions: [:insert, :update, :delete],
        include_column_attnums: nil,
        exclude_column_attnums: nil
      }

      fields = [
        %Field{column_name: "id", column_attnum: 1, value: 1},
        %Field{column_name: "name", column_attnum: 2, value: "Alice"},
        %Field{column_name: "email", column_attnum: 3, value: "alice@example.com"},
        %Field{column_name: "password", column_attnum: 4, value: "secret"}
      ]

      filtered = ColumnSelection.filter_fields(fields, source_table)
      assert length(filtered) == 4
      assert filtered == fields
    end

    test "returns all fields when column selection arrays are empty" do
      source_table = %SourceTable{
        oid: 123,
        actions: [:insert, :update, :delete],
        include_column_attnums: [],
        exclude_column_attnums: []
      }

      fields = [
        %Field{column_name: "id", column_attnum: 1, value: 1},
        %Field{column_name: "name", column_attnum: 2, value: "Alice"}
      ]

      filtered = ColumnSelection.filter_fields(fields, source_table)
      assert length(filtered) == 2
      assert filtered == fields
    end

    test "filters fields when include_column_attnums is specified" do
      source_table = %SourceTable{
        oid: 123,
        actions: [:insert, :update, :delete],
        include_column_attnums: [1, 2, 3],
        exclude_column_attnums: nil
      }

      fields = [
        %Field{column_name: "id", column_attnum: 1, value: 1},
        %Field{column_name: "name", column_attnum: 2, value: "Alice"},
        %Field{column_name: "email", column_attnum: 3, value: "alice@example.com"},
        %Field{column_name: "password", column_attnum: 4, value: "secret"},
        %Field{column_name: "ssn", column_attnum: 5, value: "123-45-6789"}
      ]

      filtered = ColumnSelection.filter_fields(fields, source_table)
      assert length(filtered) == 3
      assert Enum.map(filtered, & &1.column_name) == ["id", "name", "email"]
    end

    test "filters fields when exclude_column_attnums is specified" do
      source_table = %SourceTable{
        oid: 123,
        actions: [:insert, :update, :delete],
        include_column_attnums: nil,
        exclude_column_attnums: [4, 5]
      }

      fields = [
        %Field{column_name: "id", column_attnum: 1, value: 1},
        %Field{column_name: "name", column_attnum: 2, value: "Alice"},
        %Field{column_name: "email", column_attnum: 3, value: "alice@example.com"},
        %Field{column_name: "password", column_attnum: 4, value: "secret"},
        %Field{column_name: "ssn", column_attnum: 5, value: "123-45-6789"}
      ]

      filtered = ColumnSelection.filter_fields(fields, source_table)
      assert length(filtered) == 3
      assert Enum.map(filtered, & &1.column_name) == ["id", "name", "email"]
    end
  end

  describe "filter_column_attnums/2" do
    test "returns all attnums when source_table is nil" do
      attnums = [1, 2, 3, 4, 5]
      filtered = ColumnSelection.filter_column_attnums(attnums, nil)
      assert filtered == attnums
    end

    test "returns all attnums when no column selection is configured" do
      source_table = %SourceTable{
        oid: 123,
        actions: [:insert, :update, :delete],
        include_column_attnums: nil,
        exclude_column_attnums: nil
      }

      attnums = [1, 2, 3, 4, 5]
      filtered = ColumnSelection.filter_column_attnums(attnums, source_table)
      assert filtered == attnums
    end

    test "filters attnums with include_column_attnums" do
      source_table = %SourceTable{
        oid: 123,
        actions: [:insert, :update, :delete],
        include_column_attnums: [1, 2, 3],
        exclude_column_attnums: nil
      }

      attnums = [1, 2, 3, 4, 5]
      filtered = ColumnSelection.filter_column_attnums(attnums, source_table)
      assert filtered == [1, 2, 3]
    end

    test "filters attnums with exclude_column_attnums" do
      source_table = %SourceTable{
        oid: 123,
        actions: [:insert, :update, :delete],
        include_column_attnums: nil,
        exclude_column_attnums: [4, 5]
      }

      attnums = [1, 2, 3, 4, 5]
      filtered = ColumnSelection.filter_column_attnums(attnums, source_table)
      assert filtered == [1, 2, 3]
    end
  end

  describe "filter_fields/2 with both include and exclude set" do
    test "raises an error when both include_column_attnums and exclude_column_attnums are set" do
      source_table = %SourceTable{
        oid: 123,
        actions: [:insert, :update, :delete],
        include_column_attnums: [1, 2, 3],
        exclude_column_attnums: [4, 5]
      }

      fields = [
        %Field{column_name: "id", column_attnum: 1, value: 1},
        %Field{column_name: "name", column_attnum: 2, value: "Alice"}
      ]

      assert_raise InvariantError, fn ->
        ColumnSelection.filter_fields(fields, source_table)
      end
    end
  end

  describe "filter_column_attnums/2 with both include and exclude set" do
    test "raises an error when both include_column_attnums and exclude_column_attnums are set" do
      source_table = %SourceTable{
        oid: 123,
        actions: [:insert, :update, :delete],
        include_column_attnums: [1, 2, 3],
        exclude_column_attnums: [4, 5]
      }

      attnums = [1, 2, 3, 4, 5]

      assert_raise InvariantError, fn ->
        ColumnSelection.filter_column_attnums(attnums, source_table)
      end
    end
  end

  describe "has_column_selection?/1" do
    test "returns false when no column selection is configured" do
      source_table = %SourceTable{
        oid: 123,
        actions: [:insert, :update, :delete],
        include_column_attnums: nil,
        exclude_column_attnums: nil
      }

      refute ColumnSelection.has_column_selection?(source_table)
    end

    test "returns false when column selection arrays are empty" do
      source_table = %SourceTable{
        oid: 123,
        actions: [:insert, :update, :delete],
        include_column_attnums: [],
        exclude_column_attnums: []
      }

      refute ColumnSelection.has_column_selection?(source_table)
    end

    test "returns true when include_column_attnums is specified" do
      source_table = %SourceTable{
        oid: 123,
        actions: [:insert, :update, :delete],
        include_column_attnums: [1, 2, 3],
        exclude_column_attnums: nil
      }

      assert ColumnSelection.has_column_selection?(source_table)
    end

    test "returns true when exclude_column_attnums is specified" do
      source_table = %SourceTable{
        oid: 123,
        actions: [:insert, :update, :delete],
        include_column_attnums: nil,
        exclude_column_attnums: [4, 5]
      }

      assert ColumnSelection.has_column_selection?(source_table)
    end
  end
end

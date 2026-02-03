defmodule Sequin.WalPipeline.SourceTableTest do
  use Sequin.DataCase, async: true

  alias Sequin.WalPipeline.SourceTable

  describe "changeset/2" do
    test "accepts valid source table without column selection" do
      attrs = %{
        oid: 123,
        actions: [:insert, :update, :delete]
      }

      changeset = SourceTable.changeset(%SourceTable{}, attrs)
      assert changeset.valid?
    end

    test "accepts valid source table with include_column_attnums" do
      attrs = %{
        oid: 123,
        actions: [:insert, :update, :delete],
        include_column_attnums: [1, 2, 3]
      }

      changeset = SourceTable.changeset(%SourceTable{}, attrs)
      assert changeset.valid?
    end

    test "accepts valid source table with exclude_column_attnums" do
      attrs = %{
        oid: 123,
        actions: [:insert, :update, :delete],
        exclude_column_attnums: [4, 5]
      }

      changeset = SourceTable.changeset(%SourceTable{}, attrs)
      assert changeset.valid?
    end

    test "accepts empty arrays for column selection" do
      attrs = %{
        oid: 123,
        actions: [:insert, :update, :delete],
        include_column_attnums: [],
        exclude_column_attnums: []
      }

      changeset = SourceTable.changeset(%SourceTable{}, attrs)
      assert changeset.valid?
    end

    test "rejects when both include_column_attnums and exclude_column_attnums are set" do
      attrs = %{
        oid: 123,
        actions: [:insert, :update, :delete],
        include_column_attnums: [1, 2, 3],
        exclude_column_attnums: [4, 5]
      }

      changeset = SourceTable.changeset(%SourceTable{}, attrs)
      refute changeset.valid?
      assert "cannot be set when include_column_attnums is set" in errors_on(changeset).exclude_column_attnums
    end

    test "accepts when one list is empty and the other is not" do
      attrs = %{
        oid: 123,
        actions: [:insert, :update, :delete],
        include_column_attnums: [1, 2, 3],
        exclude_column_attnums: []
      }

      changeset = SourceTable.changeset(%SourceTable{}, attrs)
      assert changeset.valid?
    end
  end

  describe "JSON encoding" do
    test "includes column selection fields in JSON output" do
      source_table = %SourceTable{
        oid: 123,
        schema_name: "public",
        table_name: "users",
        actions: [:insert, :update],
        include_column_attnums: [1, 2, 3],
        exclude_column_attnums: nil,
        column_filters: []
      }

      json = Jason.encode!(source_table)
      decoded = Jason.decode!(json)

      assert decoded["oid"] == 123
      assert decoded["include_column_attnums"] == [1, 2, 3]
      assert decoded["exclude_column_attnums"] == nil
    end
  end
end

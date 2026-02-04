defmodule Sequin.Consumers.ColumnFilteringTest do
  use Sequin.DataCase, async: true

  alias Sequin.Consumers
  alias Sequin.Runtime.SlotProcessor.Message
  alias Sequin.Runtime.SlotProcessor.Message.Field
  alias Sequin.WalPipeline.SourceTable

  describe "message_record/2 with column filtering" do
    test "returns all columns when source_table is nil" do
      message = %Message{
        action: :insert,
        table_oid: 123,
        fields: [
          %Field{column_name: "id", column_attnum: 1, value: 1},
          %Field{column_name: "name", column_attnum: 2, value: "Alice"},
          %Field{column_name: "email", column_attnum: 3, value: "alice@example.com"},
          %Field{column_name: "password", column_attnum: 4, value: "secret"}
        ]
      }

      record = Consumers.message_record(message, nil)

      assert map_size(record) == 4
      assert record["id"] == 1
      assert record["name"] == "Alice"
      assert record["email"] == "alice@example.com"
      assert record["password"] == "secret"
    end

    test "returns only included columns when include_column_attnums is set" do
      source_table = %SourceTable{
        oid: 123,
        actions: [:insert, :update, :delete],
        include_column_attnums: [1, 2, 3]
      }

      message = %Message{
        action: :insert,
        table_oid: 123,
        fields: [
          %Field{column_name: "id", column_attnum: 1, value: 1},
          %Field{column_name: "name", column_attnum: 2, value: "Alice"},
          %Field{column_name: "email", column_attnum: 3, value: "alice@example.com"},
          %Field{column_name: "password", column_attnum: 4, value: "secret"},
          %Field{column_name: "ssn", column_attnum: 5, value: "123-45-6789"}
        ]
      }

      record = Consumers.message_record(message, source_table)

      assert map_size(record) == 3
      assert record["id"] == 1
      assert record["name"] == "Alice"
      assert record["email"] == "alice@example.com"
      refute Map.has_key?(record, "password")
      refute Map.has_key?(record, "ssn")
    end

    test "excludes specified columns when exclude_column_attnums is set" do
      source_table = %SourceTable{
        oid: 123,
        actions: [:insert, :update, :delete],
        exclude_column_attnums: [4, 5]
      }

      message = %Message{
        action: :insert,
        table_oid: 123,
        fields: [
          %Field{column_name: "id", column_attnum: 1, value: 1},
          %Field{column_name: "name", column_attnum: 2, value: "Alice"},
          %Field{column_name: "email", column_attnum: 3, value: "alice@example.com"},
          %Field{column_name: "password", column_attnum: 4, value: "secret"},
          %Field{column_name: "ssn", column_attnum: 5, value: "123-45-6789"}
        ]
      }

      record = Consumers.message_record(message, source_table)

      assert map_size(record) == 3
      assert record["id"] == 1
      assert record["name"] == "Alice"
      assert record["email"] == "alice@example.com"
      refute Map.has_key?(record, "password")
      refute Map.has_key?(record, "ssn")
    end
  end

  describe "message_changes/2 with column filtering" do
    test "returns nil for insert action" do
      source_table = %SourceTable{
        oid: 123,
        actions: [:insert, :update, :delete],
        exclude_column_attnums: [4]
      }

      message = %Message{
        action: :insert,
        table_oid: 123,
        fields: [
          %Field{column_name: "id", column_attnum: 1, value: 1},
          %Field{column_name: "password", column_attnum: 4, value: "secret"}
        ]
      }

      assert Consumers.message_changes(message, source_table) == nil
    end

    test "filters changes for update action" do
      source_table = %SourceTable{
        oid: 123,
        actions: [:insert, :update, :delete],
        exclude_column_attnums: [4]
      }

      message = %Message{
        action: :update,
        table_oid: 123,
        old_fields: [
          %Field{column_name: "id", column_attnum: 1, value: 1},
          %Field{column_name: "name", column_attnum: 2, value: "Alice"},
          %Field{column_name: "email", column_attnum: 3, value: "alice@old.com"},
          %Field{column_name: "password", column_attnum: 4, value: "old_secret"}
        ],
        fields: [
          %Field{column_name: "id", column_attnum: 1, value: 1},
          %Field{column_name: "name", column_attnum: 2, value: "Alice"},
          %Field{column_name: "email", column_attnum: 3, value: "alice@new.com"},
          %Field{column_name: "password", column_attnum: 4, value: "new_secret"}
        ]
      }

      changes = Consumers.message_changes(message, source_table)

      # Only email should be in changes (password is excluded)
      assert map_size(changes) == 1
      assert changes["email"] == "alice@old.com"
      refute Map.has_key?(changes, "password")
    end

    test "filters changes with include_column_attnums" do
      source_table = %SourceTable{
        oid: 123,
        actions: [:insert, :update, :delete],
        include_column_attnums: [1, 2, 3]
      }

      message = %Message{
        action: :update,
        table_oid: 123,
        old_fields: [
          %Field{column_name: "id", column_attnum: 1, value: 1},
          %Field{column_name: "name", column_attnum: 2, value: "Alice"},
          %Field{column_name: "email", column_attnum: 3, value: "alice@old.com"},
          %Field{column_name: "password", column_attnum: 4, value: "old_secret"}
        ],
        fields: [
          %Field{column_name: "id", column_attnum: 1, value: 1},
          %Field{column_name: "name", column_attnum: 2, value: "Alice Updated"},
          %Field{column_name: "email", column_attnum: 3, value: "alice@new.com"},
          %Field{column_name: "password", column_attnum: 4, value: "new_secret"}
        ]
      }

      changes = Consumers.message_changes(message, source_table)

      assert map_size(changes) == 2
      assert changes["name"] == "Alice"
      assert changes["email"] == "alice@old.com"
      refute Map.has_key?(changes, "password")
    end
  end
end

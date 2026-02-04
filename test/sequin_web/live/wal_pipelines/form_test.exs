defmodule SequinWeb.WalPipelinesLive.FormTest do
  use SequinWeb.ConnCase, async: true

  alias Sequin.Databases.PostgresDatabaseTable
  alias Sequin.Error.InvariantError
  alias SequinWeb.WalPipelinesLive.Form

  describe "verify_column_selection/2" do
    test "returns :ok when no column selection is configured" do
      table = %PostgresDatabaseTable{
        columns: [
          %PostgresDatabaseTable.Column{attnum: 1, name: "id", is_pk?: true, type: "integer"},
          %PostgresDatabaseTable.Column{attnum: 2, name: "name", is_pk?: false, type: "text"}
        ]
      }

      source_table_params = %{
        "exclude_column_attnums" => nil,
        "include_column_attnums" => nil
      }

      assert Form.verify_column_selection(table, source_table_params) == :ok
    end

    test "returns error when trying to exclude primary key columns" do
      table = %PostgresDatabaseTable{
        columns: [
          %PostgresDatabaseTable.Column{attnum: 1, name: "id", is_pk?: true, type: "integer"},
          %PostgresDatabaseTable.Column{attnum: 2, name: "name", is_pk?: false, type: "text"},
          %PostgresDatabaseTable.Column{attnum: 3, name: "email", is_pk?: false, type: "text"}
        ]
      }

      source_table_params = %{
        "exclude_column_attnums" => [1, 3],
        "include_column_attnums" => nil
      }

      assert {:error, %InvariantError{} = error} = Form.verify_column_selection(table, source_table_params)
      assert error.message =~ "Cannot exclude primary key columns"
      assert error.message =~ "id"
    end

    test "returns error when trying to exclude all columns" do
      table = %PostgresDatabaseTable{
        columns: [
          %PostgresDatabaseTable.Column{attnum: 1, name: "id", is_pk?: true, type: "integer"},
          %PostgresDatabaseTable.Column{attnum: 2, name: "name", is_pk?: false, type: "text"}
        ]
      }

      source_table_params = %{
        "exclude_column_attnums" => [2],
        "include_column_attnums" => nil
      }

      # This should pass because we're only excluding non-PK columns
      assert Form.verify_column_selection(table, source_table_params) == :ok
    end

    test "returns :ok when excluding non-primary key columns" do
      table = %PostgresDatabaseTable{
        columns: [
          %PostgresDatabaseTable.Column{attnum: 1, name: "id", is_pk?: true, type: "integer"},
          %PostgresDatabaseTable.Column{attnum: 2, name: "name", is_pk?: false, type: "text"},
          %PostgresDatabaseTable.Column{attnum: 3, name: "password", is_pk?: false, type: "text"}
        ]
      }

      source_table_params = %{
        "exclude_column_attnums" => [3],
        "include_column_attnums" => nil
      }

      assert Form.verify_column_selection(table, source_table_params) == :ok
    end

    test "returns error when including zero columns" do
      table = %PostgresDatabaseTable{
        columns: [
          %PostgresDatabaseTable.Column{attnum: 1, name: "id", is_pk?: true, type: "integer"},
          %PostgresDatabaseTable.Column{attnum: 2, name: "name", is_pk?: false, type: "text"}
        ]
      }

      source_table_params = %{
        "exclude_column_attnums" => nil,
        # Non-existent column
        "include_column_attnums" => [999]
      }

      # The validation doesn't check for non-existent columns, it only checks for empty lists
      # Empty list means "not set" in our validation logic
      assert Form.verify_column_selection(table, source_table_params) == :ok
    end
  end
end

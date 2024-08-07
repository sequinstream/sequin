defmodule Sequin.MigrationsTest do
  use Sequin.DataCase, async: true

  alias Ecto.Migration.Index
  alias Ecto.Migration.Table
  alias Sequin.Factory.StreamsFactory
  alias Sequin.Streams.Migrations

  @schema_name "public"
  @table_name "test_table"

  describe "provision_stream_table_ddl/1" do
    test "generates DDL for append mode with basic columns" do
      stream_table =
        StreamsFactory.stream_table(
          insert_mode: :append,
          table_schema_name: @schema_name,
          table_name: @table_name,
          columns: [
            StreamsFactory.stream_table_column(name: "col1", type: :text, is_conflict_key: false),
            StreamsFactory.stream_table_column(name: "col2", type: :integer, is_conflict_key: false)
          ]
        )

      ddl = Migrations.provision_stream_table_ddl(stream_table)

      # Create table + 2 column indexes
      assert length(ddl) == 3

      create_table_cmd =
        Enum.find(ddl, fn cmd ->
          match?({:create, %Table{name: @table_name, prefix: @schema_name, primary_key: false}, _columns}, cmd)
        end)

      assert create_table_cmd

      {:create, _table, columns} = create_table_cmd

      required_columns = [
        {:add, :sequin_id, :uuid, [primary_key: true]},
        {:add, :seq, :bigserial, [null: false]},
        {:add, :data, :text, []},
        {:add, :recorded_at, :timestamp, [null: false]},
        {:add, :deleted, :boolean, [null: false]}
      ]

      custom_columns = [
        {:add, "col1", :string, []},
        {:add, "col2", :integer, []}
      ]

      expected_columns = required_columns ++ custom_columns

      assert_lists_equal(columns, expected_columns)

      assert Enum.any?(ddl, fn cmd -> match?({:create, %Index{columns: ["col1"]}}, cmd) end)
      assert Enum.any?(ddl, fn cmd -> match?({:create, %Index{columns: ["col2"]}}, cmd) end)
    end

    test "generates DDL for upsert mode with conflict keys" do
      stream_table =
        StreamsFactory.stream_table(
          insert_mode: :upsert,
          table_schema_name: @schema_name,
          table_name: @table_name,
          columns: [
            StreamsFactory.stream_table_column(name: "id", type: :uuid, is_conflict_key: true),
            StreamsFactory.stream_table_column(name: "value", type: :integer, is_conflict_key: false)
          ]
        )

      ddl = Migrations.provision_stream_table_ddl(stream_table)

      # Create table + 1 column index + 1 conflict key index
      assert length(ddl) == 3

      assert Enum.any?(ddl, fn cmd ->
               match?({:create, %Table{name: @table_name, prefix: @schema_name}, _columns}, cmd)
             end)

      assert Enum.any?(ddl, fn cmd -> match?({:create, %Index{columns: ["value"], unique: false}}, cmd) end)
      assert Enum.any?(ddl, fn cmd -> match?({:create, %Index{columns: ["id"], unique: true}}, cmd) end)
    end

    test "generates DDL with multiple conflict keys" do
      stream_table =
        StreamsFactory.stream_table(
          insert_mode: :upsert,
          table_schema_name: @schema_name,
          table_name: @table_name,
          columns: [
            StreamsFactory.stream_table_column(name: "id1", type: :uuid, is_conflict_key: true),
            StreamsFactory.stream_table_column(name: "id2", type: :text, is_conflict_key: true),
            StreamsFactory.stream_table_column(name: "value", type: :integer, is_conflict_key: false)
          ]
        )

      ddl = Migrations.provision_stream_table_ddl(stream_table)

      assert Enum.any?(ddl, fn cmd -> match?({:create, %Index{columns: ["value"], unique: false}}, cmd) end)
      assert Enum.any?(ddl, fn cmd -> match?({:create, %Index{columns: ["id1"], unique: false}}, cmd) end)
      assert Enum.any?(ddl, fn cmd -> match?({:create, %Index{columns: ["id1"], unique: false}}, cmd) end)
      assert Enum.any?(ddl, fn cmd -> match?({:create, %Index{columns: ["id1", "id2"], unique: true}}, cmd) end)
    end

    test "generates DDL with all possible column types" do
      stream_table =
        StreamsFactory.stream_table(
          insert_mode: :append,
          table_schema_name: @schema_name,
          table_name: @table_name,
          columns: [
            StreamsFactory.stream_table_column(name: "text_col", type: :text, is_conflict_key: false),
            StreamsFactory.stream_table_column(name: "integer_col", type: :integer, is_conflict_key: false),
            StreamsFactory.stream_table_column(name: "boolean_col", type: :boolean, is_conflict_key: false),
            StreamsFactory.stream_table_column(name: "timestamp_col", type: :timestamp, is_conflict_key: false),
            StreamsFactory.stream_table_column(name: "uuid_col", type: :uuid, is_conflict_key: false)
          ]
        )

      ddl = Migrations.provision_stream_table_ddl(stream_table)

      # Create table + 5 column indexes
      assert length(ddl) == 6

      create_table_cmd =
        Enum.find(ddl, fn cmd ->
          match?({:create, %Table{name: @table_name, prefix: @schema_name}, _columns}, cmd)
        end)

      {:create, _table, columns} = create_table_cmd
      assert Enum.any?(columns, fn {:add, name, type, _} -> name == "text_col" and type == :string end)
      assert Enum.any?(columns, fn {:add, name, type, _} -> name == "integer_col" and type == :integer end)
      assert Enum.any?(columns, fn {:add, name, type, _} -> name == "boolean_col" and type == :boolean end)
      assert Enum.any?(columns, fn {:add, name, type, _} -> name == "timestamp_col" and type == :utc_datetime end)
      assert Enum.any?(columns, fn {:add, name, type, _} -> name == "uuid_col" and type == :uuid end)
    end

    test "generates DDL with custom table schema name" do
      custom_schema = "custom_schema"

      stream_table =
        StreamsFactory.stream_table(
          table_schema_name: custom_schema,
          table_name: @table_name
        )

      ddl = Migrations.provision_stream_table_ddl(stream_table)

      assert Enum.any?(ddl, fn cmd ->
               match?({:create, %Table{name: @table_name, prefix: ^custom_schema}, _columns}, cmd)
             end)
    end
  end

  describe "migrate_stream_table_ddl/2" do
    test "generates DDL for adding a new column" do
      existing_column = StreamsFactory.stream_table_column(name: "existing_col", type: :text, is_conflict_key: false)
      new_column = StreamsFactory.stream_table_column(name: "new_col", type: :integer, is_conflict_key: false)

      old_stream_table =
        StreamsFactory.stream_table(
          table_schema_name: @schema_name,
          table_name: @table_name,
          columns: [existing_column]
        )

      new_stream_table = %{
        old_stream_table
        | columns: [existing_column, new_column]
      }

      ddl = Migrations.migrate_stream_table_ddl(old_stream_table, new_stream_table)

      # Alter table + Create index
      assert length(ddl) == 2

      assert Enum.any?(ddl, fn cmd ->
               match?(
                 {:alter, %Table{name: @table_name, prefix: @schema_name}, [{:add, "new_col", :integer, []}]},
                 cmd
               )
             end)

      assert Enum.any?(ddl, fn cmd ->
               match?({:create, %Index{table: @table_name, columns: ["new_col"]}}, cmd)
             end)
    end

    test "generates DDL for removing a column" do
      column1 = StreamsFactory.stream_table_column(name: "col1", type: :text, is_conflict_key: false)
      column2 = StreamsFactory.stream_table_column(name: "col2", type: :integer, is_conflict_key: false)

      old_stream_table =
        StreamsFactory.stream_table(
          table_schema_name: @schema_name,
          table_name: @table_name,
          columns: [column1, column2]
        )

      new_stream_table = %{
        old_stream_table
        | columns: [column1]
      }

      ddl = Migrations.migrate_stream_table_ddl(old_stream_table, new_stream_table)

      # Alter table
      assert length(ddl) == 1

      assert Enum.any?(ddl, fn cmd ->
               match?(
                 {:alter, %Table{name: @table_name, prefix: @schema_name}, [{:remove, "col2"}]},
                 cmd
               )
             end)
    end

    test "generates DDL for renaming a column" do
      old_column = StreamsFactory.stream_table_column(name: "old_name", type: :text, is_conflict_key: false)
      new_column = %{old_column | name: "new_name"}

      old_stream_table =
        StreamsFactory.stream_table(
          table_schema_name: @schema_name,
          table_name: @table_name,
          columns: [old_column]
        )

      new_stream_table = %{
        old_stream_table
        | columns: [new_column]
      }

      ddl = Migrations.migrate_stream_table_ddl(old_stream_table, new_stream_table)

      # Rename column + Rename index
      assert length(ddl) == 2

      assert Enum.any?(ddl, fn cmd ->
               match?(
                 {:rename, %Table{name: @table_name, prefix: @schema_name}, "old_name", "new_name"},
                 cmd
               )
             end)

      assert Enum.any?(ddl, fn cmd ->
               match?({:rename_index, [from: _, to: _, prefix: @schema_name]}, cmd)
             end)
    end

    test "generates DDL for changing table name" do
      old_table_name = "old_table"
      new_table_name = "new_table"

      columns = [
        StreamsFactory.stream_table_column(name: "col1", type: :text, is_conflict_key: false),
        StreamsFactory.stream_table_column(name: "col2", type: :integer, is_conflict_key: false)
      ]

      old_stream_table =
        StreamsFactory.stream_table(
          table_schema_name: @schema_name,
          table_name: old_table_name,
          columns: columns
        )

      new_stream_table = %{old_stream_table | table_name: new_table_name}

      ddl = Migrations.migrate_stream_table_ddl(old_stream_table, new_stream_table)

      # Rename table + Rename indexes for each column
      assert length(ddl) == 3

      assert Enum.any?(ddl, fn cmd ->
               match?({:rename_table, [from: ^old_table_name, to: ^new_table_name, prefix: @schema_name]}, cmd)
             end)

      Enum.each(columns, fn column ->
        old_index_name = "idx_#{old_table_name}_#{column.name}"
        new_index_name = "idx_#{new_table_name}_#{column.name}"

        assert Enum.any?(ddl, fn cmd ->
                 match?({:rename_index, [from: ^old_index_name, to: ^new_index_name, prefix: @schema_name]}, cmd)
               end)
      end)
    end

    test "generates DDL for changing schema name" do
      old_schema = "old_schema"
      new_schema = "new_schema"

      old_stream_table =
        StreamsFactory.stream_table(
          table_schema_name: old_schema,
          table_name: @table_name
        )

      new_stream_table = %{old_stream_table | table_schema_name: new_schema}

      ddl = Migrations.migrate_stream_table_ddl(old_stream_table, new_stream_table)

      # Change schema
      assert length(ddl) == 1

      assert Enum.any?(ddl, fn cmd ->
               match?({:change_schema, [from: ^old_schema, to: ^new_schema, table_name: @table_name]}, cmd)
             end)
    end

    test "generates DDL for combination of multiple changes" do
      old_schema = "old_schema"
      new_schema = "new_schema"
      old_table_name = "old_table"
      new_table_name = "new_table"

      column1 = StreamsFactory.stream_table_column(name: "col1", type: :text, is_conflict_key: false)
      column2 = StreamsFactory.stream_table_column(name: "col2", type: :integer, is_conflict_key: false)
      new_column = StreamsFactory.stream_table_column(name: "new_col", type: :boolean, is_conflict_key: false)

      old_stream_table =
        StreamsFactory.stream_table(
          insert_mode: :append,
          table_schema_name: old_schema,
          table_name: old_table_name,
          columns: [column1, column2]
        )

      new_stream_table = %{
        old_stream_table
        | table_schema_name: new_schema,
          table_name: new_table_name,
          columns: [column1, new_column]
      }

      ddl = Migrations.migrate_stream_table_ddl(old_stream_table, new_stream_table)

      # Rename table + Change schema + Remove column + Add column + Create index + Alter index
      assert length(ddl) == 6

      assert Enum.any?(ddl, fn cmd ->
               match?({:rename_table, [from: ^old_table_name, to: ^new_table_name, prefix: ^old_schema]}, cmd)
             end)

      assert Enum.any?(ddl, fn cmd ->
               match?({:change_schema, [from: ^old_schema, to: ^new_schema, table_name: ^new_table_name]}, cmd)
             end)

      assert Enum.any?(ddl, fn cmd -> match?({:alter, _, [{:remove, "col2"}]}, cmd) end)
      assert Enum.any?(ddl, fn cmd -> match?({:alter, _, [{:add, "new_col", :boolean, []}]}, cmd) end)
      assert Enum.any?(ddl, fn cmd -> match?({:create, %Index{columns: ["new_col"]}}, cmd) end)
      assert Enum.any?(ddl, fn cmd -> match?({:rename_index, _}, cmd) end)
    end

    test "generates DDL for renaming index when column is renamed" do
      old_column_name = "old_name"
      new_column_name = "new_name"
      column = StreamsFactory.stream_table_column(name: old_column_name, type: :text, is_conflict_key: false)

      old_stream_table =
        StreamsFactory.stream_table(
          table_schema_name: @schema_name,
          table_name: @table_name,
          columns: [column]
        )

      new_stream_table = %{
        old_stream_table
        | columns: [%{column | name: new_column_name}]
      }

      ddl = Migrations.migrate_stream_table_ddl(old_stream_table, new_stream_table)
      from = "idx_#{@table_name}_#{old_column_name}"
      to = "idx_#{@table_name}_#{new_column_name}"

      assert Enum.any?(ddl, fn cmd ->
               match?(
                 {:rename_index,
                  [
                    from: ^from,
                    to: ^to,
                    prefix: @schema_name
                  ]},
                 cmd
               )
             end)
    end

    test "generates DDL for renaming indexes when table is renamed" do
      old_table_name = "old_table"
      new_table_name = "new_table"

      old_stream_table =
        StreamsFactory.stream_table(
          table_schema_name: @schema_name,
          table_name: old_table_name,
          columns: [
            StreamsFactory.stream_table_column(name: "col1", type: :text, is_conflict_key: false),
            StreamsFactory.stream_table_column(name: "col2", type: :integer, is_conflict_key: false)
          ]
        )

      new_stream_table = %{old_stream_table | table_name: new_table_name}

      ddl = Migrations.migrate_stream_table_ddl(old_stream_table, new_stream_table)
      from = "idx_#{old_table_name}_col1"
      to = "idx_#{new_table_name}_col1"

      assert Enum.any?(ddl, fn cmd ->
               match?(
                 {:rename_index, [from: ^from, to: ^to, prefix: @schema_name]},
                 cmd
               )
             end)

      from = "idx_#{old_table_name}_col2"
      to = "idx_#{new_table_name}_col2"

      assert Enum.any?(ddl, fn cmd ->
               match?(
                 {:rename_index, [from: ^from, to: ^to, prefix: @schema_name]},
                 cmd
               )
             end)
    end
  end

  describe "drop_stream_table_ddl/1" do
    test "generates DDL for dropping a table" do
      stream_table =
        StreamsFactory.stream_table(
          table_schema_name: @schema_name,
          table_name: @table_name
        )

      ddl = Migrations.drop_stream_table_ddl(stream_table)

      assert {:drop, %Table{name: @table_name, prefix: @schema_name}, :cascade} = ddl
    end
  end
end

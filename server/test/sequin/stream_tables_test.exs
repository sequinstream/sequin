defmodule Sequin.StreamTablesTest do
  use Sequin.DataCase, async: true

  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Factory.StreamsFactory
  alias Sequin.Repo
  alias Sequin.Streams
  alias Sequin.Streams.StreamTable

  @test_schema "stream_tables_test_schema"
  @test_schema_new "stream_tables_test_schema_new"

  setup do
    # Drop and recreate the test schema
    Repo.query!("DROP SCHEMA IF EXISTS #{@test_schema} CASCADE")
    Repo.query!("CREATE SCHEMA #{@test_schema}")
    Repo.query!("DROP SCHEMA IF EXISTS #{@test_schema_new} CASCADE")
    Repo.query!("CREATE SCHEMA #{@test_schema_new}")

    account = AccountsFactory.insert_account!()
    postgres_database = DatabasesFactory.insert_postgres_database!(account_id: account.id)

    replication_slot =
      ReplicationFactory.insert_postgres_replication!(account_id: account.id, postgres_database_id: postgres_database.id)

    test_pid = self()

    on_exit(fn ->
      Ecto.Adapters.SQL.Sandbox.allow(Repo, test_pid, self())
      Repo.query!("DROP SCHEMA IF EXISTS #{@test_schema} CASCADE")
      Repo.query!("DROP SCHEMA IF EXISTS #{@test_schema_new} CASCADE")
    end)

    base_attrs = %{
      table_schema_name: @test_schema,
      account_id: account.id,
      source_postgres_database_id: postgres_database.id,
      source_replication_slot_id: replication_slot.id
    }

    {:ok,
     account: account, postgres_database: postgres_database, replication_slot: replication_slot, base_attrs: base_attrs}
  end

  describe "CRUD operations on stream_tables" do
    test "creating a stream table with append insert mode", %{base_attrs: base_attrs} do
      attrs =
        stream_table_attrs(base_attrs, %{
          name: "append_stream_table",
          table_name: "append_table",
          insert_mode: :append,
          columns: [
            %{name: "value", type: :integer, is_conflict_key: false}
          ]
        })

      assert {:ok, stream_table} = Streams.create_stream_table(base_attrs.account_id, attrs)
      assert_stream_table(stream_table, attrs)
    end

    test "creating a stream table with upsert insert mode", %{base_attrs: base_attrs} do
      attrs =
        stream_table_attrs(base_attrs, %{
          name: "upsert_stream_table",
          table_name: "upsert_table",
          insert_mode: :upsert,
          columns: [
            %{name: "id", type: :text, is_conflict_key: true},
            %{name: "value", type: :integer, is_conflict_key: false}
          ]
        })

      assert {:ok, stream_table} = Streams.create_stream_table(base_attrs.account_id, attrs)
      assert_stream_table(stream_table, attrs)
    end

    test "creating a stream table with invalid conflict keys", %{base_attrs: base_attrs} do
      append_attrs =
        stream_table_attrs(base_attrs, %{
          name: "invalid_append_stream_table",
          table_name: "invalid_append_table",
          insert_mode: :append,
          columns: [
            %{name: "id", type: :text, is_conflict_key: true},
            %{name: "value", type: :integer, is_conflict_key: false}
          ]
        })

      assert {:error, changeset} = Streams.create_stream_table(base_attrs.account_id, append_attrs)
      assert Sequin.Error.errors_on(changeset).columns == ["cannot have conflict keys when insert_mode is :append"]

      upsert_attrs =
        stream_table_attrs(base_attrs, %{
          name: "invalid_upsert_stream_table",
          table_name: "invalid_upsert_table",
          insert_mode: :upsert,
          columns: [
            %{name: "value", type: :integer, is_conflict_key: false}
          ]
        })

      assert {:error, changeset} = Streams.create_stream_table(base_attrs.account_id, upsert_attrs)

      assert Sequin.Error.errors_on(changeset).columns == [
               "must have at least one conflict key when insert_mode is :upsert"
             ]
    end

    test "updating a stream table with columns", %{account: account} do
      stream_table = StreamsFactory.insert_stream_table!(account_id: account.id)
      new_account = AccountsFactory.insert_account!()

      update_attrs = %{
        name: "updated_name",
        # This should be ignored
        account_id: new_account.id,
        columns: [
          %{name: "new_column", type: :boolean, is_conflict_key: false}
        ]
      }

      assert {:ok, updated_stream_table} = Streams.update_stream_table(stream_table, update_attrs)
      assert updated_stream_table.name == "updated_name"
      # Should not change
      assert updated_stream_table.account_id == account.id
      assert length(updated_stream_table.columns) == 1
      assert Enum.at(updated_stream_table.columns, 0).name == "new_column"
    end

    test "deleting a stream table", %{account: account} do
      stream_table = StreamsFactory.insert_stream_table!(account_id: account.id)
      assert {:ok, _} = Streams.delete_stream_table(stream_table)
      assert Repo.get(StreamTable, stream_table.id) == nil
    end
  end

  describe "Migrations on stream_tables - create" do
    test "create_with_lifecycle provisions the table with correct primary key and indexes for append mode", %{
      base_attrs: base_attrs
    } do
      attrs =
        stream_table_attrs(base_attrs, %{
          name: "append_lifecycle_test",
          table_name: "append_lifecycle_table",
          insert_mode: :append,
          columns: [
            %{name: "value", type: :integer, is_conflict_key: false}
          ]
        })

      assert {:ok, _stream_table} = Streams.create_stream_table_for_account_with_lifecycle(base_attrs.account_id, attrs)

      # Check if the table exists in the database
      assert {:ok, _} = Repo.query("SELECT * FROM #{@test_schema}.append_lifecycle_table LIMIT 0")

      # Check if columns exist and sequin_id is the primary key
      columns = list_columns_for_table(@test_schema, "append_lifecycle_table")

      assert Enum.any?(columns, fn col -> col[:name] == "sequin_id" and col[:type] == "uuid" end)
      assert Enum.any?(columns, fn col -> col[:name] == "value" and col[:type] == "integer" end)

      # Check if sequin_id is the primary key
      assert ["sequin_id"] == list_primary_keys(@test_schema, "append_lifecycle_table")

      # Check that expected indexes exist
      assert_lists_equal(
        [
          %{name: "append_lifecycle_table_pkey", columns: ["sequin_id"], unique: true},
          %{name: "idx_append_lifecycle_table_value", columns: ["value"], unique: false}
        ],
        list_indexes(@test_schema, "append_lifecycle_table")
      )
    end

    test "create_with_lifecycle provisions the table with correct primary key and indexes for upsert mode", %{
      base_attrs: base_attrs
    } do
      attrs =
        stream_table_attrs(base_attrs, %{
          name: "upsert_lifecycle_test",
          table_name: "upsert_lifecycle_table",
          insert_mode: :upsert,
          columns: [
            %{name: "id", type: :uuid, is_conflict_key: true},
            %{name: "value", type: :integer, is_conflict_key: false}
          ]
        })

      assert {:ok, _stream_table} = Streams.create_stream_table_for_account_with_lifecycle(base_attrs.account_id, attrs)

      # Check if the table exists in the database
      assert {:ok, _} = Repo.query("SELECT * FROM #{@test_schema}.upsert_lifecycle_table LIMIT 0")

      # Check if columns exist
      columns = list_columns_for_table(@test_schema, "upsert_lifecycle_table")

      assert Enum.any?(columns, fn col -> col[:name] == "id" and col[:type] == "uuid" end)
      assert Enum.any?(columns, fn col -> col[:name] == "value" and col[:type] == "integer" end)
      assert Enum.any?(columns, fn col -> col[:name] == "sequin_id" and col[:type] == "uuid" end)

      # Check if sequin_id is the primary key
      assert ["sequin_id"] == list_primary_keys(@test_schema, "upsert_lifecycle_table")

      # Check if there's a unique index on the conflict key
      indexes = list_indexes(@test_schema, "upsert_lifecycle_table")
      assert Enum.any?(indexes, fn index -> index.columns == ["id"] and index.unique end)
    end

    test "create_with_lifecycle creates a compound unique index when there are multiple conflict keys", %{
      base_attrs: base_attrs
    } do
      attrs =
        stream_table_attrs(base_attrs, %{
          name: "compound_conflict_key_test",
          table_name: "compound_conflict_key_table",
          insert_mode: :upsert,
          columns: [
            %{name: "id1", type: :text, is_conflict_key: true},
            %{name: "id2", type: :text, is_conflict_key: true},
            %{name: "value", type: :integer, is_conflict_key: false}
          ]
        })

      assert {:ok, _stream_table} = Streams.create_stream_table_for_account_with_lifecycle(base_attrs.account_id, attrs)

      # Check if sequin_id is the primary key
      assert ["sequin_id"] == list_primary_keys(@test_schema, "compound_conflict_key_table")

      # Check if there's a compound unique index on the conflict keys
      indexes = list_indexes(@test_schema, "compound_conflict_key_table")
      assert Enum.any?(indexes, fn index -> Enum.sort(index.columns) == ["id1", "id2"] and index.unique end)
    end

    test "create_with_lifecycle creates indexes for all columns in append mode", %{base_attrs: base_attrs} do
      attrs =
        stream_table_attrs(base_attrs, %{
          name: "append_indexes_test",
          table_name: "append_indexes_table",
          insert_mode: :append,
          columns: [
            %{name: "col1", type: :text, is_conflict_key: false},
            %{name: "col2", type: :integer, is_conflict_key: false}
          ]
        })

      assert {:ok, _stream_table} = Streams.create_stream_table_for_account_with_lifecycle(base_attrs.account_id, attrs)

      indexes = list_indexes(@test_schema, "append_indexes_table")
      assert Enum.any?(indexes, fn index -> index.columns == ["col1"] end)
      assert Enum.any?(indexes, fn index -> index.columns == ["col2"] end)
    end

    test "create_with_lifecycle creates indexes for all columns and a unique index for conflict keys in upsert mode", %{
      base_attrs: base_attrs
    } do
      attrs =
        stream_table_attrs(base_attrs, %{
          name: "upsert_indexes_test",
          table_name: "upsert_indexes_table",
          insert_mode: :upsert,
          columns: [
            %{name: "id", type: :text, is_conflict_key: true},
            %{name: "value", type: :integer, is_conflict_key: false}
          ]
        })

      assert {:ok, _stream_table} = Streams.create_stream_table_for_account_with_lifecycle(base_attrs.account_id, attrs)

      indexes = list_indexes(@test_schema, "upsert_indexes_table")
      assert Enum.any?(indexes, fn index -> index.columns == ["id"] and index.unique end)
      assert Enum.any?(indexes, fn index -> index.columns == ["value"] and not index.unique end)
    end
  end

  describe "Migrations on stream_tables - update and delete" do
    setup %{base_attrs: base_attrs} do
      attrs =
        stream_table_attrs(base_attrs, %{
          name: "lifecycle_test",
          table_name: "lifecycle_table",
          insert_mode: :append,
          columns: [
            %{name: "existing_column", type: :text, is_conflict_key: false},
            %{name: "to_be_renamed", type: :integer, is_conflict_key: false},
            %{name: "to_be_deleted", type: :boolean, is_conflict_key: false}
          ]
        })

      {:ok, stream_table} = Streams.create_stream_table_for_account_with_lifecycle(base_attrs.account_id, attrs)
      {:ok, stream_table: stream_table}
    end

    test "update_with_lifecycle reflects schema and table name changes", %{stream_table: stream_table} do
      update_attrs = %{
        table_schema_name: @test_schema_new,
        table_name: "new_table_name"
      }

      assert {:ok, _updated_stream_table} = Streams.update_stream_table_with_lifecycle(stream_table, update_attrs)

      # Check if the old table doesn't exist
      assert {:error, _} = Repo.query("SELECT * FROM #{@test_schema}.#{stream_table.table_name} LIMIT 0")

      # Check if the new table exists
      assert {:ok, _} = Repo.query("SELECT * FROM #{@test_schema_new}.new_table_name LIMIT 0")
    end

    test "update_with_lifecycle handles column additions, deletions, and name changes", %{stream_table: stream_table} do
      # Fetch existing columns
      existing_columns = Repo.preload(stream_table, :columns).columns
      existing_column_id = Enum.find(existing_columns, &(&1.name == "existing_column")).id
      to_be_renamed_id = Enum.find(existing_columns, &(&1.name == "to_be_renamed")).id

      update_attrs = %{
        columns: [
          %{id: existing_column_id, name: "existing_column", type: :text, is_conflict_key: false},
          %{id: to_be_renamed_id, name: "renamed_column", type: :integer, is_conflict_key: false},
          %{name: "new_column", type: :timestamp, is_conflict_key: false}
        ]
      }

      assert {:ok, updated_stream_table} = Streams.update_stream_table_with_lifecycle(stream_table, update_attrs)

      # Check if columns exist
      columns = list_columns_for_table(@test_schema, updated_stream_table.table_name)
      column_names = Enum.map(columns, & &1[:name])

      assert "existing_column" in column_names
      assert "renamed_column" in column_names
      assert "new_column" in column_names
      refute "to_be_deleted" in column_names
      refute "to_be_renamed" in column_names
    end

    test "update_with_lifecycle renames index when column name is changed", %{stream_table: stream_table} do
      existing_columns = Repo.preload(stream_table, :columns).columns
      to_be_renamed_id = Enum.find(existing_columns, &(&1.name == "to_be_renamed")).id

      update_attrs = %{
        columns: [
          %{id: to_be_renamed_id, name: "renamed_column", type: :integer, is_conflict_key: false}
        ]
      }

      assert {:ok, updated_stream_table} = Streams.update_stream_table_with_lifecycle(stream_table, update_attrs)

      indexes = list_indexes(@test_schema, updated_stream_table.table_name)
      assert Enum.any?(indexes, fn index -> index.columns == ["renamed_column"] end)
      refute Enum.any?(indexes, fn index -> index.columns == ["to_be_renamed"] end)
    end

    test "update_with_lifecycle renames indexes when table name is changed", %{stream_table: stream_table} do
      update_attrs = %{
        table_name: "new_table_name"
      }

      assert {:ok, _updated_stream_table} = Streams.update_stream_table_with_lifecycle(stream_table, update_attrs)

      old_indexes = list_indexes(@test_schema, stream_table.table_name)
      new_indexes = list_indexes(@test_schema, "new_table_name")

      assert Enum.empty?(old_indexes)
      # -1 for the required primary key (and its idx)
      assert length(new_indexes) - 1 == length(stream_table.columns)

      for column <- stream_table.columns do
        assert Enum.any?(new_indexes, fn index -> index.columns == [column.name] end)
      end
    end

    test "delete_with_lifecycle removes the table", %{stream_table: stream_table} do
      assert :ok = Streams.delete_stream_table_with_lifecycle(stream_table)

      # Check if the table doesn't exist
      assert {:error, _} = Repo.query("SELECT * FROM #{@test_schema}.#{stream_table.table_name} LIMIT 0")
    end
  end

  defp list_columns_for_table(schema, table) do
    query = """
    SELECT column_name, data_type, is_identity, column_default
    FROM information_schema.columns
    WHERE table_schema = $1 AND table_name = $2
    """

    {:ok, result} = Repo.query(query, [schema, table])
    Enum.map(result.rows, &Enum.zip([:name, :type, :is_identity, :default], &1))
  end

  defp list_primary_keys(schema, table) do
    query = """
    SELECT a.attname
    FROM   pg_index i
    JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
    WHERE  i.indrelid = (SELECT oid FROM pg_class WHERE relnamespace = $1::text::regnamespace AND relname = $2)
    AND    i.indisprimary
    """

    {:ok, result} = Repo.query(query, [schema, table])
    Enum.map(result.rows, &List.first/1)
  end

  defp list_indexes(schema, table) do
    query = """
    SELECT
      i.relname AS index_name,
      array_agg(a.attname ORDER BY k.i) AS columns,
      ix.indisunique AS unique
    FROM
      pg_class t,
      pg_class i,
      pg_index ix,
      pg_attribute a,
      generate_subscripts(ix.indkey, 1) k(i)
    WHERE
      t.oid = ix.indrelid
      AND i.oid = ix.indexrelid
      AND a.attrelid = t.oid
      AND a.attnum = ix.indkey[k.i]
      AND t.relkind = 'r'
      AND t.relname = $2
      AND t.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $1)
    GROUP BY
      i.relname,
      ix.indisunique
    ORDER BY
      i.relname;
    """

    {:ok, result} = Repo.query(query, [schema, table])

    Enum.map(result.rows, fn [name, columns, unique] ->
      %{name: name, columns: columns, unique: unique}
    end)
  end

  defp stream_table_attrs(base_attrs, attrs) do
    StreamsFactory.stream_table_attrs(Map.merge(base_attrs, attrs))
  end

  defp assert_stream_table(stream_table, attrs) do
    stream_table = Repo.preload(stream_table, :columns)
    assert stream_table.name == attrs.name
    assert stream_table.insert_mode == attrs.insert_mode
    assert length(stream_table.columns) == length(attrs.columns)

    Enum.each(stream_table.columns, fn column ->
      attr_column = Enum.find(attrs.columns, &(&1.name == column.name))
      assert column.type == attr_column.type
      assert column.is_conflict_key == attr_column.is_conflict_key
    end)
  end
end

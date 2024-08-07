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

    {:ok, account: account, postgres_database: postgres_database, replication_slot: replication_slot}
  end

  describe "CRUD operations on stream_tables" do
    test "creating a stream table with append insert mode", %{
      account: account,
      postgres_database: postgres_database,
      replication_slot: replication_slot
    } do
      attrs =
        StreamsFactory.stream_table_attrs(%{
          name: "append_stream_table",
          table_schema_name: @test_schema,
          table_name: "append_table",
          account_id: account.id,
          source_postgres_database_id: postgres_database.id,
          source_replication_slot_id: replication_slot.id,
          insert_mode: :append,
          columns: [
            %{name: "value", type: :integer, is_pk: false}
          ]
        })

      assert {:ok, stream_table} = Streams.create_stream_table(account.id, attrs)
      stream_table = Repo.preload(stream_table, :columns)
      assert stream_table.name == "append_stream_table"
      assert stream_table.insert_mode == :append
      assert length(stream_table.columns) == 1
      assert Enum.all?(stream_table.columns, &(not &1.is_pk))
    end

    test "creating a stream table with upsert insert mode", %{
      account: account,
      postgres_database: postgres_database,
      replication_slot: replication_slot
    } do
      attrs =
        StreamsFactory.stream_table_attrs(%{
          name: "upsert_stream_table",
          table_schema_name: @test_schema,
          table_name: "upsert_table",
          account_id: account.id,
          source_postgres_database_id: postgres_database.id,
          source_replication_slot_id: replication_slot.id,
          insert_mode: :upsert,
          columns: [
            %{name: "id", type: :text, is_pk: true},
            %{name: "value", type: :integer, is_pk: false}
          ]
        })

      assert {:ok, stream_table} = Streams.create_stream_table(account.id, attrs)
      stream_table = Repo.preload(stream_table, :columns)
      assert stream_table.name == "upsert_stream_table"
      assert stream_table.insert_mode == :upsert
      assert length(stream_table.columns) == 2
      assert Enum.any?(stream_table.columns, & &1.is_pk)
    end

    test "creating a stream table with invalid primary keys", %{
      account: account,
      postgres_database: postgres_database,
      replication_slot: replication_slot
    } do
      append_attrs =
        StreamsFactory.stream_table_attrs(%{
          name: "invalid_append_stream_table",
          table_schema_name: @test_schema,
          table_name: "invalid_append_table",
          account_id: account.id,
          source_postgres_database_id: postgres_database.id,
          source_replication_slot_id: replication_slot.id,
          insert_mode: :append,
          columns: [
            %{name: "id", type: :text, is_pk: true},
            %{name: "value", type: :integer, is_pk: false}
          ]
        })

      assert {:error, changeset} = Streams.create_stream_table(account.id, append_attrs)
      assert Sequin.Error.errors_on(changeset).columns == ["cannot have primary keys when insert_mode is :append"]

      upsert_attrs =
        StreamsFactory.stream_table_attrs(%{
          name: "invalid_upsert_stream_table",
          table_schema_name: @test_schema,
          table_name: "invalid_upsert_table",
          account_id: account.id,
          source_postgres_database_id: postgres_database.id,
          source_replication_slot_id: replication_slot.id,
          insert_mode: :upsert,
          columns: [
            %{name: "value", type: :integer, is_pk: false}
          ]
        })

      assert {:error, changeset} = Streams.create_stream_table(account.id, upsert_attrs)

      assert Sequin.Error.errors_on(changeset).columns == [
               "must have at least one primary key when insert_mode is :upsert"
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
          %{name: "new_column", type: :boolean, is_pk: false}
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
    test "create_with_lifecycle provisions the table with correct primary keys for append mode", %{
      account: account,
      postgres_database: postgres_database,
      replication_slot: replication_slot
    } do
      attrs =
        StreamsFactory.stream_table_attrs(%{
          name: "append_lifecycle_test",
          table_schema_name: @test_schema,
          table_name: "append_lifecycle_table",
          source_postgres_database_id: postgres_database.id,
          source_replication_slot_id: replication_slot.id,
          account_id: account.id,
          insert_mode: :append,
          columns: [
            %{name: "value", type: :integer, is_pk: false}
          ]
        })

      assert {:ok, _stream_table} = Streams.create_stream_table_for_account_with_lifecycle(account.id, attrs)

      # Check if the table exists in the database
      assert {:ok, _} = Repo.query("SELECT * FROM #{@test_schema}.append_lifecycle_table LIMIT 0")

      # Check if columns exist and sequin_id is the primary key
      columns_query = """
      SELECT column_name, data_type, is_identity, column_default
      FROM information_schema.columns
      WHERE table_schema = '#{@test_schema}' AND table_name = 'append_lifecycle_table'
      """

      {:ok, result} = Repo.query(columns_query)
      columns = Enum.map(result.rows, &Enum.zip([:name, :type, :is_identity, :default], &1))

      assert Enum.any?(columns, fn col -> col[:name] == "sequin_id" and col[:type] == "uuid" end)
      assert Enum.any?(columns, fn col -> col[:name] == "value" and col[:type] == "integer" end)

      # Check if sequin_id is the primary key
      pk_query = """
      SELECT a.attname
      FROM   pg_index i
      JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
      WHERE  i.indrelid = '#{@test_schema}.append_lifecycle_table'::regclass
      AND    i.indisprimary
      """

      {:ok, pk_result} = Repo.query(pk_query)
      assert pk_result.rows == [["sequin_id"]]
    end

    test "create_with_lifecycle provisions the table with correct primary keys for upsert mode", %{
      account: account,
      postgres_database: postgres_database,
      replication_slot: replication_slot
    } do
      attrs =
        StreamsFactory.stream_table_attrs(%{
          name: "upsert_lifecycle_test",
          table_schema_name: @test_schema,
          table_name: "upsert_lifecycle_table",
          source_postgres_database_id: postgres_database.id,
          source_replication_slot_id: replication_slot.id,
          account_id: account.id,
          insert_mode: :upsert,
          columns: [
            %{name: "id", type: :text, is_pk: true},
            %{name: "value", type: :integer, is_pk: false}
          ]
        })

      assert {:ok, _stream_table} = Streams.create_stream_table_for_account_with_lifecycle(account.id, attrs)

      # Check if the table exists in the database
      assert {:ok, _} = Repo.query("SELECT * FROM #{@test_schema}.upsert_lifecycle_table LIMIT 0")

      # Check if columns exist and id is the primary key
      columns_query = """
      SELECT column_name, data_type, is_identity, column_default
      FROM information_schema.columns
      WHERE table_schema = '#{@test_schema}' AND table_name = 'upsert_lifecycle_table'
      """

      {:ok, result} = Repo.query(columns_query)
      columns = Enum.map(result.rows, &Enum.zip([:name, :type, :is_identity, :default], &1))

      assert Enum.any?(columns, fn col -> col[:name] == "id" and col[:type] == "character varying" end)
      assert Enum.any?(columns, fn col -> col[:name] == "value" and col[:type] == "integer" end)
      assert Enum.any?(columns, fn col -> col[:name] == "sequin_id" end)

      # Check if id is the primary key
      pk_query = """
      SELECT a.attname
      FROM   pg_index i
      JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
      WHERE  i.indrelid = '#{@test_schema}.upsert_lifecycle_table'::regclass
      AND    i.indisprimary
      """

      {:ok, pk_result} = Repo.query(pk_query)
      assert pk_result.rows == [["id"]]
    end

    test "create_with_lifecycle creates a compound PK when there are two PKs", %{
      account: account,
      postgres_database: postgres_database,
      replication_slot: replication_slot
    } do
      attrs =
        StreamsFactory.stream_table_attrs(%{
          name: "compound_pk_test",
          table_schema_name: @test_schema,
          table_name: "compound_pk_table",
          source_postgres_database_id: postgres_database.id,
          source_replication_slot_id: replication_slot.id,
          account_id: account.id,
          insert_mode: :upsert,
          columns: [
            %{name: "id1", type: :text, is_pk: true},
            %{name: "id2", type: :text, is_pk: true},
            %{name: "value", type: :integer, is_pk: false}
          ]
        })

      assert {:ok, _stream_table} = Streams.create_stream_table_for_account_with_lifecycle(account.id, attrs)

      # Check if the compound PK exists
      pk_query = """
      SELECT a.attname
      FROM   pg_index i
      JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
      WHERE  i.indrelid = '#{@test_schema}.compound_pk_table'::regclass
      AND    i.indisprimary
      """

      {:ok, result} = Repo.query(pk_query)
      assert length(result.rows) == 2
      assert Enum.sort(List.flatten(result.rows)) == ["id1", "id2"]
    end
  end

  describe "Migrations on stream_tables - update and delete" do
    setup %{account: account, postgres_database: postgres_database, replication_slot: replication_slot} do
      attrs =
        StreamsFactory.stream_table_attrs(%{
          name: "lifecycle_test",
          table_schema_name: @test_schema,
          table_name: "lifecycle_table",
          source_postgres_database_id: postgres_database.id,
          source_replication_slot_id: replication_slot.id,
          account_id: account.id,
          insert_mode: :append,
          columns: [
            %{name: "existing_column", type: :text, is_pk: false},
            %{name: "to_be_renamed", type: :integer, is_pk: false},
            %{name: "to_be_deleted", type: :boolean, is_pk: false}
          ]
        })

      {:ok, stream_table} = Streams.create_stream_table_for_account_with_lifecycle(account.id, attrs)
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
          %{id: existing_column_id, name: "existing_column", type: :text, is_pk: false},
          %{id: to_be_renamed_id, name: "renamed_column", type: :integer, is_pk: false},
          %{name: "new_column", type: :timestamp, is_pk: false}
        ]
      }

      assert {:ok, updated_stream_table} = Streams.update_stream_table_with_lifecycle(stream_table, update_attrs)

      # Check if columns exist
      columns_query = """
      SELECT column_name, data_type
      FROM information_schema.columns
      WHERE table_schema = '#{@test_schema}' AND table_name = '#{updated_stream_table.table_name}'
      """

      {:ok, result} = Repo.query(columns_query)
      column_names = Enum.map(result.rows, fn [name, _] -> name end)

      assert "existing_column" in column_names
      assert "renamed_column" in column_names
      assert "new_column" in column_names
      refute "to_be_deleted" in column_names
      refute "to_be_renamed" in column_names
    end

    test "delete_with_lifecycle removes the table", %{stream_table: stream_table} do
      assert :ok = Streams.delete_stream_table_with_lifecycle(stream_table)

      # Check if the table doesn't exist
      assert {:error, _} = Repo.query("SELECT * FROM #{@test_schema}.#{stream_table.table_name} LIMIT 0")
    end
  end
end

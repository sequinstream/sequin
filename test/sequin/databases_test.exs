defmodule Sequin.DatabasesTest do
  use Sequin.DataCase, async: true

  alias Sequin.Databases
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Databases.PostgresDatabaseTable
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.TestSupport.Models.Character

  describe "tables/1" do
    test "returns tables for a database with existing tables" do
      db = DatabasesFactory.insert_configured_postgres_database!(tables: [DatabasesFactory.table()])

      assert {:ok, tables} = Databases.tables(db)
      assert length(tables) > 0
      assert %PostgresDatabaseTable{} = hd(tables)
    end

    test "fetches and returns tables for a database without existing tables" do
      db = DatabasesFactory.insert_configured_postgres_database!(tables: [])

      assert {:ok, tables} = Databases.tables(db)
      assert length(tables) > 0
      assert %PostgresDatabaseTable{} = hd(tables)
    end
  end

  describe "update_tables/1" do
    test "updates tables for a database" do
      db = DatabasesFactory.insert_configured_postgres_database!()

      assert {:ok, updated_db} = Databases.update_tables(db)
      assert length(updated_db.tables) > 0
      assert %PostgresDatabaseTable{} = hd(updated_db.tables)
      assert updated_db.tables_refreshed_at != nil

      # Verify that the Characters table is present
      characters_table = Enum.find(updated_db.tables, &(&1.name == "Characters"))
      assert characters_table
      assert characters_table.schema == "public"
      assert characters_table.oid == Character.table_oid()
    end

    test "updates tables and columns for a database" do
      db = DatabasesFactory.insert_configured_postgres_database!()

      assert {:ok, updated_db} = Databases.update_tables(db)
      assert length(updated_db.tables) > 0

      characters_table = Enum.find(updated_db.tables, &(&1.name == "Characters"))
      assert characters_table
      assert length(characters_table.columns) > 0

      expected_columns = ["id", "name", "house", "planet", "is_active", "tags", "inserted_at", "updated_at"]
      actual_columns = Enum.map(characters_table.columns, & &1.name)
      assert Enum.all?(expected_columns, &(&1 in actual_columns))

      id_column = Enum.find(characters_table.columns, &(&1.name == "id"))
      assert id_column.is_pk?
      assert id_column.type == "bigint"
      assert id_column.pg_typtype == "b"

      assert Enum.all?(characters_table.columns, &(&1.pg_typtype != nil))
    end
  end

  describe "update_sequences_from_db/1" do
    test "updates sequence information from the database" do
      db = DatabasesFactory.insert_configured_postgres_database!()

      sequence =
        DatabasesFactory.insert_sequence!(
          account_id: db.account_id,
          postgres_database_id: db.id,
          table_oid: Character.table_oid(),
          table_schema: "wrong_schema",
          table_name: "wrong_table",
          sort_column_attnum: Character.column_attnum("id"),
          sort_column_name: "wrong_column"
        )

      assert {:ok, _updated_db} = Databases.update_tables(db)

      updated_sequence = Repo.reload!(sequence)
      assert updated_sequence.table_oid == Character.table_oid()
      assert updated_sequence.table_schema == "public"
      assert updated_sequence.table_name == "Characters"
      assert updated_sequence.sort_column_attnum == Character.column_attnum("id")
      assert updated_sequence.sort_column_name == "id"
    end
  end

  describe "tables/1 and update_tables/1 error handling" do
    @tag capture_log: true
    test "returns an error when unable to connect to the database" do
      db =
        DatabasesFactory.insert_configured_postgres_database!(
          hostname: "non_existent_host",
          tables: [],
          queue_target: 1,
          queue_interval: 1
        )

      assert {:error, _} = Databases.tables(db)
      assert {:error, _} = Databases.update_tables(db)
    end
  end

  describe "to_postgrex_opts/1" do
    test "updates hostname and port when using a local tunnel" do
      db = DatabasesFactory.insert_configured_postgres_database!(use_local_tunnel: true, port: nil)

      postgrex_opts = PostgresDatabase.to_postgrex_opts(db)

      assert Keyword.get(postgrex_opts, :hostname) == Application.get_env(:sequin, :portal_hostname)
      assert Keyword.get(postgrex_opts, :port) == db.port
    end

    test "uses original hostname and port when not using a local tunnel" do
      db = DatabasesFactory.insert_configured_postgres_database!(use_local_tunnel: false)

      postgrex_opts = PostgresDatabase.to_postgrex_opts(db)

      assert Keyword.get(postgrex_opts, :hostname) == db.hostname
      assert Keyword.get(postgrex_opts, :port) == db.port
    end
  end

  describe "Sequence CRUD operations" do
    setup do
      account = AccountsFactory.insert_account!()
      database = DatabasesFactory.insert_postgres_database!(account_id: account.id)
      {:ok, account: account, database: database}
    end

    test "create_sequence/1 creates a new sequence", %{account: account, database: database} do
      attrs = DatabasesFactory.sequence_attrs(postgres_database_id: database.id)
      assert {:ok, sequence} = Databases.create_sequence(account.id, attrs)
      assert sequence.postgres_database_id == database.id
      assert sequence.table_oid == attrs.table_oid
      assert sequence.table_schema == attrs.table_schema
      assert sequence.table_name == attrs.table_name
      assert sequence.sort_column_attnum == attrs.sort_column_attnum
      assert sequence.sort_column_name == attrs.sort_column_name
    end

    test "find_sequence_for_account/2 retrieves a sequence", %{account: account, database: database} do
      sequence = DatabasesFactory.insert_sequence!(account_id: account.id, postgres_database_id: database.id)
      assert {:ok, retrieved_sequence} = Databases.find_sequence_for_account(account.id, id: sequence.id)
      assert retrieved_sequence.id == sequence.id
    end

    test "find_sequence_for_account/2 returns error for non-existent sequence", %{account: account} do
      assert {:error, _} = Databases.find_sequence_for_account(account.id, id: Ecto.UUID.generate())
    end

    test "list_sequences_for_account/1 lists all sequences for an account", %{account: account, database: database} do
      sequence1 = DatabasesFactory.insert_sequence!(account_id: account.id, postgres_database_id: database.id)
      sequence2 = DatabasesFactory.insert_sequence!(account_id: account.id, postgres_database_id: database.id)
      sequences = Databases.list_sequences_for_account(account.id)
      assert length(sequences) == 2
      assert Enum.any?(sequences, fn s -> s.id == sequence1.id end)
      assert Enum.any?(sequences, fn s -> s.id == sequence2.id end)
    end

    test "delete_sequence/1 deletes a sequence", %{account: account, database: database} do
      sequence = DatabasesFactory.insert_sequence!(account_id: account.id, postgres_database_id: database.id)
      assert {:ok, deleted_sequence} = Databases.delete_sequence(sequence)
      assert deleted_sequence.id == sequence.id
      assert {:error, _} = Databases.find_sequence_for_account(account.id, id: sequence.id)
    end

    test "delete_sequence/1 returns error when sequence is used by consumers", %{account: account, database: database} do
      sequence = DatabasesFactory.insert_sequence!(account_id: account.id, postgres_database_id: database.id)
      ConsumersFactory.insert_sink_consumer!(account_id: account.id, sequence_id: sequence.id)
      assert {:error, _} = Databases.delete_sequence(sequence)
    end

    test "delete_sequences/1 deletes all sequences for a database", %{account: account, database: database} do
      DatabasesFactory.insert_sequence!(account_id: account.id, postgres_database_id: database.id)
      DatabasesFactory.insert_sequence!(account_id: account.id, postgres_database_id: database.id)
      assert {:ok, 2} = Databases.delete_sequences(database)
      assert Databases.list_sequences_for_account(account.id) == []
    end
  end
end

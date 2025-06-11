defmodule Sequin.DatabasesTest do
  use Sequin.DataCase, async: true

  alias Sequin.Databases
  alias Sequin.Databases.ConnectionCache
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Databases.PostgresDatabaseTable
  alias Sequin.Error.ValidationError
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Replication.PostgresReplicationSlot
  alias Sequin.TestSupport.Models.Character

  describe "list_dbs" do
    test "returns empty list if no databases exist" do
      assert Databases.list_dbs() == []
    end

    test "returns all databases" do
      DatabasesFactory.insert_postgres_database!()
      assert length(Databases.list_dbs()) == 1
    end
  end

  describe "list_active_dbs" do
    test "returns empty list if no databases exist" do
      assert Databases.list_active_dbs() == []
    end

    test "returns all databases with active replication slots" do
      account = AccountsFactory.insert_account!()
      db = DatabasesFactory.insert_postgres_database!(account_id: account.id)

      slot =
        ReplicationFactory.insert_postgres_replication!(
          account_id: account.id,
          status: :active,
          postgres_database_id: db.id
        )

      db = DatabasesFactory.insert_postgres_database!(account_id: account.id)

      _disabled_slot =
        ReplicationFactory.insert_postgres_replication!(
          account_id: account.id,
          status: :disabled,
          postgres_database_id: db.id
        )

      assert [db] = Databases.list_active_dbs()
      db = Repo.preload(db, :replication_slot)
      assert db.replication_slot.id == slot.id
    end
  end

  describe "tables/1" do
    test "returns tables for a database with existing tables" do
      db = insert_valid_postgres_database(tables: [DatabasesFactory.table()])

      assert {:ok, tables} = Databases.tables(db)
      assert length(tables) > 0
      assert %PostgresDatabaseTable{} = hd(tables)
    end

    test "fetches and returns tables for a database without existing tables" do
      db = insert_valid_postgres_database(tables: [])

      assert {:ok, tables} = Databases.tables(db)
      assert length(tables) > 0
      assert %PostgresDatabaseTable{} = hd(tables)
    end
  end

  describe "update_tables/1" do
    test "updates tables for a database" do
      db = insert_valid_postgres_database()

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
      db = insert_valid_postgres_database()

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
      db = insert_valid_postgres_database(use_local_tunnel: true, port: nil)

      postgrex_opts = PostgresDatabase.to_postgrex_opts(db)

      assert Keyword.get(postgrex_opts, :hostname) == Application.get_env(:sequin, :portal_hostname)
      assert Keyword.get(postgrex_opts, :port) == db.port
    end

    test "uses original hostname and port when not using a local tunnel" do
      db = insert_valid_postgres_database(use_local_tunnel: false)

      postgrex_opts = PostgresDatabase.to_postgrex_opts(db)

      assert Keyword.get(postgrex_opts, :hostname) == db.hostname
      assert Keyword.get(postgrex_opts, :port) == db.port
    end
  end

  describe "create_db_with_slot/3" do
    setup do
      account = AccountsFactory.insert_account!()
      {:ok, account: account}
    end

    test "successfully creates a database and replication slot", %{account: account} do
      db_attrs = DatabasesFactory.configured_postgres_database_attrs()
      slot_attrs = ReplicationFactory.configured_postgres_replication_attrs()

      assert {:ok, db} = Databases.create_db_with_slot(account.id, db_attrs, slot_attrs)

      assert db.id
      assert %Sequin.Replication.PostgresReplicationSlot{} = db.replication_slot
      assert db.replication_slot.id

      # Verify they exist in DB
      assert [db] = Repo.all(PostgresDatabase)
      assert [slot] = Repo.all(PostgresReplicationSlot)

      assert slot.postgres_database_id == db.id
      assert db.name == db_attrs.name
      assert slot.slot_name == slot_attrs.slot_name
      assert slot.publication_name == slot_attrs.publication_name
    end

    test "returns error and rolls back if db creation fails", %{account: account} do
      db_attrs = DatabasesFactory.configured_postgres_database_attrs(name: nil)
      slot_attrs = ReplicationFactory.configured_postgres_replication_attrs()

      assert {:error, %ValidationError{}} = Databases.create_db_with_slot(account.id, db_attrs, slot_attrs)
      assert Repo.all(PostgresDatabase) == []
      assert Repo.all(PostgresReplicationSlot) == []
    end

    test "returns error and rolls back if slot creation fails", %{account: account} do
      db_attrs = DatabasesFactory.configured_postgres_database_attrs()
      slot_attrs = ReplicationFactory.configured_postgres_replication_attrs(slot_name: nil)

      assert {:error, _error} = Databases.create_db_with_slot(account.id, db_attrs, slot_attrs)
      assert Repo.all(PostgresDatabase) == []
      assert Repo.all(PostgresReplicationSlot) == []
    end
  end

  describe "update_db_with_slot/3" do
    setup do
      account = AccountsFactory.insert_account!()
      db = DatabasesFactory.insert_postgres_database!(account_id: account.id)

      _slot =
        ReplicationFactory.insert_postgres_replication!(postgres_database_id: db.id, account_id: account.id)

      db = Repo.preload(db, :replication_slot)
      {:ok, account: account, db: db}
    end

    test "successfully updates a database and replication slot", %{db: db} do
      new_db_attrs = DatabasesFactory.configured_postgres_database_attrs()
      new_slot_attrs = ReplicationFactory.configured_postgres_replication_attrs()
      db_attrs = %{name: new_db_attrs.name}
      slot_attrs = %{slot_name: new_slot_attrs.slot_name, publication_name: new_slot_attrs.publication_name}

      assert {:ok, updated_db} = Databases.update_db_with_slot(db, db_attrs, slot_attrs)

      assert updated_db.name == new_db_attrs.name
      assert updated_db.replication_slot.slot_name == new_slot_attrs.slot_name
      assert updated_db.replication_slot.publication_name == new_slot_attrs.publication_name

      # Verify changes in DB
      reloaded_db = Repo.get!(PostgresDatabase, db.id)
      reloaded_slot = Repo.get!(Sequin.Replication.PostgresReplicationSlot, db.replication_slot.id)
      assert reloaded_db.name == new_db_attrs.name
      assert reloaded_slot.slot_name == new_slot_attrs.slot_name
      assert reloaded_slot.publication_name == new_slot_attrs.publication_name
    end

    test "returns error and rolls back if db update fails", %{db: db} do
      original_name = db.name
      original_slot_name = db.replication_slot.slot_name

      db_attrs = DatabasesFactory.postgres_database_attrs(name: 1)
      slot_attrs = ReplicationFactory.configured_postgres_replication_attrs()

      assert {:error, %ValidationError{}} = Databases.update_db_with_slot(db, db_attrs, slot_attrs)

      reloaded_db = Repo.get!(PostgresDatabase, db.id)
      reloaded_slot = Repo.get!(Sequin.Replication.PostgresReplicationSlot, db.replication_slot.id)
      assert reloaded_db.name == original_name
      assert reloaded_slot.slot_name == original_slot_name
    end

    test "returns error and rolls back if slot update fails", %{db: db} do
      original_name = db.name
      original_slot_name = db.replication_slot.slot_name

      db_attrs = DatabasesFactory.configured_postgres_database_attrs()
      slot_attrs = ReplicationFactory.postgres_replication_attrs(slot_name: 1)

      assert {:error, %Ecto.Changeset{}} = Databases.update_db_with_slot(db, db_attrs, slot_attrs)

      reloaded_db = Repo.get!(PostgresDatabase, db.id)
      reloaded_slot = Repo.get!(Sequin.Replication.PostgresReplicationSlot, db.replication_slot.id)
      assert reloaded_db.name == original_name
      assert reloaded_slot.slot_name == original_slot_name
    end
  end

  defp insert_valid_postgres_database(attrs \\ []) do
    db = DatabasesFactory.insert_configured_postgres_database!(attrs)
    # Avoid creating a new connection for each test
    ConnectionCache.cache_connection(db, Repo)
    db
  end
end

defmodule Sequin.DatabasesTest do
  use Sequin.DataCase, async: true

  alias Sequin.Databases
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Factory.DatabasesFactory

  describe "tables/1" do
    test "returns tables for a database with existing tables" do
      db = DatabasesFactory.insert_configured_postgres_database!(tables: [DatabasesFactory.table()])

      assert {:ok, tables} = Databases.tables(db)
      assert length(tables) > 0
      assert %PostgresDatabase.Table{} = hd(tables)
    end

    test "fetches and returns tables for a database without existing tables" do
      db = DatabasesFactory.insert_configured_postgres_database!(tables: [])

      assert {:ok, tables} = Databases.tables(db)
      assert length(tables) > 0
      assert %PostgresDatabase.Table{} = hd(tables)
    end
  end

  describe "update_tables/1" do
    test "updates tables for a database" do
      db = DatabasesFactory.insert_configured_postgres_database!()

      assert {:ok, updated_db} = Databases.update_tables(db)
      assert length(updated_db.tables) > 0
      assert %PostgresDatabase.Table{} = hd(updated_db.tables)
      assert updated_db.tables_refreshed_at != nil
    end

    test "updates tables and columns for a database" do
      db = DatabasesFactory.insert_configured_postgres_database!()

      assert {:ok, updated_db} = Databases.update_tables(db)
      assert length(updated_db.tables) > 0
      table = hd(updated_db.tables)
      assert %PostgresDatabase.Table{} = table
      assert length(table.columns) > 0
      column = hd(table.columns)
      assert %PostgresDatabase.Table.Column{} = column
      assert is_boolean(column.is_pk?)
    end
  end

  describe "tables/1 and update_tables/1 error handling" do
    @tag capture_log: true
    test "returns an error when unable to connect to the database" do
      db = DatabasesFactory.insert_configured_postgres_database!(hostname: "non_existent_host", tables: [])

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
end

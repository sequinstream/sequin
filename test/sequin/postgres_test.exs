defmodule Sequin.PostgresTest do
  use Sequin.DataCase, async: true

  alias Sequin.Postgres
  alias Sequin.Test.UnboxedRepo

  setup do
    {:ok, conn} = Postgrex.start_link(UnboxedRepo.config())
    %{conn: conn}
  end

  @stream_schema Application.compile_env(:sequin, [Sequin.Repo, :stream_schema_prefix])

  describe "list_schemas/1" do
    test "returns a list of schemas", %{conn: conn} do
      {:ok, schemas} = Postgres.list_schemas(conn)
      assert is_list(schemas)
      assert "public" in schemas
      refute "pg_toast" in schemas
      refute "pg_catalog" in schemas
      refute "information_schema" in schemas
    end
  end

  describe "list_tables/2" do
    test "returns a list of tables in the public schema", %{conn: conn} do
      {:ok, tables} = Postgres.list_tables(conn, "public")
      assert is_list(tables)
      # Assert that some known table exists (adjust as needed for your schema)
      assert "schema_migrations" in tables
    end
  end

  describe "fetch_table_oid/3" do
    test "returns the OID for a known table", %{conn: conn} do
      oid = Postgres.fetch_table_oid(conn, "public", "schema_migrations")
      assert is_integer(oid)
    end

    test "returns nil for an unknown table", %{conn: conn} do
      oid = Postgres.fetch_table_oid(conn, "public", "non_existent_table")
      assert is_nil(oid)
    end
  end

  describe "list_columns/3" do
    test "returns a list of columns for a known table", %{conn: conn} do
      {:ok, columns} = Postgres.list_columns(conn, "public", "schema_migrations")
      assert is_list(columns)
      assert length(columns) > 0
      [first_column | _] = columns
      assert length(first_column) == 4
      assert [attnum, name, type, is_pk] = first_column
      assert is_integer(attnum)
      assert is_binary(name)
      assert is_binary(type)
      assert is_boolean(is_pk)
    end

    test "returns correct columns for a partitioned table without duplicates", %{conn: conn} do
      {:ok, columns} = Postgres.list_columns(conn, @stream_schema, "consumer_events")
      assert is_list(columns)
      assert length(columns) > 0

      # Check for no duplicate columns
      unique_columns = Enum.uniq_by(columns, fn [attnum, name, _, _] -> {attnum, name} end)
      assert length(unique_columns) == length(columns), "Duplicate columns were found"

      # Check for expected columns
      column_names = Enum.map(columns, fn [_, name, _, _] -> name end)
      assert "consumer_id" in column_names
      assert "id" in column_names
      assert "commit_lsn" in column_names

      # Check primary key
      pk_columns = Enum.filter(columns, fn [_, _, _, is_pk] -> is_pk end)
      assert length(pk_columns) > 0
    end

    test "returns an error for an unknown table", %{conn: conn} do
      result = Postgres.list_columns(conn, "public", "non_existent_table")
      assert {:error, _} = result
    end
  end

  describe "identifier/2" do
    test "truncates long identifiers" do
      long_name = String.duplicate("a", 100)
      result = Postgres.identifier(long_name)
      assert String.length(result) == 63
    end

    test "handles prefix and suffix" do
      result = Postgres.identifier("table_name", prefix: "prefix", suffix: "suffix")
      assert result == "prefix_table_name_suffix"
    end
  end

  describe "quote_name/1" do
    test "quotes identifiers correctly" do
      assert to_string(Postgres.quote_name("table_name")) == "\"table_name\""
      assert to_string(Postgres.quote_name(:column_name)) == "\"column_name\""
    end
  end
end

defmodule Sequin.PostgresTest do
  use Sequin.DataCase, async: true

  alias Sequin.Error.NotFoundError
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Postgres
  alias Sequin.Repo
  alias Sequin.Test.UnboxedRepo

  setup do
    {:ok, conn} = Postgrex.start_link(UnboxedRepo.config())
    %{conn: conn}
  end

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
      {:ok, oid} = Postgres.fetch_table_oid(conn, "public", "schema_migrations")
      assert is_integer(oid)
    end

    test "returns error for an unknown table", %{conn: conn} do
      assert {:error, %NotFoundError{}} = Postgres.fetch_table_oid(conn, "public", "non_existent_table")
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

  describe "load_rows/2" do
    test "correctly loads and formats rows" do
      table =
        DatabasesFactory.table(
          columns: [
            %{name: "id", type: "uuid", is_pk?: true},
            %{name: "index", type: "int4"},
            %{name: "name", type: "text"},
            %{name: "age", type: "integer"},
            %{name: "optional_uuid", type: "uuid"},
            %{name: "complex_type", type: "complex"}
          ]
        )

      uuid_string = UUID.uuid4()
      uuid = UUID.string_to_binary!(uuid_string)
      optional_uuid_string = UUID.uuid4()
      optional_uuid = UUID.string_to_binary!(optional_uuid_string)
      non_encodable = {:a, :b}

      rows = [
        %{
          "index" => 1,
          "id" => uuid,
          "name" => "Alice",
          "age" => 30,
          "optional_uuid" => optional_uuid,
          "complex_type" => "some value"
        },
        %{
          "index" => 2,
          "id" => uuid,
          "name" => "Bob",
          "age" => 25,
          "optional_uuid" => nil,
          "complex_type" => non_encodable
        }
      ]

      loaded_rows = Postgres.load_rows(table, rows)

      assert [
               %{
                 "index" => 1,
                 "id" => ^uuid_string,
                 "name" => "Alice",
                 "age" => 30,
                 "optional_uuid" => ^optional_uuid_string,
                 "complex_type" => "some value"
               },
               %{
                 "index" => 2,
                 "id" => ^uuid_string,
                 "name" => "Bob",
                 "age" => 25,
                 "optional_uuid" => nil,
                 "complex_type" => nil
               }
             ] = Enum.sort_by(loaded_rows, & &1["index"])

      # Ensure UUIDs are converted to strings
      assert is_binary(hd(loaded_rows)["id"])
      assert is_binary(hd(loaded_rows)["optional_uuid"])

      # Ensure nil UUID is passed through as nil
      assert is_nil(List.last(loaded_rows)["optional_uuid"])

      # Ensure non-JSON encodable value is converted to nil
      assert is_nil(List.last(loaded_rows)["complex_type"])
    end

    test "correctly loads and formats interval values" do
      table =
        DatabasesFactory.table(
          columns: [
            %{name: "id", type: "int4", is_pk?: true},
            %{name: "duration", type: "interval"}
          ]
        )

      # Postgrex.Interval struct: 1 year 2 months 3 days 4:05:06.789
      interval = %Postgrex.Interval{
        months: 14,
        days: 3,
        secs: 14_706,
        microsecs: 789_000
      }

      rows = [
        %{"id" => 1, "duration" => interval},
        %{"id" => 2, "duration" => nil}
      ]

      loaded_rows = Postgres.load_rows(table, rows)

      assert [
               %{
                 "id" => 1,
                 "duration" => %{
                   "months" => 14,
                   "days" => 3,
                   "microseconds" => 14_706_789_000
                 }
               },
               %{
                 "id" => 2,
                 "duration" => nil
               }
             ] = Enum.sort_by(loaded_rows, & &1["id"])
    end
  end

  describe "verify_table_in_publication/3" do
    test "correctly identifies tables in and out of the publication", %{conn: conn} do
      # Tables that should be in the publication
      {:ok, table_oid} = Postgres.fetch_table_oid(conn, "public", "Characters")

      assert :ok ==
               Postgres.verify_table_in_publication(
                 conn,
                 "characters_publication",
                 table_oid
               )

      {:ok, table_oid} = Postgres.fetch_table_oid(conn, "public", "test_event_logs")
      # Tables that should not be in the publication
      assert {:error, %NotFoundError{}} =
               Postgres.verify_table_in_publication(
                 conn,
                 "characters_publication",
                 table_oid
               )

      # Non-existent table
      assert {:error, %NotFoundError{}} =
               Postgres.verify_table_in_publication(conn, "characters_publication", 4_294_967_295)

      {:ok, table_oid} = Postgres.fetch_table_oid(conn, "public", "Characters")
      # Non-existent publication
      assert {:error, %NotFoundError{}} =
               Postgres.verify_table_in_publication(
                 conn,
                 "non_existent_publication",
                 table_oid
               )
    end
  end

  describe "check_partitioned_replica_identity/2" do
    @table "postgres_test_test_partitioned"
    setup do
      # Create a partitioned table and its partitions
      """
      drop table if exists #{@table} cascade;

      create table #{@table} (
        id serial,
        value int,
        created_at timestamp not null
      ) partition by range (created_at);

      create table #{@table}_2021 partition of #{@table}
        for values from ('2021-01-01') to ('2022-01-01');

      create table #{@table}_2022 partition of #{@table}
        for values from ('2022-01-01') to ('2023-01-01');
      """
      |> String.split(";")
      |> Enum.each(&Repo.query!(&1))

      # Get the OIDs for cleanup and testing
      {:ok, %{rows: [[parent_oid]]}} = Repo.query("select oid from pg_class where relname = '#{@table}'")

      %{parent_oid: parent_oid}
    end

    test "returns the same replica identity when all partitions match", %{parent_oid: parent_oid} do
      # By default, all partitions inherit the same replica identity (default)
      {:ok, identity} = Postgres.check_partitioned_replica_identity(Repo, parent_oid)
      assert identity == :default

      # Change parent, will come back as :mixed
      Repo.query!("alter table #{@table} replica identity full")

      {:ok, identity} = Postgres.check_partitioned_replica_identity(Repo, parent_oid)
      assert identity == :mixed

      # Change children
      Repo.query!("alter table #{@table}_2021 replica identity full")
      Repo.query!("alter table #{@table}_2022 replica identity full")

      {:ok, identity} = Postgres.check_partitioned_replica_identity(Repo, parent_oid)
      assert identity == :full
    end

    test "returns error for non-existent table", %{conn: conn} do
      non_existent_oid = 999_999_999

      assert {:error, %NotFoundError{}} = Postgres.check_partitioned_replica_identity(conn, non_existent_oid)
    end
  end

  describe "create_publication/3" do
    test "creates a publication with default SQL when init_sql is not provided" do
      pub_name = "test_pub"

      # Publication shouldn't exist yet
      assert {:error, _} = Postgres.get_publication(Repo, pub_name)

      # Create publication
      assert :ok = Postgres.create_publication(Repo, pub_name)

      # Verify publication exists
      {:ok, pub_info} = Postgres.get_publication(Repo, pub_name)
      assert pub_info["pubname"] == pub_name
    end

    test "creates a publication with custom init_sql" do
      pub_name = "test_pub_custom"
      init_sql = "CREATE PUBLICATION test_pub_custom FOR TABLES IN SCHEMA public WITH (publish = 'insert, update')"

      # Publication shouldn't exist yet
      assert {:error, _} = Postgres.get_publication(Repo, pub_name)

      # Create publication
      assert :ok = Postgres.create_publication(Repo, pub_name, init_sql)

      # Verify publication exists with correct properties
      {:ok, pub_info} = Postgres.get_publication(Repo, pub_name)
      assert pub_info["pubname"] == pub_name
      assert pub_info["pubinsert"] == true
      assert pub_info["pubupdate"] == true
      # Should be false because we only specified insert, update
      assert pub_info["pubdelete"] == false
    end

    test "returns error for invalid init_sql" do
      pub_name = "test_pub_invalid"
      invalid_sql = "CREATE PUBLICATION wrong_name FOR TABLES IN SCHEMA public"

      result = Postgres.create_publication(Repo, pub_name, invalid_sql)
      assert {:error, error} = result
      assert error.message =~ "Invalid publication `init_sql`"

      # Verify publication wasn't created
      assert {:error, _} = Postgres.get_publication(Repo, pub_name)
    end

    test "returns :ok for already existing publication" do
      pub_name = "test_pub_existing"

      # Create the publication first
      :ok = Postgres.create_publication(Repo, pub_name)

      # Try to create it again
      assert :ok = Postgres.create_publication(Repo, pub_name)
    end
  end
end

defmodule SequinWeb.YamlControllerTest do
  use SequinWeb.ConnCase, async: true

  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Databases.Sequence
  alias Sequin.Test.Support.ReplicationSlots
  alias Sequin.Test.UnboxedRepo

  @moduletag :unboxed

  @publication "characters_publication"

  def replication_slot, do: ReplicationSlots.slot_name(__MODULE__)

  setup :authenticated_conn

  setup do
    Application.put_env(:sequin, :self_hosted, true)

    # Fast-forward the replication slot to the current WAL position
    :ok = ReplicationSlots.reset_slot(UnboxedRepo, replication_slot())

    :ok
  end

  describe "plan/2" do
    test "returns planned resources for valid yaml", %{conn: conn} do
      yaml = """
      users:
        - email: "admin@sequinstream.com"
          password: "sequinpassword!"

      databases:
        - name: "test-db"
          username: "postgres"
          password: "postgres"
          hostname: "localhost"
          port: 5432
          database: "sequin_test"
          slot_name: "#{replication_slot()}"
          publication_name: "#{@publication}"
          pool_size: 10

      sequences:
        - name: "characters"
          database: "test-db"
          table_schema: "public"
          table_name: "Characters"
          sort_column_name: "updated_at"
      """

      conn = post(conn, ~p"/api/config/plan", %{yaml: yaml})

      assert %{
               "changes" => [
                 %{
                   "action" => "create",
                   "resource_type" => "user",
                   "new" => %{
                     "email" => "admin@sequinstream.com",
                     "id" => user_id
                   },
                   "old" => nil
                 },
                 %{
                   "action" => "create",
                   "resource_type" => "database",
                   "new" => %{
                     "database" => "sequin_test",
                     "hostname" => "localhost",
                     "ipv6" => false,
                     "name" => "test-db",
                     "password" => "********",
                     "pool_size" => 10,
                     "port" => 5432,
                     "ssl" => false,
                     "use_local_tunnel" => false,
                     "username" => "postgres",
                     "id" => postgres_database_id
                   },
                   "old" => nil
                 },
                 %{
                   "action" => "create",
                   "resource_type" => "sequence",
                   "new" => %{
                     "name" => "characters",
                     "sort_column_name" => "updated_at",
                     "table_name" => "Characters",
                     "table_schema" => "public",
                     "id" => sequence_id
                   },
                   "old" => nil
                 },
                 %{
                   "action" => "update",
                   "new" => %{
                     "id" => account_id,
                     "name" => account_name
                   },
                   "old" => %{
                     "id" => account_id,
                     "name" => account_name
                   },
                   "resource_type" => "account"
                 }
               ]
             } = json_response(conn, 200)

      assert Sequin.String.is_uuid?(account_id)
      assert Sequin.String.is_uuid?(user_id)
      assert Sequin.String.is_uuid?(postgres_database_id)
      assert Sequin.String.is_uuid?(sequence_id)
    end

    test "returns error for invalid yaml", %{conn: conn} do
      yaml = """
      databases:
        - name: "test-db"
      """

      conn = post(conn, ~p"/api/config/plan", %{yaml: yaml})

      assert json_response(conn, 400) == %{
               "summary" => "Error creating database 'test-db': - database: can't be blank"
             }
    end
  end

  describe "apply/2" do
    test "successfully applies valid yaml configuration", %{conn: conn} do
      yaml = """
      users:
        - email: "admin@sequinstream.com"
          password: "sequinpassword!"

      databases:
        - name: "test-db"
          username: "postgres"
          password: "postgres"
          hostname: "localhost"
          port: 5432
          database: "sequin_test"
          slot_name: "#{replication_slot()}"
          publication_name: "#{@publication}"
          pool_size: 10

      change_capture_pipelines:
        - name: "characters"
          source_database: "test-db"
          source_table_schema: "public"
          source_table_name: "Characters"
          destination_database: "test-db"
          destination_table_schema: "public"
          destination_table_name: "Characters"

      sequences:
        - name: "characters"
          database: "test-db"
          table_schema: "public"
          table_name: "Characters"
          sort_column_name: "updated_at"
      """

      conn = post(conn, ~p"/api/config/apply", %{yaml: yaml})

      assert %{
               "resources" => [
                 %{
                   "id" => account_id,
                   "name" => account_name,
                   "inserted_at" => _,
                   "updated_at" => _
                 },
                 %{
                   "auth_provider" => "identity",
                   "email" => "admin@sequinstream.com",
                   "id" => user_id,
                   "auth_provider_id" => nil,
                   "inserted_at" => _,
                   "name" => nil,
                   "updated_at" => _
                 },
                 %{
                   "database" => "sequin_test",
                   "hostname" => "localhost",
                   "id" => database_id,
                   "name" => "test-db",
                   "ipv6" => false,
                   "password" => "postgres",
                   "pool_size" => 10,
                   "port" => 5432,
                   "ssl" => false,
                   "use_local_tunnel" => false,
                   "username" => "postgres"
                 },
                 %{
                   "destination_database_id" => database_id,
                   "destination_oid" => _,
                   "id" => wal_pipeline_id,
                   "name" => "characters",
                   "seq" => _,
                   "source_tables" => [
                     %{
                       "actions" => ["insert", "update", "delete"],
                       "column_filters" => [],
                       "group_column_attnums" => nil,
                       "oid" => _,
                       "schema_name" => nil,
                       "sort_column_attnum" => nil,
                       "table_name" => nil
                     }
                   ],
                   "status" => "active"
                 },
                 %{
                   "id" => sequence_id,
                   "name" => "characters",
                   "sort_column_attnum" => sort_column_attnum,
                   "sort_column_name" => "updated_at",
                   "table_name" => "Characters",
                   "table_oid" => table_oid,
                   "table_schema" => "public"
                 }
               ]
             } = json_response(conn, 200)

      assert is_binary(account_name)
      assert Sequin.String.is_uuid?(account_id)
      assert Sequin.String.is_uuid?(user_id)
      assert Sequin.String.is_uuid?(database_id)
      assert Sequin.String.is_uuid?(sequence_id)
      assert Sequin.String.is_uuid?(wal_pipeline_id)
      assert is_integer(sort_column_attnum)
      assert is_integer(table_oid)
    end

    test "returns error for invalid yaml", %{conn: conn} do
      yaml = """
      databases:
        - name: "test-db"
      """

      conn = post(conn, ~p"/api/config/apply", %{yaml: yaml})

      assert json_response(conn, 400) == %{
               "summary" => "Error creating database 'test-db': - database: can't be blank"
             }
    end
  end

  describe "export/2" do
    test "returns yaml representation of existing resources", %{conn: conn} do
      # First apply some configuration
      yaml = """
      users:
        - email: "admin@sequinstream.com"
          password: "sequinpassword!"

      databases:
        - name: "test-db"
          username: "postgres"
          password: "postgres"
          hostname: "localhost"
          port: 5432
          database: "sequin_test"
          slot_name: "#{replication_slot()}"
          publication_name: "#{@publication}"
          pool_size: 10

      sequences:
        - name: "characters"
          database: "test-db"
          table_schema: "public"
          table_name: "Characters"
          sort_column_name: "updated_at"
      """

      # Apply the configuration first
      post(conn, ~p"/api/config/apply", %{yaml: yaml})

      # Now test the export endpoint
      conn = get(conn, ~p"/api/config/export")

      assert %{"yaml" => exported_yaml} = json_response(conn, 200)

      [database] = Repo.all(PostgresDatabase)
      [sequence] = Repo.all(Sequence)

      # Parse the exported YAML to verify its structure
      parsed_yaml = YamlElixir.read_from_string!(exported_yaml)

      assert get_in(parsed_yaml, ["databases", Access.at(0)]) == %{
               "id" => database.id,
               "database" => "sequin_test",
               "hostname" => "localhost",
               "name" => "test-db",
               "password" => "********",
               "pool_size" => 10,
               "port" => 5432,
               "publication_name" => @publication,
               "slot_name" => replication_slot(),
               "ssl" => false,
               "ipv6" => false,
               "use_local_tunnel" => false,
               "username" => "postgres"
             }

      assert get_in(parsed_yaml, ["sequences", Access.at(0)]) == %{
               "id" => sequence.id,
               "database" => "test-db",
               "name" => "characters",
               "sort_column_name" => "updated_at",
               "table_name" => "Characters",
               "table_schema" => "public"
             }
    end
  end
end

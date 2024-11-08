defmodule SequinWeb.YamlControllerTest do
  use SequinWeb.ConnCase, async: true

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
                     "auth_provider" => "identity",
                     "auth_provider_id" => nil,
                     "email" => "admin@sequinstream.com",
                     "name" => nil,
                     "id" => user_id
                   },
                   "old" => nil
                 },
                 %{
                   "action" => "create",
                   "resource_type" => "postgres_database",
                   "new" => %{
                     "database" => "sequin_test",
                     "hostname" => "localhost",
                     "ipv6" => false,
                     "name" => "test-db",
                     "password" => "postgres",
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
                     "sort_column_attnum" => sort_column_attnum,
                     "sort_column_name" => "updated_at",
                     "table_name" => "Characters",
                     "table_oid" => table_oid,
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
      assert is_integer(table_oid)
      assert is_integer(sort_column_attnum)
    end

    test "returns error for invalid yaml", %{conn: conn} do
      yaml = """
      databases:
        - name: "test-db"
      """

      conn = post(conn, ~p"/api/config/plan", %{yaml: yaml})

      assert json_response(conn, 422) == %{
               "code" => nil,
               "summary" => nil,
               "validation_errors" => %{
                 "database" => ["can't be blank"]
               }
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
                   "name" => account_name
                 },
                 %{
                   "auth_provider" => "identity",
                   "email" => "admin@sequinstream.com",
                   "id" => user_id
                 },
                 %{
                   "database" => "sequin_test",
                   "hostname" => "localhost",
                   "id" => database_id,
                   "name" => "test-db"
                 },
                 %{
                   "id" => sequence_id,
                   "name" => "characters",
                   "sort_column_name" => "updated_at",
                   "table_name" => "Characters",
                   "table_schema" => "public"
                 }
               ]
             } = json_response(conn, 200)

      assert is_binary(account_name)
      assert Sequin.String.is_uuid?(account_id)
      assert Sequin.String.is_uuid?(user_id)
      assert Sequin.String.is_uuid?(database_id)
      assert Sequin.String.is_uuid?(sequence_id)
    end

    test "returns error for invalid yaml", %{conn: conn} do
      yaml = """
      databases:
        - name: "test-db"
      """

      conn = post(conn, ~p"/api/config/apply", %{yaml: yaml})

      assert json_response(conn, 422) == %{
               "code" => nil,
               "summary" => nil,
               "validation_errors" => %{
                 "database" => ["can't be blank"]
               }
             }
    end
  end
end

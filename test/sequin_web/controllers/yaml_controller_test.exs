defmodule SequinWeb.YamlControllerTest do
  use SequinWeb.ConnCase, async: true

  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Test.UnboxedRepo
  alias Sequin.TestSupport.Models.Character
  alias Sequin.TestSupport.ReplicationSlots

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
          tables:
            - table_name: "Characters"
              table_schema: "public"
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
                     "password" => "p******s",
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

      assert Sequin.String.uuid?(account_id)
      assert Sequin.String.uuid?(user_id)
      assert Sequin.String.uuid?(postgres_database_id)
    end

    test "returns error for invalid yaml", %{conn: conn} do
      yaml = """
      databases:
        - name: "test-db"
          port: not-a-port
      """

      conn = post(conn, ~p"/api/config/plan", %{yaml: yaml})

      assert json_response(conn, 400) == %{
               "summary" =>
                 "Error creating database 'test-db': \n- port: is invalid\n- hostname: can't be blank\n- database: can't be blank"
             }
    end

    test "successfully plans configuration with wider set of fields", %{conn: conn} do
      yaml = """
      change_retentions:
        - name: test_retention
          filters: []
          destination_database: sequin_test
          source_database: sequin_test
          actions:
            - insert
            - update
            - delete
          source_table_name: #{Character.table_name()}
          source_table_schema: public
          destination_table_name: sequin_events
          destination_table_schema: public
      databases:
        - name: sequin_test
          port: 5432
          ssl: false
          ipv6: false
          hostname: localhost
          pool_size: 10
          username: postgres
          password: postgres
          database: sequin_test
          slot_name: "#{replication_slot()}"
          use_local_tunnel: false
          publication_name: "#{@publication}"
      http_endpoints:
        - name: test_http_endpoint
          url: http://localhost:4000/something
          headers: {}
      sinks:
        - name: accounts_sink
          status: active
          destination:
            port: 4222
            type: nats
            host: localhost
            tls: false
          database: sequin_test
          transform: record-transform
          active_backfill:
          batch_size: 1
          load_shedding_policy: pause_on_full
          max_retry_count:
          timestamp_format: iso8601
          actions:
            - insert
            - update
            - delete
      transforms:
        - name: record-transform
          type: path
          path: record
          description: Extracts just the record from the Sequin message shape.
      """

      conn = post(conn, ~p"/api/config/plan", %{yaml: yaml})

      response = json_response(conn, 200)
      assert %{"changes" => changes} = response
      assert is_list(changes)
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

      change_retentions:
        - name: "characters"
          source_database: "test-db"
          source_table_schema: "public"
          source_table_name: "Characters"
          destination_database: "test-db"
          destination_table_schema: "public"
          destination_table_name: "Characters"
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
                       "table_name" => nil
                     }
                   ],
                   "status" => "active"
                 }
               ]
             } = json_response(conn, 200)

      assert is_binary(account_name)
      assert Sequin.String.uuid?(account_id)
      assert Sequin.String.uuid?(user_id)
      assert Sequin.String.uuid?(database_id)
      assert Sequin.String.uuid?(wal_pipeline_id)
    end

    test "returns nice error when table doesnt exist", %{conn: conn} do
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

      http_endpoints:
        - name: "sequin-playground-webhook"
          url: "https://example.com/webhook"

      sinks:
        - name: "sequin-playground-webhook"
          database: "test-db"
          source:
            include_tables: ["does not exist"]
          destination:
            type: "webhook"
            http_endpoint: "sequin-playground-webhook"
      """

      conn = post(conn, ~p"/api/config/apply", %{yaml: yaml})

      %{"summary" => summary} = json_response(conn, 422)
      assert summary =~ "Table 'does not exist' not found in database 'test-db'"
    end

    test "returns error for invalid yaml", %{conn: conn} do
      yaml = """
      databases:
        - name: "test-db"
      """

      conn = post(conn, ~p"/api/config/apply", %{yaml: yaml})

      assert json_response(conn, 400) == %{
               "summary" => "Error creating database 'test-db': \n- hostname: can't be blank\n- database: can't be blank"
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

      http_endpoints:
        - name: "sequin-playground-webhook"
          url: "https://example.com/webhook"

      sinks:
        - name: "sequin-playground-webhook"
          database: "test-db"
          destination:
            type: "webhook"
            http_endpoint: "sequin-playground-webhook"
      """

      # Apply the configuration first
      assert conn |> post(~p"/api/config/apply", %{yaml: yaml}) |> json_response(200)

      # Now test the export endpoint
      conn = get(conn, ~p"/api/config/export")

      assert %{"yaml" => exported_yaml} = json_response(conn, 200)

      [_database] = Repo.all(PostgresDatabase)

      # Parse the exported YAML to verify its structure
      parsed_yaml = YamlElixir.read_from_string!(exported_yaml)

      assert get_in(parsed_yaml, ["databases", Access.at(0)]) == %{
               "database" => "sequin_test",
               "hostname" => "localhost",
               "name" => "test-db",
               "password" => "p******s",
               "pool_size" => 10,
               "port" => 5432,
               "publication" => %{"name" => @publication},
               "slot" => %{"name" => replication_slot()},
               "ssl" => false,
               "ipv6" => false,
               "use_local_tunnel" => false,
               "username" => "postgres"
             }

      assert %{
               "name" => "sequin-playground-webhook",
               "url" => "https://example.com/webhook"
             } = get_in(parsed_yaml, ["http_endpoints", Access.at(0)])

      assert %{
               "name" => "sequin-playground-webhook",
               "database" => "test-db",
               "destination" => %{
                 "type" => "webhook",
                 "http_endpoint" => "sequin-playground-webhook"
               }
             } = get_in(parsed_yaml, ["sinks", Access.at(0)])
    end
  end

  describe "apply_from_yml!/1" do
    test "returns error for invalid yaml", %{conn: conn} do
      yaml = """
      ---
      - -
      databases:
        - name: "test-db"
      """

      assert conn |> post(~p"/api/config/apply", %{yaml: yaml}) |> json_response(400)
    end
  end
end

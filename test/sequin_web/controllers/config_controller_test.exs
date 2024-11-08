defmodule SequinWeb.ConfigControllerTest do
  use SequinWeb.ConnCase, async: true

  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory

  setup :authenticated_conn

  describe "plan_yaml" do
    test "returns changes when yaml differs from database", %{conn: conn, account: account} do
      # Create existing database in different state
      DatabasesFactory.insert_postgres_database!(
        account_id: account.id,
        name: "test-db",
        pool_size: 3
      )

      yaml = """
      account:
        name: "#{account.name}"

      databases:
        - name: "test-db"
          username: "postgres"
          password: "postgres"
          hostname: "localhost"
          database: "sequin_test"
          pool_size: 5
      """

      conn = post(conn, ~p"/api/config/plan/yaml", %{yaml: yaml})

      assert %{"data" => [changeset]} = json_response(conn, 200)
      assert changeset["action"] == "update"
      assert changeset["data"]["name"] == "test-db"
      assert changeset["changes"]["pool_size"] == 5
    end

    test "returns create action for new entities", %{conn: conn} do
      yaml = """
      account:
        name: "Test Account"

      http_endpoints:
        - name: "new-endpoint"
          url: "https://example.com/webhook"
      """

      conn = post(conn, ~p"/api/config/plan/yaml", %{yaml: yaml})

      assert %{"data" => [changeset]} = json_response(conn, 200)
      assert changeset["action"] == "create"
      assert changeset["changes"]["name"] == "new-endpoint"
      assert changeset["changes"]["host"] == "example.com"
      assert changeset["changes"]["path"] == "/webhook"
      refute changeset["data"]["id"]
      refute changeset["data"]["name"]
    end

    test "returns validation errors for invalid config", %{conn: conn} do
      yaml = """
      account:
        name: "Test Account"

      databases:
        - name: "invalid-db"
      """

      conn = post(conn, ~p"/api/config/plan/yaml", %{yaml: yaml})

      assert %{"validation_errors" => [changeset]} = json_response(conn, 422)
      assert changeset["database"] == ["can't be blank"]
      assert changeset["username"] == ["can't be blank"]
    end

    test "handles multiple entity types in same yaml", %{conn: conn, account: account} do
      # Create existing endpoint in different state
      ConsumersFactory.insert_http_endpoint!(
        account_id: account.id,
        name: "test-endpoint",
        port: 8080
      )

      yaml = """
      account:
        name: "#{account.name}"

      databases:
        - name: "new-db"
          username: "postgres"
          password: "postgres"
          hostname: "localhost"
          database: "sequin_test"
          pool_size: 5

      http_endpoints:
        - name: "test-endpoint"
          url: "https://example.com:443/webhook"
      """

      conn = post(conn, ~p"/api/config/plan/yaml", %{yaml: yaml})

      assert %{"data" => changesets} = json_response(conn, 200)
      assert length(changesets) == 2

      [db_changeset, endpoint_changeset] = changesets

      # Check database creation
      assert db_changeset["action"] == "create"
      assert db_changeset["changes"]["name"] == "new-db"

      # Check endpoint update
      assert endpoint_changeset["action"] == "update"
      assert endpoint_changeset["data"]["name"] == "test-endpoint"
      assert endpoint_changeset["changes"]["port"] == 443
    end

    test "handles http push consumer configuration", %{conn: conn, account: account} do
      # Create existing consumer in different state
      endpoint = ConsumersFactory.insert_http_endpoint!(account_id: account.id, name: "test-endpoint")
      slot = ReplicationFactory.insert_postgres_replication!(account_id: account.id)

      ConsumersFactory.insert_http_push_consumer!(
        account_id: account.id,
        name: "test-consumer",
        http_endpoint_id: endpoint.id,
        replication_slot_id: slot.id,
        max_waiting: 10
      )

      yaml = """
      account:
        name: "#{account.name}"

      http_push_consumers:
        - name: "test-consumer"
          max_waiting: 20
          http_endpoint_id: "#{endpoint.id}"
          replication_slot_id: "#{slot.id}"
      """

      conn = post(conn, ~p"/api/config/plan/yaml", %{yaml: yaml})

      assert %{"data" => [changeset]} = json_response(conn, 200)
      assert changeset["action"] == "update"
      assert changeset["data"]["name"] == "test-consumer"
      assert changeset["changes"]["max_waiting"] == 20
    end

    test "handles http pull consumer configuration", %{conn: conn, account: account} do
      # Create existing consumer in different state
      slot = ReplicationFactory.insert_postgres_replication!(account_id: account.id)

      ConsumersFactory.insert_http_pull_consumer!(
        account_id: account.id,
        name: "test-pull-consumer",
        replication_slot_id: slot.id,
        max_ack_pending: 5000
      )

      yaml = """
      account:
        name: "#{account.name}"

      http_pull_consumers:
        - name: "test-pull-consumer"
          max_ack_pending: 10000
          replication_slot_id: "#{slot.id}"
      """

      conn = post(conn, ~p"/api/config/plan/yaml", %{yaml: yaml})

      assert %{"data" => [changeset]} = json_response(conn, 200)
      assert changeset["action"] == "update"
      assert changeset["data"]["name"] == "test-pull-consumer"
      assert changeset["changes"]["max_ack_pending"] == 10_000
    end

    test "handles wal pipeline configuration", %{conn: conn, account: account} do
      # Create existing pipeline in different state
      source_db = DatabasesFactory.insert_postgres_database!(account_id: account.id)
      dest_db = DatabasesFactory.insert_postgres_database!(account_id: account.id)

      slot =
        ReplicationFactory.insert_postgres_replication!(
          account_id: account.id,
          postgres_database_id: source_db.id
        )

      ReplicationFactory.insert_wal_pipeline!(
        account_id: account.id,
        name: "test-pipeline",
        replication_slot_id: slot.id,
        destination_database_id: dest_db.id,
        destination_oid: 12_345,
        status: :disabled
      )

      yaml = """
      account:
        name: "#{account.name}"

      wal_pipelines:
        - name: "test-pipeline"
          status: "active"
          replication_slot_id: "#{slot.id}"
          destination_database_id: "#{dest_db.id}"
          destination_oid: 12345
      """

      conn = post(conn, ~p"/api/config/plan/yaml", %{yaml: yaml})

      assert %{"data" => [changeset]} = json_response(conn, 200)
      assert changeset["action"] == "update"
      assert changeset["data"]["name"] == "test-pipeline"
      assert changeset["changes"]["status"] == "active"
    end

    test "handles multiple entity types together", %{conn: conn, account: account} do
      # Create existing entities
      endpoint = ConsumersFactory.insert_http_endpoint!(account_id: account.id, name: "test-endpoint")
      slot = ReplicationFactory.insert_postgres_replication!(account_id: account.id)
      dest_db = DatabasesFactory.insert_postgres_database!(account_id: account.id)

      yaml = """
      account:
        name: "#{account.name}"

      http_push_consumers:
        - name: "new-consumer"
          http_endpoint_id: "#{endpoint.id}"
          replication_slot_id: "#{slot.id}"

      http_pull_consumers:
        - name: "new-pull-consumer"
          replication_slot_id: "#{slot.id}"

      wal_pipelines:
        - name: "new-pipeline"
          replication_slot_id: "#{slot.id}"
          destination_database_id: "#{dest_db.id}"
          destination_oid: 12345
      """

      conn = post(conn, ~p"/api/config/plan/yaml", %{yaml: yaml})

      assert %{"data" => changesets} = json_response(conn, 200)
      assert length(changesets) == 3

      # Verify each type of changeset
      push_consumer = Sequin.Enum.find!(changesets, &(&1["changes"]["name"] == "new-consumer"))
      assert push_consumer["action"] == "create"

      pull_consumer = Sequin.Enum.find!(changesets, &(&1["changes"]["name"] == "new-pull-consumer"))
      assert pull_consumer["action"] == "create"

      pipeline = Sequin.Enum.find!(changesets, &(&1["changes"]["name"] == "new-pipeline"))
      assert pipeline["action"] == "create"
    end
  end
end

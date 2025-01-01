defmodule Sequin.HealthTest do
  use Sequin.DataCase, async: true

  alias Sequin.Error
  alias Sequin.Factory
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ErrorFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Health
  alias Sequin.Health.Check

  describe "initializes a new health" do
    test "initializes a new health" do
      entity = ConsumersFactory.sink_consumer(id: Factory.uuid())
      assert {:ok, %Health{} = health} = Health.get(entity)
      assert health.status == :initializing
      assert is_list(health.checks) and length(health.checks) > 0
    end
  end

  describe "update/4" do
    test "updates the health of an entity with an expected check" do
      entity = ConsumersFactory.sink_consumer(id: Factory.uuid())

      assert :ok = Health.update(entity, :receive, :healthy)
      assert {:ok, %Health{} = health} = Health.get(entity)
      assert health.status == :initializing
      assert Enum.find(health.checks, &(&1.id == :receive)).status == :healthy

      assert :ok = Health.update(entity, :push, :warning)
      assert {:ok, %Health{} = health} = Health.get(entity)
      assert health.status == :warning
      assert Enum.find(health.checks, &(&1.id == :push)).status == :warning
      assert Enum.find(health.checks, &(&1.id == :receive)).status == :healthy

      Enum.each(health.checks, fn check ->
        Health.update(entity, check.id, :healthy)
      end)

      assert {:ok, %Health{} = health} = Health.get(entity)
      assert health.status == :healthy
    end

    test "raises an error for unexpected checks" do
      entity = ConsumersFactory.sink_consumer(id: Factory.uuid())

      assert_raise FunctionClauseError, fn ->
        Health.update(entity, :unexpected_check, :healthy)
      end
    end

    test "sets status to the worst status of any incoming check" do
      entity = ConsumersFactory.sink_consumer(id: Factory.uuid())

      assert :ok = Health.update(entity, :receive, :healthy)
      assert {:ok, %Health{} = health} = Health.get(entity)
      assert health.status == :initializing

      assert :ok = Health.update(entity, :push, :warning)
      assert {:ok, %Health{} = health} = Health.get(entity)
      assert health.status == :warning

      assert :ok = Health.update(entity, :filters, :error, ErrorFactory.random_error())
      assert {:ok, %Health{} = health} = Health.get(entity)
      assert health.status == :error

      assert :ok = Health.update(entity, :receive, :healthy)
      # Still error because other checks are in error state
      assert {:ok, %Health{} = health} = Health.get(entity)
      assert health.status == :error
    end
  end

  describe "get/1" do
    test "finds the health of an entity" do
      entity = ConsumersFactory.sink_consumer(id: Factory.uuid())

      assert :ok = Health.update(entity, :receive, :healthy)

      assert {:ok, %Health{} = health} = Health.get(entity)
      assert health.status == :initializing
    end
  end

  describe "to_external/1" do
    test "converts the health to an external format" do
      entity = ConsumersFactory.sink_consumer(id: Factory.uuid())

      assert {:ok, %Health{} = health} = Health.get(entity)
      assert external = Health.to_external(health)
      assert external.status == :initializing

      assert :ok = Health.update(entity, :receive, :error, ErrorFactory.random_error())

      assert {:ok, %Health{} = health} = Health.get(entity)
      assert external = Health.to_external(health)
      assert external.status == :error
      assert Enum.find(external.checks, &(not is_nil(&1.error)))
    end

    test ":postgres_database :replication_connected check is marked as erroring if it waiting for > 5 minutes" do
      entity = DatabasesFactory.postgres_database(id: Factory.uuid())
      assert {:ok, %Health{} = health} = Health.get(entity)

      ten_minutes_ago = DateTime.add(DateTime.utc_now(), -300, :second)

      health =
        update_in(health.checks, fn checks ->
          Enum.map(checks, fn
            %Check{id: :replication_connected} = check ->
              %{check | created_at: ten_minutes_ago}

            %Check{} = check ->
              check
          end)
        end)

      Health.set_health(health.entity_id, health)

      assert {:ok, %Health{} = health} = Health.get(entity)
      assert external = Health.to_external(health)
      assert external.status == :error
    end
  end

  describe "snapshots" do
    test "get_snapshot returns not found for non-existent snapshot" do
      entity = DatabasesFactory.postgres_database(id: Factory.uuid())
      assert {:error, %Error.NotFoundError{}} = Health.get_snapshot(entity)
    end

    test "upsert_snapshot creates new snapshot for database" do
      entity = DatabasesFactory.postgres_database(id: Factory.uuid())

      # Set initial health
      :ok = Health.update(entity, :replication_connected, :healthy)

      assert {:ok, snapshot} = Health.upsert_snapshot(entity)
      assert snapshot.entity_id == entity.id
      assert snapshot.entity_kind == :postgres_database
      assert snapshot.status == :initializing
      assert is_map(snapshot.health_json)
    end

    test "upsert_snapshot updates existing snapshot" do
      entity = DatabasesFactory.postgres_database(id: Factory.uuid())

      # Set initial health and create snapshot
      :ok = Health.update(entity, :replication_connected, :healthy)
      {:ok, initial_snapshot} = Health.upsert_snapshot(entity)

      # Update health and snapshot
      :ok =
        Health.update(
          entity,
          :replication_connected,
          :error,
          Error.service(message: "test service error", service: :postgres)
        )

      {:ok, updated_snapshot} = Health.upsert_snapshot(entity)

      assert updated_snapshot.id == initial_snapshot.id
      assert updated_snapshot.status == :error
      assert updated_snapshot.sampled_at > initial_snapshot.sampled_at
    end

    test "upsert_snapshot creates new snapshot for consumer" do
      entity = ConsumersFactory.sink_consumer(id: Factory.uuid())

      # Set initial health
      :ok = Health.update(entity, :receive, :healthy)

      assert {:ok, snapshot} = Health.upsert_snapshot(entity)
      assert snapshot.entity_id == entity.id
      assert snapshot.entity_kind == :sink_consumer
      assert snapshot.status == :initializing
      assert is_map(snapshot.health_json)
    end
  end

  describe "on_status_change/3" do
    setup do
      test_pid = self()

      Req.Test.expect(Sequin.Pagerduty, fn conn ->
        {:ok, body, _} = Plug.Conn.read_body(conn)
        send(test_pid, {:req, conn, Jason.decode!(body)})

        Req.Test.json(conn, %{status: "success"})
      end)

      :ok
    end

    test "alerts PagerDuty when database status changes to error" do
      entity =
        DatabasesFactory.postgres_database(
          id: Factory.uuid(),
          name: "test-db",
          account: AccountsFactory.account(),
          account_id: Factory.uuid()
        )

      Health.on_status_change(entity, :healthy, :error)

      assert_receive {:req, conn, body}
      # Let the task finish
      assert_receive {:DOWN, _ref, :process, _pid, _reason}

      assert conn.method == "POST"
      assert conn.path_info == ["v2", "enqueue"]

      assert body["dedup_key"] == "database_health_#{entity.id}"
      assert body["event_action"] == "trigger"
    end

    test "alerts PagerDuty when consumer status changes to warning" do
      entity =
        ConsumersFactory.sink_consumer(
          id: Factory.uuid(),
          name: "test-consumer",
          account: AccountsFactory.account()
        )

      Health.on_status_change(entity, :healthy, :warning)

      assert_receive {:req, conn, body}
      # Let the task finish
      assert_receive {:DOWN, _ref, :process, _pid, _reason}

      assert conn.path_info == ["v2", "enqueue"]
      assert body["dedup_key"] == "consumer_health_#{entity.id}"
      assert body["event_action"] == "trigger"
    end

    test "resolves PagerDuty alert when status changes to healthy" do
      entity =
        DatabasesFactory.postgres_database(
          id: Factory.uuid(),
          name: "test-db",
          account: AccountsFactory.account(),
          account_id: Factory.uuid()
        )

      Health.on_status_change(entity, :error, :healthy)

      assert_receive {:req, conn, body}
      # Let the task finish
      assert_receive {:DOWN, _ref, :process, _pid, _reason}

      assert conn.path_info == ["v2", "enqueue"]
      assert body["dedup_key"] == "database_health_#{entity.id}"
      assert body["event_action"] == "resolve"
    end

    test "skips PagerDuty when entity has ignore_health annotation" do
      entity =
        DatabasesFactory.postgres_database(
          id: Factory.uuid(),
          annotations: %{"ignore_health" => true}
        )

      Req.Test.stub(Sequin.Pagerduty, fn _req ->
        raise "should not be called"
      end)

      Health.on_status_change(entity, :healthy, :error)
    end

    test "skips PagerDuty when account has ignore_health annotation" do
      entity =
        DatabasesFactory.postgres_database(
          id: Factory.uuid(),
          account_id: Factory.uuid(),
          account: AccountsFactory.account(annotations: %{"ignore_health" => true})
        )

      Req.Test.stub(Sequin.Pagerduty, fn _req ->
        raise "should not be called"
      end)

      Health.on_status_change(entity, :healthy, :error)
    end
  end

  describe "update_snapshots/0" do
    setup do
      test_pid = self()

      Req.Test.stub(Sequin.Pagerduty, fn conn ->
        {:ok, body, _} = Plug.Conn.read_body(conn)
        send(test_pid, {:req, conn, Jason.decode!(body)})

        Req.Test.json(conn, %{status: "success"})
      end)

      :ok
    end

    test "snapshots health for active entities" do
      # Create active entities with proper relationships
      db = DatabasesFactory.insert_postgres_database!(name: "test-db")

      slot =
        ReplicationFactory.insert_postgres_replication!(
          status: :active,
          postgres_database_id: db.id,
          account_id: db.account_id
        )

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          status: :active,
          name: "test-consumer",
          replication_slot_id: slot.id,
          account_id: db.account_id
        )

      pipeline =
        ReplicationFactory.insert_wal_pipeline!(
          status: :active,
          name: "test-pipeline",
          replication_slot_id: slot.id,
          account_id: db.account_id
        )

      # Set some health states
      Health.update(db, :replication_connected, :healthy)
      Health.update(consumer, :receive, :warning)
      Health.update(pipeline, :ingestion, :error, ErrorFactory.service_error())

      # Run snapshot update
      :ok = Health.update_snapshots()

      # Verify snapshots were created with correct status
      assert {:ok, db_snapshot} = Health.get_snapshot(db)
      assert db_snapshot.status == :initializing
      assert db_snapshot.entity_kind == :postgres_database

      assert {:ok, consumer_snapshot} = Health.get_snapshot(consumer)
      assert consumer_snapshot.status == :warning
      assert consumer_snapshot.entity_kind == :sink_consumer

      assert {:ok, pipeline_snapshot} = Health.get_snapshot(pipeline)
      assert pipeline_snapshot.status == :error
      assert pipeline_snapshot.entity_kind == :wal_pipeline
    end

    test "skips inactive entities" do
      # Create database with inactive replication slot
      db = DatabasesFactory.insert_postgres_database!(name: "test-db")

      slot =
        ReplicationFactory.insert_postgres_replication!(
          # Disabled slot means no active replication
          status: :disabled,
          postgres_database_id: db.id,
          account_id: db.account_id
        )

      deleted_consumer =
        ConsumersFactory.insert_sink_consumer!(
          status: :disabled,
          name: "deleted-consumer",
          replication_slot_id: slot.id,
          account_id: db.account_id
        )

      # Set some health states
      Health.update(db, :replication_connected, :error, ErrorFactory.service_error())
      Health.update(deleted_consumer, :receive, :error, ErrorFactory.service_error())

      # Run snapshot update
      :ok = Health.update_snapshots()

      # Verify no snapshots were created for inactive entities
      # No snapshot because slot is disabled
      assert {:error, _} = Health.get_snapshot(db)
      # No snapshot because consumer is disabled
      assert {:error, _} = Health.get_snapshot(deleted_consumer)
    end

    test "updates existing snapshots" do
      # Create entity and initial snapshot with proper relationships
      db =
        DatabasesFactory.insert_postgres_database!(name: "test-db")

      slot =
        ReplicationFactory.insert_postgres_replication!(
          status: :active,
          postgres_database_id: db.id,
          account_id: db.account_id
        )

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          status: :active,
          name: "test-consumer",
          replication_slot_id: slot.id,
          account_id: db.account_id
        )

      # Set initial health and create snapshot
      Health.update(consumer, :receive, :healthy)
      {:ok, initial_snapshot} = Health.upsert_snapshot(consumer)

      # Change health status
      Health.update(consumer, :receive, :error, ErrorFactory.service_error())

      # Run snapshot update
      :ok = Health.update_snapshots()

      # Verify snapshot was updated
      {:ok, updated_snapshot} = Health.get_snapshot(consumer)
      assert updated_snapshot.id == initial_snapshot.id
      assert updated_snapshot.status == :error
      assert DateTime.compare(updated_snapshot.sampled_at, initial_snapshot.sampled_at) in [:gt, :eq]

      # Assert that PagerDuty was called
      assert_receive {:req, _conn, _body}
    end
  end
end

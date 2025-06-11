defmodule Sequin.HealthTest do
  use Sequin.DataCase, async: true

  alias Sequin.Error
  alias Sequin.ErrorFactory
  alias Sequin.Factory
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ErrorFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Health
  alias Sequin.Health.Event

  describe "initializes a new health" do
    test "initializes a new health" do
      entity = sink_consumer()
      assert {:ok, %Health{} = health} = Health.health(entity)
      assert health.status == :initializing
      assert is_list(health.checks) and length(health.checks) > 0
    end
  end

  describe "put_event/2" do
    test "updates the health of an entity with a health event" do
      entity = http_endpoint()

      assert {:ok, %Health{} = health} = Health.health(entity)
      assert health.status == :initializing

      assert :ok = Health.put_event(entity, %Event{slug: :endpoint_reachable, status: :success})
      assert {:ok, %Health{} = health} = Health.health(entity)
      assert health.status == :healthy
    end

    test "updates the health of a SinkConsumer with health events" do
      entity = sink_consumer()

      assert {:ok, %Health{} = health} = Health.health(entity)
      assert health.status == :initializing

      assert :ok =
               Health.put_event(entity, %Event{
                 slug: :sink_config_checked,
                 data: %{
                   "tables_with_replica_identities" => [
                     %{
                       "table_oid" => 123,
                       "relation_kind" => "r",
                       "replica_identity" => "f"
                     }
                   ]
                 }
               })

      assert :ok = Health.put_event(entity, %Event{slug: :messages_filtered, status: :success})
      assert {:ok, %Health{} = health} = Health.health(entity)
      assert health.status == :waiting

      assert Enum.find(health.checks, &(&1.slug == :messages_filtered)).status == :healthy

      for slug <- [:messages_ingested, :messages_pending_delivery] do
        assert :ok = Health.put_event(entity, %Event{slug: slug, status: :success})
      end

      assert {:ok, %Health{} = health} = Health.health(entity)
      # If one remaining check is waiting, the health status should be waiting
      assert health.status == :waiting

      assert :ok = Health.put_event(entity, %Event{slug: :messages_delivered, status: :success})
      assert {:ok, %Health{} = health} = Health.health(entity)
      assert health.status == :healthy
    end

    test "health is in error if something is erroring" do
      entity = sink_consumer()

      assert :ok = Health.put_event(entity, %Event{slug: :sink_config_checked, status: :success})

      assert :ok =
               Health.put_event(entity, %Event{
                 slug: :messages_ingested,
                 status: :fail,
                 error: ErrorFactory.random_error()
               })
    end

    test "raises an error for unexpected events" do
      entity = sink_consumer()

      assert_raise ArgumentError, fn ->
        Health.put_event(entity, %Event{slug: :unexpected_event, status: :success})
      end
    end
  end

  describe "health/1" do
    test ":postgres_database :reachable goes into error if not present after 5 minutes of creation" do
      entity = postgres_replication(inserted_at: DateTime.add(DateTime.utc_now(), -6, :minute))

      assert {:ok, health} = Health.health(entity)

      assert health.status == :error
    end
  end

  describe "messages_ingested check" do
    test "reflects ingested event state when no backfill event exists" do
      entity = sink_consumer()

      # Test initial state (no events)
      {:ok, health} = Health.health(entity)
      check = Enum.find(health.checks, &(&1.slug == :messages_ingested))
      assert check.status == :waiting

      # Test success state
      :ok = Health.put_event(entity, %Event{slug: :messages_ingested, status: :success})
      {:ok, health} = Health.health(entity)
      check = Enum.find(health.checks, &(&1.slug == :messages_ingested))
      assert check.status == :healthy

      # Test warning state
      :ok = Health.put_event(entity, %Event{slug: :messages_ingested, status: :warning})
      {:ok, health} = Health.health(entity)
      check = Enum.find(health.checks, &(&1.slug == :messages_ingested))
      assert check.status == :warning

      # Test error state
      error = ErrorFactory.random_error()
      :ok = Health.put_event(entity, %Event{slug: :messages_ingested, status: :fail, error: error})
      {:ok, health} = Health.health(entity)
      check = Enum.find(health.checks, &(&1.slug == :messages_ingested))
      assert check.status == :error
      assert check.error == error
    end

    test "shows warning when backfill fetch batch event has warning" do
      entity = sink_consumer()

      # Set up a successful ingestion event
      :ok = Health.put_event(entity, %Event{slug: :messages_ingested, status: :success})

      # Add a warning backfill event
      backfill_error = ErrorFactory.service_error()

      :ok =
        Health.put_event(entity, %Event{
          slug: :backfill_fetch_batch,
          status: :warning,
          error: backfill_error
        })

      # Check that the warning state is reflected
      {:ok, health} = Health.health(entity)
      check = Enum.find(health.checks, &(&1.slug == :messages_ingested))
      assert check.status == :warning
      assert check.error == backfill_error
    end
  end

  describe "to_external/1" do
    test "converts the health to an external format" do
      entity = ConsumersFactory.sink_consumer(id: Factory.uuid(), inserted_at: DateTime.utc_now())

      assert {:ok, %Health{} = health} = Health.health(entity)
      assert external = Health.to_external(health)
      assert external.status == :initializing

      assert :ok =
               Health.put_event(entity, %Event{
                 slug: :messages_ingested,
                 status: :fail,
                 error: ErrorFactory.random_error()
               })

      assert {:ok, %Health{} = health} = Health.health(entity)
      assert external = Health.to_external(health)
      assert external.status == :error
      assert Enum.find(external.checks, &(not is_nil(&1.error)))
    end
  end

  describe "snapshots" do
    test "get_snapshot returns not found for non-existent snapshot" do
      entity = postgres_replication()
      assert {:error, %Error.NotFoundError{}} = Health.get_snapshot(entity)
    end

    test "upsert_snapshot creates new snapshot for replication slot" do
      entity = postgres_replication()

      # Set initial health
      :ok = Health.put_event(entity, %Event{slug: :replication_connected, status: :success})

      assert {:ok, snapshot} = Health.upsert_snapshot(entity)
      assert snapshot.entity_id == entity.id
      assert snapshot.entity_kind == :postgres_replication_slot
      assert snapshot.status == :initializing
      assert is_map(snapshot.health_json)
    end

    test "upsert_snapshot updates existing snapshot" do
      entity = postgres_replication(status: :enabled)

      # Set initial health and create snapshot
      :ok = Health.put_event(entity, %Event{slug: :replication_connected, status: :success})
      {:ok, initial_snapshot} = Health.upsert_snapshot(entity)

      # Update health and snapshot
      :ok =
        Health.put_event(
          entity,
          %Event{slug: :replication_connected, status: :fail, error: ErrorFactory.service_error()}
        )

      {:ok, updated_snapshot} = Health.upsert_snapshot(entity)

      assert updated_snapshot.id == initial_snapshot.id
      assert updated_snapshot.status == :error
      assert DateTime.compare(updated_snapshot.sampled_at, initial_snapshot.sampled_at) in [:gt, :eq]
    end

    test "upsert_snapshot creates new snapshot for consumer" do
      entity = sink_consumer()

      # Set initial health
      :ok = Health.put_event(entity, %Event{slug: :messages_filtered, status: :success})

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

    @tag capture_log: true
    test "alerts PagerDuty when database status changes to error" do
      entity =
        postgres_replication(
          account: AccountsFactory.account(),
          account_id: Factory.uuid(),
          postgres_database: DatabasesFactory.postgres_database()
        )

      Health.on_status_change(entity, :healthy, :error)

      assert_receive {:req, conn, body}

      assert conn.method == "POST"
      assert conn.path_info == ["v2", "enqueue"]

      assert body["dedup_key"] == "replication_slot_health_#{entity.id}"
      assert body["event_action"] == "trigger"
    end

    test "resolves PagerDuty alert when status changes to healthy" do
      entity =
        postgres_replication(
          account: AccountsFactory.account(),
          account_id: Factory.uuid(),
          postgres_database: DatabasesFactory.postgres_database()
        )

      Health.on_status_change(entity, :error, :healthy)

      assert_receive {:req, conn, body}

      assert conn.path_info == ["v2", "enqueue"]
      assert body["dedup_key"] == "replication_slot_health_#{entity.id}"
      assert body["event_action"] == "resolve"
    end

    test "skips PagerDuty when entity has ignore_health annotation" do
      entity =
        postgres_replication(
          postgres_database: DatabasesFactory.postgres_database(),
          annotations: %{"ignore_health" => true}
        )

      Req.Test.stub(Sequin.Pagerduty, fn _req ->
        raise "should not be called"
      end)

      Health.on_status_change(entity, :healthy, :error)
    end

    test "skips PagerDuty when account has ignore_health annotation" do
      entity =
        postgres_replication(
          account_id: Factory.uuid(),
          account: AccountsFactory.account(annotations: %{"ignore_health" => true}),
          postgres_database: DatabasesFactory.postgres_database()
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
      Health.put_event(slot, %Event{slug: :replication_connected, status: :success})
      Health.put_event(consumer, %Event{slug: :messages_filtered, status: :warning})
      Health.put_event(pipeline, %Event{slug: :messages_ingested, status: :fail, error: ErrorFactory.service_error()})

      # Run snapshot update
      :ok = Health.update_snapshots()

      # Verify snapshots were created with correct status
      assert {:ok, slot_snapshot} = Health.get_snapshot(slot)
      assert slot_snapshot.status == :initializing
      assert slot_snapshot.entity_kind == :postgres_replication_slot

      assert {:ok, consumer_snapshot} = Health.get_snapshot(consumer)
      assert consumer_snapshot.status == :warning
      assert consumer_snapshot.entity_kind == :sink_consumer

      assert {:ok, pipeline_snapshot} = Health.get_snapshot(pipeline)
      assert pipeline_snapshot.status == :error
      assert pipeline_snapshot.entity_kind == :wal_pipeline
    end

    test "skips inactive entities" do
      db = DatabasesFactory.insert_postgres_database!(name: "test-db")

      slot =
        ReplicationFactory.insert_postgres_replication!(
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
      Health.put_event(slot, %Event{slug: :replication_connected, status: :fail, error: ErrorFactory.service_error()})

      Health.put_event(deleted_consumer, %Event{
        slug: :messages_filtered,
        status: :fail,
        error: ErrorFactory.service_error()
      })

      # Run snapshot update
      :ok = Health.update_snapshots()

      # Verify no snapshots were created for inactive entities
      # No snapshot because slot is disabled
      assert {:error, _} = Health.get_snapshot(slot)
      # No snapshot because consumer is disabled
      assert {:error, _} = Health.get_snapshot(deleted_consumer)
    end

    test "updates existing snapshots" do
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

      # Set initial health and create snapshot
      Health.put_event(consumer, %Event{slug: :messages_filtered, status: :success})
      {:ok, initial_snapshot} = Health.upsert_snapshot(consumer)

      # Change health status
      Health.put_event(consumer, %Event{slug: :messages_filtered, status: :fail, error: ErrorFactory.service_error()})

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

  describe "health status when disabled or paused" do
    test "health is paused when entity is disabled regardless of health events" do
      for status <- [:disabled, :paused] do
        entity = sink_consumer(status: status)

        assert {:ok, %Health{} = health} = Health.health(entity)
        assert health.status == :paused

        # Even successful health events don't change the paused status
        assert :ok = Health.put_event(entity, %Event{slug: :messages_filtered, status: :success})
        assert :ok = Health.put_event(entity, %Event{slug: :messages_ingested, status: :success})

        assert {:ok, %Health{} = health} = Health.health(entity)
        assert health.status == :paused

        # Error events also don't change the paused status
        assert :ok =
                 Health.put_event(entity, %Event{
                   slug: :messages_delivered,
                   status: :fail,
                   error: ErrorFactory.random_error()
                 })

        assert {:ok, %Health{} = health} = Health.health(entity)
        assert health.status == :paused
      end
    end
  end

  describe "sink configuration check" do
    test "shows appropriate warnings for TOAST columns and replica identity when replica identity is not full" do
      entity = sink_consumer(message_kind: :event)

      # Set up base configuration check
      :ok =
        Health.put_event(entity, %Event{
          slug: :sink_config_checked,
          status: :success,
          data: %{
            "tables_with_replica_identities" => [
              %{
                "table_oid" => 123,
                "relation_kind" => "r",
                "replica_identity" => "d"
              }
            ]
          }
        })

      # Initial state - should show replica identity warning
      {:ok, health} = Health.health(entity)
      config_check = Enum.find(health.checks, &(&1.slug == :sink_configuration))
      assert config_check.status == :notice
      assert config_check.error_slug == :replica_identity_not_full

      # Dismiss replica identity warning
      :ok =
        Health.put_event(entity, %Event{
          slug: :alert_replica_identity_not_full_dismissed,
          status: :success
        })

      # Add TOAST columns detection
      :ok =
        Health.put_event(entity, %Event{
          slug: :toast_columns_detected,
          status: :success
        })

      # Should now show TOAST columns warning
      {:ok, health} = Health.health(entity)
      config_check = Enum.find(health.checks, &(&1.slug == :sink_configuration))
      assert config_check.status == :notice
      assert config_check.error_slug == :toast_columns_detected

      # Dismiss TOAST columns warning
      :ok =
        Health.put_event(entity, %Event{
          slug: :alert_toast_columns_detected_dismissed,
          status: :success
        })

      # Should now be healthy
      {:ok, health} = Health.health(entity)
      config_check = Enum.find(health.checks, &(&1.slug == :sink_configuration))
      assert config_check.status == :healthy
      assert is_nil(config_check.error_slug)
    end

    test "is healthy when replica identity is full regardless of TOAST columns" do
      entity = sink_consumer(message_kind: :event)

      # Set replica identity to full
      :ok =
        Health.put_event(entity, %Event{
          slug: :sink_config_checked,
          status: :success,
          data: %{
            "tables_with_replica_identities" => [
              %{
                "table_oid" => 123,
                "relation_kind" => "r",
                "replica_identity" => "f"
              }
            ]
          }
        })

      # Add TOAST columns detection
      :ok =
        Health.put_event(entity, %Event{
          slug: :toast_columns_detected,
          status: :success
        })

      # Should be healthy since replica identity is full
      {:ok, health} = Health.health(entity)
      config_check = Enum.find(health.checks, &(&1.slug == :sink_configuration))
      assert config_check.status == :healthy
      assert is_nil(config_check.error_slug)
    end

    test "shows appropriate warnings for load shedding policy events" do
      entity = sink_consumer(message_kind: :event)

      # Set up base delivery check
      :ok = Health.put_event(entity, %Event{slug: :messages_delivered, status: :success})

      # Add load shedding policy discarded event
      :ok = Health.put_event(entity, %Event{slug: :load_shedding_policy_discarded, status: :success})

      # Should now show load shedding warning
      {:ok, health} = Health.health(entity)
      delivery_check = Enum.find(health.checks, &(&1.slug == :messages_delivered))
      assert delivery_check.status == :warning
      assert delivery_check.error_slug == :load_shedding_policy_discarded

      # Dismiss load shedding warning
      :ok = Health.put_event(entity, %Event{slug: :load_shedding_policy_discarded_dismissed, status: :success})

      # Should now be healthy again
      {:ok, health} = Health.health(entity)
      delivery_check = Enum.find(health.checks, &(&1.slug == :messages_delivered))
      assert delivery_check.status == :healthy
      assert is_nil(delivery_check.error_slug)
    end
  end

  defp sink_consumer(opts \\ []) do
    ConsumersFactory.sink_consumer(
      Keyword.merge([id: Factory.uuid(), inserted_at: DateTime.utc_now(), status: :enabled], opts)
    )
  end

  defp postgres_replication(opts \\ []) do
    ReplicationFactory.postgres_replication(
      Keyword.merge([id: Factory.uuid(), inserted_at: DateTime.utc_now(), status: :enabled], opts)
    )
  end

  defp http_endpoint(opts \\ []) do
    ConsumersFactory.http_endpoint(Keyword.merge([id: Factory.uuid(), inserted_at: DateTime.utc_now()], opts))
  end
end

defmodule Sequin.ReplicationTest do
  use Sequin.DataCase, async: true

  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Health
  alias Sequin.Metrics
  alias Sequin.Replication

  describe "list_wal_events/2" do
    setup do
      wal_pipeline = ReplicationFactory.insert_wal_pipeline!()
      {:ok, wal_pipeline: wal_pipeline}
    end

    test "returns events sorted by commit_lsn in ascending order", %{
      wal_pipeline: wal_pipeline
    } do
      event3 =
        ReplicationFactory.insert_wal_event!(
          wal_pipeline_id: wal_pipeline.id,
          commit_lsn: 300
        )

      event1 =
        ReplicationFactory.insert_wal_event!(
          wal_pipeline_id: wal_pipeline.id,
          commit_lsn: 100
        )

      event2 =
        ReplicationFactory.insert_wal_event!(
          wal_pipeline_id: wal_pipeline.id,
          commit_lsn: 200
        )

      events =
        Replication.list_wal_events(wal_pipeline.id, order_by: [asc: :commit_lsn])

      assert length(events) == 3
      assert Enum.map(events, & &1.id) == [event1.id, event2.id, event3.id]
      assert Enum.map(events, & &1.commit_lsn) == [100, 200, 300]
    end

    test "respects limit parameter", %{wal_pipeline: wal_pipeline} do
      for lsn <- 1..5 do
        ReplicationFactory.insert_wal_event!(
          wal_pipeline_id: wal_pipeline.id,
          commit_lsn: lsn * 100
        )
      end

      events =
        Replication.list_wal_events(wal_pipeline.id,
          order_by: [asc: :commit_lsn],
          limit: 3
        )

      assert length(events) == 3
      assert Enum.map(events, & &1.commit_lsn) == [100, 200, 300]
    end

    test "respects offset parameter", %{wal_pipeline: wal_pipeline} do
      for lsn <- 1..5 do
        ReplicationFactory.insert_wal_event!(
          wal_pipeline_id: wal_pipeline.id,
          commit_lsn: lsn * 100
        )
      end

      events =
        Replication.list_wal_events(wal_pipeline.id,
          order_by: [asc: :commit_lsn],
          offset: 2
        )

      assert length(events) == 3
      assert Enum.map(events, & &1.commit_lsn) == [300, 400, 500]
    end

    test "returns empty list when no events match criteria" do
      non_existent_id = Ecto.UUID.generate()

      events = Replication.list_wal_events(non_existent_id)

      assert events == []
    end
  end

  describe "delete_wal_events/1" do
    test "deletes multiple wal events by their ids" do
      wal_pipeline = ReplicationFactory.insert_wal_pipeline!()

      event1 =
        ReplicationFactory.insert_wal_event!(
          wal_pipeline_id: wal_pipeline.id,
          commit_lsn: 100
        )

      event2 =
        ReplicationFactory.insert_wal_event!(
          wal_pipeline_id: wal_pipeline.id,
          commit_lsn: 200
        )

      event3 =
        ReplicationFactory.insert_wal_event!(
          wal_pipeline_id: wal_pipeline.id,
          commit_lsn: 300
        )

      {:ok, deleted_count} = Replication.delete_wal_events([event1.id, event2.id])

      assert deleted_count == 2

      remaining_events = Replication.list_wal_events(wal_pipeline.id)

      assert length(remaining_events) == 1
      assert hd(remaining_events).id == event3.id
    end

    test "returns ok with 0 count when no events match the given ids" do
      {:ok, deleted_count} = Replication.delete_wal_events([Ecto.UUID.generate(), Ecto.UUID.generate()])
      assert deleted_count == 0
    end
  end

  describe "measure_replication_lag/2" do
    setup do
      replication_slot =
        ReplicationFactory.postgres_replication(
          inserted_at: Sequin.utc_now(),
          status: :active,
          postgres_database: DatabasesFactory.postgres_database()
        )

      {:ok, slot: replication_slot}
    end

    test "records metrics and health events when lag is below threshold", %{slot: slot} do
      lag_bytes = 100_000

      measure_fn = fn _slot -> {:ok, lag_bytes} end

      assert {:ok, ^lag_bytes} = Replication.measure_replication_lag(slot, measure_fn)

      # Verify metrics were recorded
      assert {:ok, ^lag_bytes} = Metrics.get_postgres_replication_slot_lag(slot)
    end

    test "records warning health event when lag exceeds threshold", %{slot: slot} do
      # Set lag above the threshold
      lag_bytes = 10 * 1024 * 1024 * 1024

      measure_fn = fn _slot -> {:ok, lag_bytes} end

      Replication.measure_replication_lag(slot, measure_fn)

      # Verify metrics were recorded
      assert {:ok, ^lag_bytes} = Metrics.get_postgres_replication_slot_lag(slot)

      # Verify health event was recorded with warning status
      assert {:ok, event} = Health.get_event(slot.id, :replication_lag_checked)
      assert event.status == :warning
      assert {:ok, health} = Health.health(slot)
      assert health.status == :warning
    end
  end
end

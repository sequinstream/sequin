defmodule Sequin.ReplicationTest do
  use Sequin.DataCase, async: true

  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Replication

  describe "list_wal_events_for_projection/3" do
    setup do
      replication_slot = ReplicationFactory.insert_postgres_replication!()
      source_table_oid = Enum.random(1..100_000)

      {:ok, replication_slot: replication_slot, source_table_oid: source_table_oid}
    end

    test "returns events sorted by commit_lsn in ascending order", %{
      replication_slot: replication_slot,
      source_table_oid: source_table_oid
    } do
      event3 =
        ReplicationFactory.insert_wal_event!(
          source_replication_slot_id: replication_slot.id,
          source_table_oid: source_table_oid,
          commit_lsn: 300
        )

      event1 =
        ReplicationFactory.insert_wal_event!(
          source_replication_slot_id: replication_slot.id,
          source_table_oid: source_table_oid,
          commit_lsn: 100
        )

      event2 =
        ReplicationFactory.insert_wal_event!(
          source_replication_slot_id: replication_slot.id,
          source_table_oid: source_table_oid,
          commit_lsn: 200
        )

      events =
        Replication.list_wal_events_for_projection(replication_slot.id, source_table_oid, order_by: [asc: :commit_lsn])

      assert length(events) == 3
      assert Enum.map(events, & &1.id) == [event1.id, event2.id, event3.id]
      assert Enum.map(events, & &1.commit_lsn) == [100, 200, 300]
    end

    test "respects limit parameter", %{replication_slot: replication_slot, source_table_oid: source_table_oid} do
      for lsn <- 1..5 do
        ReplicationFactory.insert_wal_event!(
          source_replication_slot_id: replication_slot.id,
          source_table_oid: source_table_oid,
          commit_lsn: lsn * 100
        )
      end

      events =
        Replication.list_wal_events_for_projection(replication_slot.id, source_table_oid,
          order_by: [asc: :commit_lsn],
          limit: 3
        )

      assert length(events) == 3
      assert Enum.map(events, & &1.commit_lsn) == [100, 200, 300]
    end

    test "respects offset parameter", %{replication_slot: replication_slot, source_table_oid: source_table_oid} do
      for lsn <- 1..5 do
        ReplicationFactory.insert_wal_event!(
          source_replication_slot_id: replication_slot.id,
          source_table_oid: source_table_oid,
          commit_lsn: lsn * 100
        )
      end

      events =
        Replication.list_wal_events_for_projection(replication_slot.id, source_table_oid,
          order_by: [asc: :commit_lsn],
          offset: 2
        )

      assert length(events) == 3
      assert Enum.map(events, & &1.commit_lsn) == [300, 400, 500]
    end

    test "returns empty list when no events match criteria", %{replication_slot: replication_slot} do
      non_existent_oid = 999_999

      events = Replication.list_wal_events_for_projection(replication_slot.id, non_existent_oid)

      assert events == []
    end
  end

  describe "delete_wal_events/1" do
    test "deletes multiple wal events by their ids" do
      replication_slot = ReplicationFactory.insert_postgres_replication!()
      source_table_oid = Enum.random(1..100_000)

      event1 =
        ReplicationFactory.insert_wal_event!(
          source_replication_slot_id: replication_slot.id,
          source_table_oid: source_table_oid,
          commit_lsn: 100
        )

      event2 =
        ReplicationFactory.insert_wal_event!(
          source_replication_slot_id: replication_slot.id,
          source_table_oid: source_table_oid,
          commit_lsn: 200
        )

      event3 =
        ReplicationFactory.insert_wal_event!(
          source_replication_slot_id: replication_slot.id,
          source_table_oid: source_table_oid,
          commit_lsn: 300
        )

      {:ok, deleted_count} = Replication.delete_wal_events([event1.id, event2.id])

      assert deleted_count == 2

      remaining_events = Replication.list_wal_events_for_projection(replication_slot.id, source_table_oid)
      assert length(remaining_events) == 1
      assert hd(remaining_events).id == event3.id
    end

    test "returns ok with 0 count when no events match the given ids" do
      {:ok, deleted_count} = Replication.delete_wal_events([1, 2])
      assert deleted_count == 0
    end
  end
end

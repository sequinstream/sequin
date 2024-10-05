defmodule Sequin.ReplicationTest do
  use Sequin.DataCase, async: true

  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Replication

  describe "list_wal_events/2" do
    setup do
      wal_projection = ReplicationFactory.insert_wal_projection!()
      {:ok, wal_projection: wal_projection}
    end

    test "returns events sorted by commit_lsn in ascending order", %{
      wal_projection: wal_projection
    } do
      event3 =
        ReplicationFactory.insert_wal_event!(
          wal_projection_id: wal_projection.id,
          commit_lsn: 300
        )

      event1 =
        ReplicationFactory.insert_wal_event!(
          wal_projection_id: wal_projection.id,
          commit_lsn: 100
        )

      event2 =
        ReplicationFactory.insert_wal_event!(
          wal_projection_id: wal_projection.id,
          commit_lsn: 200
        )

      events =
        Replication.list_wal_events(wal_projection.id, order_by: [asc: :commit_lsn])

      assert length(events) == 3
      assert Enum.map(events, & &1.id) == [event1.id, event2.id, event3.id]
      assert Enum.map(events, & &1.commit_lsn) == [100, 200, 300]
    end

    test "respects limit parameter", %{wal_projection: wal_projection} do
      for lsn <- 1..5 do
        ReplicationFactory.insert_wal_event!(
          wal_projection_id: wal_projection.id,
          commit_lsn: lsn * 100
        )
      end

      events =
        Replication.list_wal_events(wal_projection.id,
          order_by: [asc: :commit_lsn],
          limit: 3
        )

      assert length(events) == 3
      assert Enum.map(events, & &1.commit_lsn) == [100, 200, 300]
    end

    test "respects offset parameter", %{wal_projection: wal_projection} do
      for lsn <- 1..5 do
        ReplicationFactory.insert_wal_event!(
          wal_projection_id: wal_projection.id,
          commit_lsn: lsn * 100
        )
      end

      events =
        Replication.list_wal_events(wal_projection.id,
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
      wal_projection = ReplicationFactory.insert_wal_projection!()

      event1 =
        ReplicationFactory.insert_wal_event!(
          wal_projection_id: wal_projection.id,
          commit_lsn: 100
        )

      event2 =
        ReplicationFactory.insert_wal_event!(
          wal_projection_id: wal_projection.id,
          commit_lsn: 200
        )

      event3 =
        ReplicationFactory.insert_wal_event!(
          wal_projection_id: wal_projection.id,
          commit_lsn: 300
        )

      {:ok, deleted_count} = Replication.delete_wal_events([event1.id, event2.id])

      assert deleted_count == 2

      remaining_events = Replication.list_wal_events(wal_projection.id)

      assert length(remaining_events) == 1
      assert hd(remaining_events).id == event3.id
    end

    test "returns ok with 0 count when no events match the given ids" do
      {:ok, deleted_count} = Replication.delete_wal_events([Ecto.UUID.generate(), Ecto.UUID.generate()])
      assert deleted_count == 0
    end
  end
end

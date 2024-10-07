defmodule Sequin.WalEventTest do
  use Sequin.DataCase, async: true

  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Replication
  alias Sequin.Replication.WalEvent

  describe "WalEvent" do
    setup do
      wal_pipeline = ReplicationFactory.insert_wal_pipeline!()
      %{wal_pipeline: wal_pipeline}
    end

    test "insert_wal_events/1 inserts batch of wal events", %{wal_pipeline: wal_pipeline} do
      events =
        for _ <- 1..3 do
          ReplicationFactory.wal_event(%{wal_pipeline_id: wal_pipeline.id})
        end

      assert {:ok, 3} = Replication.insert_wal_events(events)

      inserted_events = Repo.all(WalEvent)
      assert length(inserted_events) == 3

      assert_lists_equal(inserted_events, events, fn e1, e2 ->
        assert_maps_equal(e1, e2, [
          :wal_pipeline_id,
          :commit_lsn,
          :record_pks,
          :replication_message_trace_id,
          :source_table_oid,
          :record,
          :changes,
          :action,
          :committed_at
        ])
      end)
    end

    test "list_wal_events/2 returns events only for the specified pipeline", %{
      wal_pipeline: wal_pipeline
    } do
      other_wal_pipeline = ReplicationFactory.insert_wal_pipeline!()

      for _ <- 1..3,
          do: ReplicationFactory.insert_wal_event!(wal_pipeline_id: wal_pipeline.id)

      ReplicationFactory.insert_wal_event!(wal_pipeline_id: other_wal_pipeline.id)

      fetched_events1 = Replication.list_wal_events(wal_pipeline.id)
      assert length(fetched_events1) == 3
      assert Enum.all?(fetched_events1, &(&1.wal_pipeline_id == wal_pipeline.id))

      fetched_events2 = Replication.list_wal_events(other_wal_pipeline.id)
      assert length(fetched_events2) == 1
      assert Enum.all?(fetched_events2, &(&1.wal_pipeline_id == other_wal_pipeline.id))
    end

    test "list_wal_events/2 limits the number of returned events", %{
      wal_pipeline: wal_pipeline
    } do
      for _ <- 1..5,
          do: ReplicationFactory.insert_wal_event!(wal_pipeline_id: wal_pipeline.id)

      limited_events = Replication.list_wal_events(wal_pipeline.id, limit: 2)

      assert length(limited_events) == 2
    end

    test "list_wal_events/2 orders events by specified criteria", %{
      wal_pipeline: wal_pipeline
    } do
      for _ <- 1..5,
          do: ReplicationFactory.insert_wal_event!(wal_pipeline_id: wal_pipeline.id)

      events_asc = Replication.list_wal_events(wal_pipeline.id, order_by: [asc: :id])
      events_desc = Replication.list_wal_events(wal_pipeline.id, order_by: [desc: :id])

      assert Enum.map(events_asc, & &1.id) == Enum.sort(Enum.map(events_asc, & &1.id))
      assert Enum.map(events_desc, & &1.id) == Enum.sort(Enum.map(events_desc, & &1.id), :desc)
    end
  end
end

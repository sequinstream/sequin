defmodule Sequin.WalEventTest do
  use Sequin.DataCase, async: true

  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Replication
  alias Sequin.Replication.WalEvent

  describe "WalEvent" do
    setup do
      postgres_replication = ReplicationFactory.insert_postgres_replication!()
      %{postgres_replication: postgres_replication}
    end

    test "insert_wal_events/1 inserts batch of wal events", %{postgres_replication: postgres_replication} do
      events =
        for _ <- 1..3 do
          %{source_replication_slot_id: postgres_replication.id, source_table_oid: Enum.random(1..100_000)}
          |> ReplicationFactory.wal_event_attrs()
          |> Map.update(:data, %{}, fn data ->
            data
            |> Sequin.Map.atomize_keys()
            |> Map.update!(:metadata, &Sequin.Map.atomize_keys/1)
          end)
        end

      assert {:ok, 3} = Replication.insert_wal_events(events)

      inserted_events = Repo.all(WalEvent)
      assert length(inserted_events) == 3

      assert_lists_equal(inserted_events, events, fn e1, e2 ->
        e1
        |> Map.update!(:data, fn data ->
          data
          |> Map.update!(:metadata, &Map.from_struct/1)
          |> Map.from_struct()
        end)
        |> assert_maps_equal(e2, [:source_replication_slot_id, :source_table_oid, :commit_lsn, :data])
      end)
    end

    test "list_wal_events_for_projection/3 returns events only for the specified projection", %{
      postgres_replication: postgres_replication
    } do
      source_table_oid = Enum.random(1..100_000)
      other_postgres_replication = ReplicationFactory.insert_postgres_replication!()

      for _ <- 1..3,
          do:
            ReplicationFactory.insert_wal_event!(
              source_replication_slot_id: postgres_replication.id,
              source_table_oid: source_table_oid
            )

      ReplicationFactory.insert_wal_event!(
        source_replication_slot_id: other_postgres_replication.id,
        source_table_oid: source_table_oid
      )

      fetched_events1 = Replication.list_wal_events_for_projection(postgres_replication.id, source_table_oid)
      assert length(fetched_events1) == 3
      assert Enum.all?(fetched_events1, &(&1.source_replication_slot_id == postgres_replication.id))

      fetched_events2 = Replication.list_wal_events_for_projection(other_postgres_replication.id, source_table_oid)
      assert length(fetched_events2) == 1
      assert Enum.all?(fetched_events2, &(&1.source_replication_slot_id == other_postgres_replication.id))
    end

    test "list_wal_events_for_projection/3 limits the number of returned events", %{
      postgres_replication: postgres_replication
    } do
      source_table_oid = Enum.random(1..100_000)

      for _ <- 1..5,
          do:
            ReplicationFactory.insert_wal_event!(
              source_replication_slot_id: postgres_replication.id,
              source_table_oid: source_table_oid
            )

      limited_events = Replication.list_wal_events_for_projection(postgres_replication.id, source_table_oid, limit: 2)
      assert length(limited_events) == 2
    end

    test "list_wal_events_for_projection/3 orders events by specified criteria", %{
      postgres_replication: postgres_replication
    } do
      source_table_oid = Enum.random(1..100_000)

      for _ <- 1..5,
          do:
            ReplicationFactory.insert_wal_event!(
              source_replication_slot_id: postgres_replication.id,
              source_table_oid: source_table_oid
            )

      events_asc =
        Replication.list_wal_events_for_projection(postgres_replication.id, source_table_oid, order_by: [asc: :id])

      events_desc =
        Replication.list_wal_events_for_projection(postgres_replication.id, source_table_oid, order_by: [desc: :id])

      assert Enum.map(events_asc, & &1.id) == Enum.sort(Enum.map(events_asc, & &1.id))
      assert Enum.map(events_desc, & &1.id) == Enum.sort(Enum.map(events_desc, & &1.id), :desc)
    end
  end
end

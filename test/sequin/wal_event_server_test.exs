# test/sequin/wal_event_server_test.exs

defmodule Sequin.ReplicationRuntime.WalEventServerTest do
  use Sequin.DataCase, async: true
  use ExUnit.Case

  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Replication
  alias Sequin.ReplicationRuntime.WalEventServer
  alias Sequin.Test.Support.Models.TestEventLog

  setup do
    account = AccountsFactory.insert_account!()
    source_database = DatabasesFactory.insert_postgres_database!(account_id: account.id)
    destination_database = DatabasesFactory.insert_configured_postgres_database!(account_id: account.id, tables: [])

    replication_slot =
      ReplicationFactory.insert_postgres_replication!(
        account_id: account.id,
        postgres_database_id: source_database.id
      )

    wal_projection =
      ReplicationFactory.insert_wal_projection!(
        account_id: account.id,
        replication_slot_id: replication_slot.id,
        destination_database_id: destination_database.id,
        destination_oid: TestEventLog.table_oid()
      )

    {:ok,
     %{
       destination_database: destination_database,
       replication_slot: replication_slot,
       wal_projection: wal_projection
     }}
  end

  describe "WalEventServer" do
    test "processes WAL events and writes them to the destination table", %{
      wal_projection: wal_projection
    } do
      # Insert some WAL events
      wal_events =
        Enum.map(1..5, fn _ -> ReplicationFactory.insert_wal_event!(wal_projection_id: wal_projection.id) end)

      start_supervised!(
        {WalEventServer,
         [
           replication_slot_id: wal_projection.replication_slot_id,
           destination_oid: wal_projection.destination_oid,
           destination_database_id: wal_projection.destination_database_id,
           wal_projection_ids: [wal_projection.id],
           test_pid: self(),
           batch_size: 2
         ]}
      )

      # Wait for the WalEventServer to process events
      assert_receive {WalEventServer, :wrote_events, 2}, 1000
      assert_receive {WalEventServer, :wrote_events, 2}, 1000
      assert_receive {WalEventServer, :wrote_events, 1}, 1000

      # Verify that the events were written to the destination table
      logs = Repo.all(TestEventLog)
      assert length(logs) == 5

      as_logs =
        Enum.map(wal_events, fn wal_event ->
          %{
            seq: wal_event.commit_lsn,
            source_table_oid: wal_event.source_table_oid,
            record_pk: hd(wal_event.record_pks),
            action: to_string(wal_event.action),
            changes: wal_event.changes,
            committed_at: wal_event.committed_at,
            record: wal_event.record
          }
        end)

      # Assert that the WAL events were properly mapped to TestEventLog
      assert_lists_equal(as_logs, logs, fn log1, log2 ->
        assert_maps_equal(log1, log2, [
          :seq,
          :record_pk,
          :action,
          :changes,
          # Inscrutable timezone/dt issue
          # :committed_at,
          :record,
          :source_table_oid
        ])
      end)

      # Verify that the WAL events were deleted after processing
      assert Replication.list_wal_events(wal_projection.id) == []
    end

    test "processes WAL events on PubSub notification", %{
      wal_projection: wal_projection
    } do
      start_supervised!({WalEventServer,
       [
         replication_slot_id: wal_projection.replication_slot_id,
         destination_oid: wal_projection.destination_oid,
         destination_database_id: wal_projection.destination_database_id,
         wal_projection_ids: [wal_projection.id],
         test_pid: self(),
         batch_size: 2,
         # arbitrary delay
         interval_ms: :timer.minutes(1)
       ]})

      # Assert that we get :no_events initially
      assert_receive {WalEventServer, :no_events}, 1000

      # Insert a WAL event
      wal_event = ReplicationFactory.wal_event_attrs(wal_projection_id: wal_projection.id)
      Replication.insert_wal_events([wal_event])

      # Wait for the WalEventServer to process events
      assert_receive {WalEventServer, :wrote_events, 1}, 1000

      # Verify that the event was written to the destination table
      logs = Repo.all(TestEventLog)
      assert length(logs) == 1

      # Verify that the WAL event was deleted after processing
      assert Replication.list_wal_events(wal_projection.id) == []
    end
  end
end

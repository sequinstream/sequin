# test/sequin/wal_pipeline_server_test.exs

defmodule Sequin.ReplicationRuntime.WalPipelineServerTest do
  use Sequin.DataCase, async: true
  use ExUnit.Case

  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Health
  alias Sequin.Replication
  alias Sequin.ReplicationRuntime.WalPipelineServer
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

    wal_pipeline =
      ReplicationFactory.insert_wal_pipeline!(
        account_id: account.id,
        replication_slot_id: replication_slot.id,
        destination_database_id: destination_database.id,
        destination_oid: TestEventLog.table_oid()
      )

    {:ok,
     %{
       destination_database: destination_database,
       replication_slot: replication_slot,
       wal_pipeline: wal_pipeline
     }}
  end

  describe "WalPipelineServer" do
    test "processes WAL events, writes them to the destination table, and updates health", %{
      wal_pipeline: wal_pipeline
    } do
      # Simulate initial health
      Health.update(wal_pipeline, :filters, :healthy)
      Health.update(wal_pipeline, :ingestion, :healthy)

      commit_lsn = ReplicationFactory.commit_lsn()

      # Insert some WAL events
      wal_events =
        Enum.map(1..5, fn _ ->
          ReplicationFactory.insert_wal_event!(wal_pipeline_id: wal_pipeline.id, commit_lsn: commit_lsn)
        end)

      start_supervised!(
        {WalPipelineServer,
         [
           replication_slot_id: wal_pipeline.replication_slot_id,
           destination_oid: wal_pipeline.destination_oid,
           destination_database_id: wal_pipeline.destination_database_id,
           wal_pipeline_ids: [wal_pipeline.id],
           test_pid: self(),
           batch_size: 2
         ]}
      )

      # Wait for the WalPipelineServer to process events
      assert_receive {WalPipelineServer, :wrote_events, 2}, 1000
      assert_receive {WalPipelineServer, :wrote_events, 2}, 1000
      assert_receive {WalPipelineServer, :wrote_events, 1}, 1000

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

      # Verify that the health status is updated to healthy
      {:ok, health} = Health.get(wal_pipeline)
      assert health.status == :healthy
      assert Enum.all?(health.checks, &(&1.status == :healthy))
    end

    @tag capture_log: true
    test "updates health to error when writing to destination fails", %{
      wal_pipeline: wal_pipeline
    } do
      # Insert a WAL event
      ReplicationFactory.insert_wal_event!(wal_pipeline_id: wal_pipeline.id)

      # Mess up the destination table
      Repo.query!("alter table #{TestEventLog.table_name()} add column foo text not null")

      start_supervised!(
        {WalPipelineServer,
         [
           replication_slot_id: wal_pipeline.replication_slot_id,
           destination_oid: wal_pipeline.destination_oid,
           destination_database_id: wal_pipeline.destination_database_id,
           wal_pipeline_ids: [wal_pipeline.id],
           test_pid: self(),
           batch_size: 1
         ]}
      )

      assert_receive {WalPipelineServer, :write_failed, _reason}, 1000

      # Verify that the health status is updated to error
      {:ok, health} = Health.get(wal_pipeline)
      assert health.status == :error
      assert Enum.any?(health.checks, &(&1.status == :error))
    end

    test "processes WAL events on PubSub notification", %{
      wal_pipeline: wal_pipeline
    } do
      start_supervised!({WalPipelineServer,
       [
         replication_slot_id: wal_pipeline.replication_slot_id,
         destination_oid: wal_pipeline.destination_oid,
         destination_database_id: wal_pipeline.destination_database_id,
         wal_pipeline_ids: [wal_pipeline.id],
         test_pid: self(),
         batch_size: 2,
         # arbitrary delay
         interval_ms: :timer.minutes(1)
       ]})

      # Assert that we get :no_events initially
      assert_receive {WalPipelineServer, :no_events}, 1000

      # Insert a WAL event
      wal_event = ReplicationFactory.wal_event_attrs(wal_pipeline_id: wal_pipeline.id)
      Replication.insert_wal_events([wal_event])

      # Wait for the WalPipelineServer to process events
      assert_receive {WalPipelineServer, :wrote_events, 1}, 1000

      # Verify that the event was written to the destination table
      logs = Repo.all(TestEventLog)
      assert length(logs) == 1

      # Verify that the WAL event was deleted after processing
      assert Replication.list_wal_events(wal_pipeline.id) == []
    end
  end
end

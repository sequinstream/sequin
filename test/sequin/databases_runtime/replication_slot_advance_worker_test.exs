defmodule Sequin.Replication.ReplicationSlotAdvanceWorkerTest do
  use Sequin.DataCase, async: true

  import ExUnit.CaptureLog

  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Replication.ReplicationSlotAdvanceWorker

  describe "advance_low_watermark/1" do
    setup do
      account = AccountsFactory.insert_account!()
      database = DatabasesFactory.insert_postgres_database!(account_id: account.id)

      replication_slot =
        ReplicationFactory.insert_postgres_replication!(
          account_id: account.id,
          postgres_database_id: database.id,
          status: :active
        )

      # Create high and low watermarks for the slot
      ReplicationFactory.insert_replication_slot_watermark_wal_cursor!(
        replication_slot_id: replication_slot.id,
        boundary: :high,
        commit_lsn: 1000,
        commit_idx: 1
      )

      ReplicationFactory.insert_replication_slot_watermark_wal_cursor!(
        replication_slot_id: replication_slot.id,
        boundary: :low,
        commit_lsn: 0,
        commit_idx: 0
      )

      {:ok,
       account: account,
       replication_slot:
         Repo.preload(replication_slot, [
           :high_watermark_wal_cursor,
           :low_watermark_wal_cursor
         ])}
    end

    test "advances low watermark to minimum of slot and sink high watermarks", %{account: account, replication_slot: slot} do
      # Create two sink consumers with different high watermarks
      sink1 = ConsumersFactory.insert_sink_consumer!(account_id: account.id, replication_slot_id: slot.id)

      ConsumersFactory.insert_high_watermark_wal_cursor!(
        sink_consumer_id: sink1.id,
        commit_lsn: 500,
        commit_idx: 1
      )

      sink2 = ConsumersFactory.insert_sink_consumer!(account_id: account.id, replication_slot_id: slot.id)

      ConsumersFactory.insert_high_watermark_wal_cursor!(
        sink_consumer_id: sink2.id,
        commit_lsn: 750,
        commit_idx: 1
      )

      # Reload slot with associations
      slot = Repo.preload(slot, [:not_disabled_sink_consumers], force: true)

      # Call advance_low_watermark
      assert :ok = ReplicationSlotAdvanceWorker.advance_low_watermark(slot)

      # Verify low watermark was advanced to minimum high watermark (500)
      updated_slot = Repo.preload(slot, :low_watermark_wal_cursor, force: true)
      assert updated_slot.low_watermark_wal_cursor.commit_lsn == 500
      assert updated_slot.low_watermark_wal_cursor.commit_idx == 1
    end

    test "handles disabled sink consumers correctly", %{account: account, replication_slot: slot} do
      # Create one active and one disabled sink
      active_sink = ConsumersFactory.insert_sink_consumer!(account_id: account.id, replication_slot_id: slot.id)

      ConsumersFactory.insert_high_watermark_wal_cursor!(
        sink_consumer_id: active_sink.id,
        commit_lsn: 500,
        commit_idx: 1
      )

      disabled_sink =
        ConsumersFactory.insert_sink_consumer!(
          account_id: account.id,
          replication_slot_id: slot.id,
          status: :disabled
        )

      ConsumersFactory.insert_high_watermark_wal_cursor!(
        sink_consumer_id: disabled_sink.id,
        commit_lsn: 100,
        commit_idx: 1
      )

      # Reload slot with associations
      slot = Repo.preload(slot, [:not_disabled_sink_consumers], force: true)

      # Call advance_low_watermark
      assert :ok = ReplicationSlotAdvanceWorker.advance_low_watermark(slot)

      # Verify low watermark was advanced to 500 (ignoring disabled sink's 100)
      updated_slot = Repo.preload(slot, :low_watermark_wal_cursor, force: true)
      assert updated_slot.low_watermark_wal_cursor.commit_lsn == 500
      assert updated_slot.low_watermark_wal_cursor.commit_idx == 1
    end

    test "handles error when updating watermark", %{account: account, replication_slot: slot} do
      # Create sink with invalid watermark that would trigger DB constraint
      sink = ConsumersFactory.insert_sink_consumer!(account_id: account.id, replication_slot_id: slot.id)

      ConsumersFactory.insert_high_watermark_wal_cursor!(
        sink_consumer_id: sink.id,
        # Invalid LSN
        commit_lsn: -1,
        commit_idx: 1
      )

      # Reload slot with associations
      slot = Repo.preload(slot, [:not_disabled_sink_consumers], force: true)

      # Call advance_low_watermark and verify it handles error
      assert capture_log(fn ->
               ReplicationSlotAdvanceWorker.advance_low_watermark(slot)
             end) =~ "Error advancing low watermark for replication slot"
    end
  end
end

defmodule Sequin.MessageConsistencyCheckWorkerTest do
  use Sequin.DataCase, async: true

  import ExUnit.CaptureLog

  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Runtime.MessageConsistencyCheckWorker
  alias Sequin.Runtime.MessageLedgers

  describe "audit_and_trim_undelivered_cursors/2" do
    test "logs and trims undelivered messages that are not persisted" do
      # Create a sink consumer
      consumer = ConsumersFactory.insert_sink_consumer!()

      # Create some WAL cursors that are undelivered
      undelivered_cursors = [
        %{commit_lsn: 100, commit_idx: 1},
        %{commit_lsn: 200, commit_idx: 2},
        %{commit_lsn: 300, commit_idx: 3}
      ]

      # Add cursors to the undelivered set in Redis
      expect_utc_now(1, fn -> DateTime.add(DateTime.utc_now(), -3 * 60, :second) end)
      :ok = MessageLedgers.wal_cursors_ingested(consumer.id, undelivered_cursors)

      # Persist only one message to disk
      ConsumersFactory.insert_consumer_message!(
        message_kind: consumer.message_kind,
        consumer_id: consumer.id,
        commit_lsn: 100,
        commit_idx: 1
      )

      # Set timestamp to 2 minutes ago
      two_minutes_ago = DateTime.add(DateTime.utc_now(), -2 * 60, :second)

      # Capture logs to verify output
      assert capture_log(fn ->
               MessageConsistencyCheckWorker.audit_and_trim_undelivered_cursors(consumer.id, two_minutes_ago)
             end) =~ "Found undelivered/unpersisted cursors (count=2)"

      # Verify that the undelivered cursors set was trimmed
      {:ok, remaining_cursors} = MessageLedgers.list_undelivered_wal_cursors(consumer.id, two_minutes_ago)
      assert length(remaining_cursors) == 0
    end

    test "handles case where all undelivered messages are persisted" do
      # Create a sink consumer
      consumer = ConsumersFactory.insert_sink_consumer!()

      # Create some WAL cursors that are undelivered
      expect_utc_now(1, fn -> DateTime.add(DateTime.utc_now(), -3 * 60, :second) end)

      undelivered_cursors = [
        %{commit_lsn: 100, commit_idx: 1},
        %{commit_lsn: 200, commit_idx: 2}
      ]

      # Add cursors to the undelivered set in Redis
      :ok = MessageLedgers.wal_cursors_ingested(consumer.id, undelivered_cursors)

      # Persist all messages to disk
      for cursor <- undelivered_cursors do
        ConsumersFactory.insert_consumer_message!(
          message_kind: consumer.message_kind,
          consumer_id: consumer.id,
          commit_lsn: cursor.commit_lsn,
          commit_idx: cursor.commit_idx
        )
      end

      # Set timestamp to 2 minutes ago
      two_minutes_ago = DateTime.add(DateTime.utc_now(), -2 * 60, :second)

      # Capture logs to verify output
      refute capture_log(fn ->
               MessageConsistencyCheckWorker.audit_and_trim_undelivered_cursors(consumer.id, two_minutes_ago)
             end) =~ "Found undelivered/unpersisted cursors"

      # Verify that the undelivered cursors set was trimmed
      {:ok, remaining_cursors} = MessageLedgers.list_undelivered_wal_cursors(consumer.id, two_minutes_ago)
      assert length(remaining_cursors) == 0
    end

    test "handles case where there are no undelivered messages" do
      # Create a sink consumer
      consumer = ConsumersFactory.insert_sink_consumer!()

      # Set timestamp to 2 minutes ago
      two_minutes_ago = DateTime.add(DateTime.utc_now(), -2 * 60, :second)

      # Capture logs to verify output
      assert capture_log(fn ->
               MessageConsistencyCheckWorker.audit_and_trim_undelivered_cursors(consumer.id, two_minutes_ago)
             end) == ""

      # Verify that the undelivered cursors set is empty
      {:ok, remaining_cursors} = MessageLedgers.list_undelivered_wal_cursors(consumer.id, two_minutes_ago)
      assert length(remaining_cursors) == 0
    end

    test "handles case where messages are too recent to be considered stale" do
      # Create a sink consumer
      consumer = ConsumersFactory.insert_sink_consumer!()

      # Create some WAL cursors that are undelivered
      undelivered_cursors = [
        %{commit_lsn: 100, commit_idx: 1},
        %{commit_lsn: 200, commit_idx: 2}
      ]

      # Add cursors to the undelivered set in Redis
      :ok = MessageLedgers.wal_cursors_ingested(consumer.id, undelivered_cursors)

      # Set timestamp to 1 minute ago (not stale enough)
      one_minute_ago = DateTime.add(DateTime.utc_now(), -60, :second)

      # Capture logs to verify output
      assert capture_log(fn ->
               MessageConsistencyCheckWorker.audit_and_trim_undelivered_cursors(consumer.id, one_minute_ago)
             end) == ""

      # Verify that the undelivered cursors set still contains the messages
      {:ok, remaining_cursors} = MessageLedgers.list_undelivered_wal_cursors(consumer.id, DateTime.utc_now())
      assert length(remaining_cursors) == 2
    end
  end
end

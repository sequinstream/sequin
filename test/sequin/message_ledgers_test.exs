defmodule Sequin.ConsumersRuntime.MessageLedgersTest do
  use Sequin.Case, async: true

  alias Sequin.ConsumersRuntime.MessageLedgers
  alias Sequin.Factory

  setup do
    consumer_id = Factory.uuid()

    on_exit(fn ->
      MessageLedgers.drop_for_consumer(consumer_id)
    end)

    %{consumer_id: consumer_id}
  end

  describe "wal_cursors_delivered/2" do
    test "marks wal cursors as delivered and removes from verification set", %{consumer_id: consumer_id} do
      now = DateTime.utc_now()

      wal_cursors = [
        %{commit_lsn: 1, commit_idx: 0, commit_timestamp: datetime(now, 100)},
        %{commit_lsn: 1, commit_idx: 1, commit_timestamp: datetime(now, 100)},
        %{commit_lsn: 3, commit_idx: 0, commit_timestamp: datetime(now, 100)}
      ]

      # First record them in verification set
      :ok = MessageLedgers.wal_cursors_ingested(consumer_id, wal_cursors)
      # Then mark as delivered
      assert :ok = MessageLedgers.wal_cursors_delivered(consumer_id, wal_cursors)

      # Verify they're marked as delivered
      {:ok, delivered} = MessageLedgers.filter_delivered_wal_cursors(consumer_id, wal_cursors)
      assert delivered == wal_cursors

      # Verify they're removed from verification set
      {:ok, undelivered} = MessageLedgers.list_undelivered_wal_cursors(consumer_id, DateTime.utc_now())
      assert undelivered == []
    end
  end

  describe "filter_delivered_wal_cursors/2" do
    test "returns only delivered wal cursors", %{consumer_id: consumer_id} do
      now = DateTime.utc_now()

      delivered_cursors = [
        %{commit_lsn: 1, commit_idx: 0, commit_timestamp: datetime(now, 100)},
        %{commit_lsn: 1, commit_idx: 1, commit_timestamp: datetime(now, 100)}
      ]

      undelivered_cursors = [
        %{commit_lsn: 3, commit_idx: 0, commit_timestamp: datetime(now, 100)},
        %{commit_lsn: 3, commit_idx: 1, commit_timestamp: datetime(now, 100)}
      ]

      :ok = MessageLedgers.wal_cursors_delivered(consumer_id, delivered_cursors)

      {:ok, delivered} =
        MessageLedgers.filter_delivered_wal_cursors(
          consumer_id,
          delivered_cursors ++ undelivered_cursors
        )

      assert delivered == delivered_cursors
    end
  end

  describe "wal_cursors_ingested/2" do
    test "records wal cursors with timestamps", %{consumer_id: consumer_id} do
      now = DateTime.utc_now()

      wal_cursors = [
        %{commit_lsn: 123, commit_idx: 456, commit_timestamp: datetime(now, 100)},
        %{commit_lsn: 124, commit_idx: 457, commit_timestamp: datetime(now, 200)}
      ]

      assert :ok = MessageLedgers.wal_cursors_ingested(consumer_id, wal_cursors)

      # Verify the wal cursors were recorded
      {:ok, undelivered} = MessageLedgers.list_undelivered_wal_cursors(consumer_id, datetime(now, 300))
      assert length(undelivered) == 2
      assert assert_lists_equal(undelivered, wal_cursors, &assert_maps_equal(&1, &2, [:commit_lsn, :commit_idx]))
    end
  end

  describe "trim_delivered_cursors_set/2" do
    test "removes wal cursors up to the specified sequence", %{consumer_id: consumer_id} do
      now = DateTime.utc_now()

      delivered_cursors = [
        %{commit_lsn: 1, commit_idx: 0, commit_timestamp: datetime(now, 100)},
        %{commit_lsn: 1, commit_idx: 1, commit_timestamp: datetime(now, 100)},
        %{commit_lsn: 3, commit_idx: 0, commit_timestamp: datetime(now, 100)},
        %{commit_lsn: 3, commit_idx: 1, commit_timestamp: datetime(now, 100)}
      ]

      :ok = MessageLedgers.wal_cursors_delivered(consumer_id, delivered_cursors)
      {:ok, 2} = MessageLedgers.trim_delivered_cursors_set(consumer_id, %{commit_lsn: 3, commit_idx: 0})

      {:ok, delivered} = MessageLedgers.filter_delivered_wal_cursors(consumer_id, delivered_cursors)

      assert delivered == [
               %{commit_lsn: 3, commit_idx: 0, commit_timestamp: datetime(now, 100)},
               %{commit_lsn: 3, commit_idx: 1, commit_timestamp: datetime(now, 100)}
             ]
    end
  end

  describe "trim_stale_ingested_wal_cursors/2" do
    test "removes wal cursors older than specified timestamp", %{consumer_id: consumer_id} do
      now = DateTime.utc_now()

      wal_cursors = [
        # old
        %{commit_lsn: 100, commit_idx: 200, commit_timestamp: datetime(now, -100)},
        # old
        %{commit_lsn: 101, commit_idx: 201, commit_timestamp: datetime(now, -50)},
        # current
        %{commit_lsn: 102, commit_idx: 202, commit_timestamp: datetime(now, 100)}
      ]

      # Record all wal cursors
      :ok = MessageLedgers.wal_cursors_ingested(consumer_id, wal_cursors)

      # Trim wal cursors older than (now - 25)
      assert :ok = MessageLedgers.trim_stale_ingested_wal_cursors(consumer_id, datetime(now, -25))

      # Verify only newer wal cursor remains
      {:ok, [cursor]} = MessageLedgers.list_undelivered_wal_cursors(consumer_id, datetime(now, 200))
      assert cursor.commit_lsn == 102
      assert cursor.commit_idx == 202
    end

    test "handles empty set gracefully", %{consumer_id: consumer_id} do
      assert :ok = MessageLedgers.trim_stale_ingested_wal_cursors(consumer_id, DateTime.utc_now())
    end
  end

  describe "list_undelivered_wal_cursors/2" do
    test "returns wal cursors older than specified timestamp", %{consumer_id: consumer_id} do
      now = DateTime.utc_now()

      old_cursors = [
        %{commit_lsn: 100, commit_idx: 200, commit_timestamp: datetime(now, -100)},
        %{commit_lsn: 101, commit_idx: 201, commit_timestamp: datetime(now, -50)}
      ]

      new_cursor = %{commit_lsn: 102, commit_idx: 202, commit_timestamp: datetime(now, 100)}

      # Record all cursors
      :ok = MessageLedgers.wal_cursors_ingested(consumer_id, old_cursors)
      :ok = MessageLedgers.wal_cursors_ingested(consumer_id, [new_cursor])

      # Get cursors older than (now - 25)
      {:ok, unverified} = MessageLedgers.list_undelivered_wal_cursors(consumer_id, datetime(now, -25))

      assert length(unverified) == 2
      assert assert_lists_equal(unverified, old_cursors, &assert_maps_equal(&1, &2, [:commit_lsn, :commit_idx]))
    end

    test "returns empty list when no wal cursors exist", %{consumer_id: consumer_id} do
      {:ok, unverified} = MessageLedgers.list_undelivered_wal_cursors(consumer_id, DateTime.utc_now())
      assert unverified == []
    end

    test "excludes delivered wal cursors", %{consumer_id: consumer_id} do
      now = DateTime.utc_now()

      wal_cursors = [
        %{commit_lsn: 100, commit_idx: 200, commit_timestamp: datetime(now, -100)},
        %{commit_lsn: 101, commit_idx: 201, commit_timestamp: datetime(now, -50)}
      ]

      # Record and then mark as delivered
      :ok = MessageLedgers.wal_cursors_ingested(consumer_id, wal_cursors)
      :ok = MessageLedgers.wal_cursors_delivered(consumer_id, wal_cursors)

      {:ok, unverified} = MessageLedgers.list_undelivered_wal_cursors(consumer_id, datetime(now, 100))
      assert unverified == []
    end
  end

  defp datetime(now, from_now) do
    now |> DateTime.add(from_now, :second) |> DateTime.truncate(:second)
  end
end

defmodule Sequin.ConsumersRuntime.AtLeastOnceVerificationTest do
  use Sequin.Case, async: true

  alias Sequin.ConsumersRuntime.AtLeastOnceVerification
  alias Sequin.Factory
  alias Sequin.Redis

  setup do
    consumer_id = Factory.uuid()
    commit_key = "consumer:#{consumer_id}:commit_verification"

    on_exit(fn ->
      Redis.command(["DEL", commit_key])
    end)

    %{consumer_id: consumer_id}
  end

  describe "record_commit_tuples/2" do
    test "records commit tuples with timestamps", %{consumer_id: consumer_id} do
      now = DateTime.utc_now()

      commits = [
        %{commit_lsn: 123, commit_idx: 456, commit_timestamp: datetime(now, 100)},
        %{commit_lsn: 124, commit_idx: 457, commit_timestamp: datetime(now, 200)}
      ]

      assert :ok = AtLeastOnceVerification.record_commit_tuples(consumer_id, commits)

      # Verify the commits were recorded
      {:ok, commits} = AtLeastOnceVerification.all_commit_tuples(consumer_id)
      assert length(commits) == 2
      assert assert_lists_equal(commits, commits, &assert_maps_equal(&1, &2, [:commit_lsn, :commit_idx]))
    end

    test "can record multiple batches", %{consumer_id: consumer_id} do
      now = DateTime.utc_now()

      commits = [
        %{commit_lsn: 100, commit_idx: 200, commit_timestamp: datetime(now, 100)},
        %{commit_lsn: 101, commit_idx: 201, commit_timestamp: datetime(now, 200)},
        %{commit_lsn: 102, commit_idx: 202, commit_timestamp: datetime(now, 300)}
      ]

      assert :ok = AtLeastOnceVerification.record_commit_tuples(consumer_id, commits)

      # Verify all commits were recorded
      {:ok, 3} = AtLeastOnceVerification.count_commit_tuples(consumer_id)
    end
  end

  describe "remove_commit_tuples/2" do
    test "removes specific commit tuples", %{consumer_id: consumer_id} do
      now = DateTime.utc_now()

      commits = [
        %{commit_lsn: 123, commit_idx: 456, commit_timestamp: datetime(now, 100)},
        %{commit_lsn: 124, commit_idx: 457, commit_timestamp: datetime(now, 200)}
      ]

      :ok = AtLeastOnceVerification.record_commit_tuples(consumer_id, commits)

      assert :ok = AtLeastOnceVerification.remove_commit_tuples(consumer_id, commits)

      # Verify the commits were removed
      {:ok, commits} = AtLeastOnceVerification.all_commit_tuples(consumer_id)
      assert commits == []
    end

    test "returns ok when removing non-existent commit tuples", %{consumer_id: consumer_id} do
      assert :ok =
               AtLeastOnceVerification.remove_commit_tuples(consumer_id, [
                 %{commit_lsn: 999, commit_idx: 999},
                 %{commit_lsn: 1000, commit_idx: 1000}
               ])
    end
  end

  describe "get_unverified_commit_tuples/2" do
    test "returns commit tuples older than specified timestamp", %{consumer_id: consumer_id} do
      now = DateTime.utc_now()

      old_commits = [
        %{commit_lsn: 100, commit_idx: 200, commit_timestamp: datetime(now, -100)},
        %{commit_lsn: 101, commit_idx: 201, commit_timestamp: datetime(now, -50)}
      ]

      new_commit = %{commit_lsn: 102, commit_idx: 202, commit_timestamp: datetime(now, 100)}

      # Record all commits
      :ok = AtLeastOnceVerification.record_commit_tuples(consumer_id, old_commits)

      :ok = AtLeastOnceVerification.record_commit_tuples(consumer_id, [new_commit])

      # Get commits older than (now - 25)
      {:ok, unverified} = AtLeastOnceVerification.get_unverified_commit_tuples(consumer_id, datetime(now, -25))

      assert length(unverified) == 2
      assert assert_lists_equal(unverified, old_commits, &assert_maps_equal(&1, &2, [:commit_lsn, :commit_idx]))
    end

    test "returns empty list when no commit tuples exist", %{consumer_id: consumer_id} do
      {:ok, unverified} = AtLeastOnceVerification.get_unverified_commit_tuples(consumer_id, DateTime.utc_now())
      assert unverified == []
    end
  end

  describe "trim_commit_tuples/2" do
    test "removes commit tuples older than specified timestamp", %{consumer_id: consumer_id} do
      now = DateTime.utc_now()

      commit_tuples = [
        # old
        %{commit_lsn: 100, commit_idx: 200, commit_timestamp: datetime(now, -100)},
        # old
        %{commit_lsn: 101, commit_idx: 201, commit_timestamp: datetime(now, -50)},
        # current
        %{commit_lsn: 102, commit_idx: 202, commit_timestamp: datetime(now, 100)}
      ]

      # Record all commits
      :ok = AtLeastOnceVerification.record_commit_tuples(consumer_id, commit_tuples)

      # Trim commits older than (now - 25)
      assert :ok = AtLeastOnceVerification.trim_commit_tuples(consumer_id, datetime(now, -25))

      # Verify only newer commit remains
      {:ok, 1} = AtLeastOnceVerification.count_commit_tuples(consumer_id)

      {:ok, [commit]} = AtLeastOnceVerification.all_commit_tuples(consumer_id)
      assert commit == %{commit_lsn: 102, commit_idx: 202, commit_timestamp: datetime(now, 100)}
    end

    test "handles empty set gracefully", %{consumer_id: consumer_id} do
      assert :ok = AtLeastOnceVerification.trim_commit_tuples(consumer_id, DateTime.utc_now())
    end
  end

  defp datetime(now, from_now) do
    now |> DateTime.add(from_now, :second) |> DateTime.truncate(:second)
  end
end

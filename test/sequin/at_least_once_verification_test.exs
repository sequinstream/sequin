defmodule Sequin.ConsumersRuntime.AtLeastOnceVerificationTest do
  use ExUnit.Case, async: true

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

  describe "record_commit/3" do
    test "records a commit tuple with timestamp", %{consumer_id: consumer_id} do
      commit = {123, 456}
      timestamp = System.system_time(:second)

      assert :ok = AtLeastOnceVerification.record_commit(consumer_id, commit, timestamp)

      # Verify the commit was recorded with correct score
      {:ok, commits} = AtLeastOnceVerification.all_commits(consumer_id)
      assert length(commits) == 1
      assert List.first(commits) == {commit, timestamp}
    end

    test "can record multiple commits", %{consumer_id: consumer_id} do
      commits = [
        {{100, 200}, 1000},
        {{101, 201}, 1001},
        {{102, 202}, 1002}
      ]

      Enum.each(commits, fn {commit, ts} ->
        assert :ok = AtLeastOnceVerification.record_commit(consumer_id, commit, ts)
      end)

      # Verify all commits were recorded
      {:ok, count} = AtLeastOnceVerification.count_commits(consumer_id)
      assert String.to_integer(count) == 3
    end
  end

  describe "remove_commit/2" do
    test "removes a specific commit tuple", %{consumer_id: consumer_id} do
      commit = {123, 456}
      timestamp = System.system_time(:second)

      :ok = AtLeastOnceVerification.record_commit(consumer_id, commit, timestamp)
      assert :ok = AtLeastOnceVerification.remove_commit(consumer_id, commit)

      # Verify the commit was removed
      {:ok, members} = Redis.command(["ZRANGE", "consumer:#{consumer_id}:commit_verification", 0, -1])
      assert members == []
    end

    test "returns ok when removing non-existent commit", %{consumer_id: consumer_id} do
      assert :ok = AtLeastOnceVerification.remove_commit(consumer_id, {999, 999})
    end
  end

  describe "get_unverified_commits/2" do
    test "returns commits older than specified timestamp", %{consumer_id: consumer_id} do
      now = System.system_time(:second)

      old_commits = [
        {{100, 200}, now - 100},
        {{101, 201}, now - 50}
      ]

      new_commit = {{102, 202}, now}

      # Record all commits
      Enum.each(old_commits, fn {commit, ts} ->
        :ok = AtLeastOnceVerification.record_commit(consumer_id, commit, ts)
      end)

      :ok = AtLeastOnceVerification.record_commit(consumer_id, elem(new_commit, 0), elem(new_commit, 1))

      # Get commits older than (now - 25)
      {:ok, unverified} = AtLeastOnceVerification.get_unverified_commits(consumer_id, now - 25)

      assert length(unverified) == 2
      assert Enum.all?(unverified, fn {_commit, ts} -> ts < now - 25 end)
    end

    test "returns empty list when no commits exist", %{consumer_id: consumer_id} do
      {:ok, unverified} = AtLeastOnceVerification.get_unverified_commits(consumer_id, System.system_time(:second))
      assert unverified == []
    end
  end

  describe "trim_commits/2" do
    test "removes commits older than specified timestamp", %{consumer_id: consumer_id} do
      now = System.system_time(:second)

      commits = [
        # old
        {{100, 200}, now - 100},
        # old
        {{101, 201}, now - 50},
        # current
        {{102, 202}, now}
      ]

      # Record all commits
      Enum.each(commits, fn {commit, ts} ->
        :ok = AtLeastOnceVerification.record_commit(consumer_id, commit, ts)
      end)

      # Trim commits older than (now - 25)
      assert :ok = AtLeastOnceVerification.trim_commits(consumer_id, now - 25)

      # Verify only newer commit remains
      {:ok, remaining} = AtLeastOnceVerification.count_commits(consumer_id)
      assert String.to_integer(remaining) == 1

      {:ok, [member]} = AtLeastOnceVerification.all_commits(consumer_id)
      assert member == {{102, 202}, now}
    end

    test "handles empty set gracefully", %{consumer_id: consumer_id} do
      assert :ok = AtLeastOnceVerification.trim_commits(consumer_id, System.system_time(:second))
    end
  end
end

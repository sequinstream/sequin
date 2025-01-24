defmodule Sequin.ConsumerIdempotencyTest do
  use ExUnit.Case, async: true

  alias Sequin.ConsumersRuntime.ConsumerIdempotency
  alias Sequin.Factory

  setup do
    [consumer_id: Factory.uuid()]
  end

  describe "mark_messages_delivered/2" do
    test "marks messages as delivered", %{consumer_id: consumer_id} do
      delivered_commits = [
        %{commit_lsn: 1, commit_idx: 0},
        %{commit_lsn: 1, commit_idx: 1},
        %{commit_lsn: 3, commit_idx: 0},
        %{commit_lsn: 3, commit_idx: 1}
      ]

      assert :ok = ConsumerIdempotency.mark_messages_delivered(consumer_id, delivered_commits)

      {:ok, delivered} = ConsumerIdempotency.delivered_messages(consumer_id, delivered_commits)
      assert delivered == delivered_commits
    end
  end

  describe "delivered_messages/2" do
    test "returns only delivered message tuples", %{consumer_id: consumer_id} do
      delivered_commits = [
        %{commit_lsn: 1, commit_idx: 0},
        %{commit_lsn: 1, commit_idx: 1},
        %{commit_lsn: 3, commit_idx: 0}
      ]

      undelivered_commits = [
        %{commit_lsn: 3, commit_idx: 1},
        %{commit_lsn: 5, commit_idx: 0},
        %{commit_lsn: 5, commit_idx: 1}
      ]

      :ok = ConsumerIdempotency.mark_messages_delivered(consumer_id, delivered_commits)

      {:ok, delivered} =
        ConsumerIdempotency.delivered_messages(
          consumer_id,
          delivered_commits ++ undelivered_commits
        )

      assert delivered == delivered_commits
    end
  end

  describe "trim/2" do
    test "removes messages up to the specified sequence", %{consumer_id: consumer_id} do
      delivered_commits = [
        %{commit_lsn: 1, commit_idx: 0},
        %{commit_lsn: 1, commit_idx: 1},
        %{commit_lsn: 3, commit_idx: 0},
        %{commit_lsn: 3, commit_idx: 1}
      ]

      :ok = ConsumerIdempotency.mark_messages_delivered(consumer_id, delivered_commits)
      assert {:ok, delivered_commits} == ConsumerIdempotency.delivered_messages(consumer_id, delivered_commits)

      {:ok, 2} = ConsumerIdempotency.trim(consumer_id, %{commit_lsn: 3, commit_idx: 0})

      {:ok, delivered} = ConsumerIdempotency.delivered_messages(consumer_id, delivered_commits)

      assert delivered == [
               %{commit_lsn: 3, commit_idx: 0},
               %{commit_lsn: 3, commit_idx: 1}
             ]
    end
  end
end

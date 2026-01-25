defmodule Sequin.Runtime.BenchmarkPipelineTest do
  @moduledoc """
  Tests for the BenchmarkPipeline sink.

  These tests verify that BenchmarkPipeline correctly tracks checksums and that
  messages flow through the full pipeline correctly.
  """
  use Sequin.DataCase, async: true

  alias Sequin.Benchmark.Stats
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Runtime.SlotMessageStore
  alias Sequin.Runtime.SlotMessageStoreSupervisor

  describe "checksums/1 and reset_for_owner/1" do
    test "returns empty map when no checksums exist" do
      checksums = Stats.checksums("nonexistent-consumer")
      assert checksums == %{}
    end

    test "reset_for_owner/1 removes all data for a consumer" do
      consumer_id = "test-consumer-#{System.unique_integer()}"

      Stats.init_for_owner(consumer_id, 2, scope: :pipeline)

      Stats.message_emitted(%Stats.Message{
        owner_id: consumer_id,
        partition: 0,
        commit_lsn: 100,
        commit_idx: 0,
        scope: :pipeline
      })

      Stats.message_emitted(%Stats.Message{
        owner_id: consumer_id,
        partition: 1,
        commit_lsn: 200,
        commit_idx: 0,
        scope: :pipeline
      })

      Stats.reset_for_owner(consumer_id)

      checksums = Stats.checksums(consumer_id)
      assert checksums == %{}
    end
  end

  describe "messages flow through SlotMessageStore to benchmark pipeline" do
    setup do
      account = AccountsFactory.insert_account!()
      database = DatabasesFactory.insert_postgres_database!(account_id: account.id)

      replication =
        ReplicationFactory.insert_postgres_replication!(
          account_id: account.id,
          postgres_database_id: database.id
        )

      # Create a benchmark consumer
      consumer =
        ConsumersFactory.insert_sink_consumer!(
          account_id: account.id,
          type: :benchmark,
          sink: %{type: :benchmark, partition_count: 4},
          replication_slot_id: replication.id
        )

      {:ok, %{consumer: consumer, database: database}}
    end

    test "messages are processed and checksums are tracked", %{consumer: consumer} do
      # Set 100% sample rate for testing
      Application.put_env(:sequin, Sequin.Benchmark.Stats, checksum_sample_rate: 1.0)
      on_exit(fn -> Application.delete_env(:sequin, Sequin.Benchmark.Stats) end)

      test_pid = self()

      # Start the SlotMessageStoreSupervisor
      start_supervised!({SlotMessageStoreSupervisor, [consumer_id: consumer.id, test_pid: test_pid]})

      # Create multiple consumer events with known commit_lsn and commit_idx
      events =
        for i <- 1..10 do
          ConsumersFactory.consumer_event(
            consumer_id: consumer.id,
            commit_lsn: 1000 + i,
            commit_idx: i,
            group_id: "group-#{rem(i, 4)}"
          )
        end

      # Put messages into the store
      SlotMessageStore.put_messages(consumer, events)

      # Start the pipeline
      start_supervised!({SinkPipeline, [consumer_id: consumer.id, test_pid: test_pid]})

      # Wait for all messages to be processed
      await_acks(10)

      # Verify group checksums were tracked (BenchmarkPipeline uses message_received_for_group)
      group_checksums = Stats.group_checksums(consumer.id)
      assert map_size(group_checksums) == 4

      # Verify total count matches
      total_count =
        Enum.reduce(group_checksums, 0, fn {_group_id, {_checksum, count}}, acc -> acc + count end)

      assert total_count == 10
    end

    test "checksum computation matches expected formula", %{consumer: consumer} do
      # Set 100% sample rate for testing
      Application.put_env(:sequin, Sequin.Benchmark.Stats, checksum_sample_rate: 1.0)
      on_exit(fn -> Application.delete_env(:sequin, Sequin.Benchmark.Stats) end)

      test_pid = self()

      # Start the SlotMessageStoreSupervisor
      start_supervised!({SlotMessageStoreSupervisor, [consumer_id: consumer.id, test_pid: test_pid]})

      # Create a single event with known values
      group_id = "test-group"
      commit_lsn = 12_345
      commit_idx = 0

      event =
        ConsumersFactory.consumer_event(
          consumer_id: consumer.id,
          commit_lsn: commit_lsn,
          commit_idx: commit_idx,
          group_id: group_id
        )

      # Put the message into the store
      SlotMessageStore.put_messages(consumer, [event])

      # Start the pipeline
      start_supervised!({SinkPipeline, [consumer_id: consumer.id, test_pid: test_pid]})

      # Wait for message to be processed
      await_acks(1)

      # Compute expected checksum manually (group checksums are keyed by group_id, not partition)
      expected_checksum = :erlang.crc32(<<0::32, commit_lsn::64, commit_idx::32>>)

      group_checksums = Stats.group_checksums(consumer.id)
      {actual_checksum, count} = group_checksums[group_id]

      assert count == 1
      assert actual_checksum == expected_checksum
    end

    test "checksums are order-sensitive (rolling checksum)", %{consumer: consumer} do
      # Set 100% sample rate for testing
      Application.put_env(:sequin, Sequin.Benchmark.Stats, checksum_sample_rate: 1.0)
      on_exit(fn -> Application.delete_env(:sequin, Sequin.Benchmark.Stats) end)

      test_pid = self()

      # Start the SlotMessageStoreSupervisor
      start_supervised!({SlotMessageStoreSupervisor, [consumer_id: consumer.id, test_pid: test_pid]})

      # Create two events for the same group_id
      group_id = "same-group"

      events = [
        ConsumersFactory.consumer_event(
          consumer_id: consumer.id,
          commit_lsn: 100,
          commit_idx: 0,
          group_id: group_id
        ),
        ConsumersFactory.consumer_event(
          consumer_id: consumer.id,
          commit_lsn: 200,
          commit_idx: 0,
          group_id: group_id
        )
      ]

      # Put messages into the store
      SlotMessageStore.put_messages(consumer, events)

      # Start the pipeline
      start_supervised!({SinkPipeline, [consumer_id: consumer.id, test_pid: test_pid]})

      # Wait for messages to be processed
      await_acks(2)

      group_checksums = Stats.group_checksums(consumer.id)
      {actual_checksum, count} = group_checksums[group_id]

      assert count == 2

      # Verify the checksum is a rolling checksum (not just a single value).
      # BenchmarkPipeline sorts by (commit_lsn, commit_idx) so order should be deterministic.
      # Order: 100 then 200
      checksum_100_first = :erlang.crc32(<<0::32, 100::64, 0::32>>)
      expected_checksum = :erlang.crc32(<<checksum_100_first::32, 200::64, 0::32>>)

      assert actual_checksum == expected_checksum
    end
  end

  defp await_acks(count, acc \\ [])

  defp await_acks(count, acc) when length(acc) >= count do
    acc
  end

  defp await_acks(count, acc) do
    receive do
      {SinkPipeline, :ack_finished, successful_ack_ids, []} ->
        await_acks(count, acc ++ successful_ack_ids)
    after
      5_000 ->
        raise "Timed out waiting for acks. Expected #{count}, got #{length(acc)}"
    end
  end
end

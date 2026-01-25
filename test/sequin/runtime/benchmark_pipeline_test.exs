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
      Stats.message_emitted(consumer_id, 0, 100, 0, scope: :pipeline)
      Stats.message_emitted(consumer_id, 1, 200, 0, scope: :pipeline)

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

      # Verify checksums were tracked
      checksums = Stats.checksums(consumer.id)
      assert map_size(checksums) == 4

      # Verify total count matches
      total_count =
        Enum.reduce(checksums, 0, fn {_partition, {_checksum, count}}, acc -> acc + count end)

      assert total_count == 10
    end

    test "checksum computation matches expected formula", %{consumer: consumer} do
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

      # Compute expected checksum manually
      partition = :erlang.phash2(group_id, 4)
      expected_checksum = :erlang.crc32(<<0::32, commit_lsn::64, commit_idx::32>>)

      checksums = Stats.checksums(consumer.id)
      {actual_checksum, count} = checksums[partition]

      assert count == 1
      assert actual_checksum == expected_checksum
    end

    test "checksums are order-sensitive (rolling checksum)", %{consumer: consumer} do
      test_pid = self()

      # Start the SlotMessageStoreSupervisor
      start_supervised!({SlotMessageStoreSupervisor, [consumer_id: consumer.id, test_pid: test_pid]})

      # Create two events for the same partition
      group_id = "same-partition"
      partition = :erlang.phash2(group_id, 4)

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

      checksums = Stats.checksums(consumer.id)
      {actual_checksum, count} = checksums[partition]

      assert count == 2

      # Verify the checksum is a rolling checksum (not just a single value).
      # The order of processing within a batch may vary, so we accept either order.
      # Order A: 100 then 200
      checksum_100_first = :erlang.crc32(<<0::32, 100::64, 0::32>>)
      expected_order_a = :erlang.crc32(<<checksum_100_first::32, 200::64, 0::32>>)

      # Order B: 200 then 100
      checksum_200_first = :erlang.crc32(<<0::32, 200::64, 0::32>>)
      expected_order_b = :erlang.crc32(<<checksum_200_first::32, 100::64, 0::32>>)

      assert actual_checksum in [expected_order_a, expected_order_b],
             "Checksum #{actual_checksum} doesn't match expected order A (#{expected_order_a}) or B (#{expected_order_b})"
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

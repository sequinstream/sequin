defmodule Sequin.Runtime.SlotProducer.ReorderBufferTest do
  @moduledoc """
  Tests for ReorderBuffer GenStage consumer that reorders messages from multiple producer partitions.

  This test creates multiple fake GenStage producers that send messages and batch markers
  to the ReorderBuffer, verifying that messages are properly reordered and flushed when
  all partitions complete their batches.
  """
  use Sequin.DataCase, async: true
  use AssertEventually, interval: 1

  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Runtime.SlotProducer.Message
  alias Sequin.Runtime.SlotProducer.ReorderBuffer

  @reorder_buffer_id Module.concat(__MODULE__, ReorderBuffer)
  def reorder_buffer_id, do: @reorder_buffer_id

  defmodule TestProducer do
    @moduledoc """
    A fake GenStage producer for testing ReorderBuffer.

    Sends messages and batch markers to subscribed consumers.
    """
    use GenStage

    alias Sequin.Runtime.SlotProducer.ReorderBufferTest

    def start_link(opts) do
      GenStage.start_link(__MODULE__, opts)
    end

    def send_message(producer, message) do
      GenStage.call(producer, {:send_message, message})
    end

    def send_batch_marker(producer, batch_marker, partition_idx) do
      GenStage.sync_info(producer, {:send_batch_marker, batch_marker, partition_idx})
    end

    def get_demand(producer) do
      GenStage.call(producer, :get_demand)
    end

    def init(opts) do
      {:producer,
       %{
         demand: 0,
         pending_events: [],
         pending_markers: [],
         partition_idx: Keyword.fetch!(opts, :partition_idx)
       }}
    end

    def handle_demand(incoming_demand, %{demand: demand} = state) do
      new_demand = demand + incoming_demand
      {events, remaining_events} = Enum.split(state.pending_events, new_demand)

      state = %{state | demand: new_demand - length(events), pending_events: remaining_events}

      Process.send_after(self(), :maybe_send_batch_markers, 0)

      {:noreply, events, state}
    end

    def handle_call({:send_message, message}, _from, state) do
      if state.demand > 0 do
        {:reply, :ok, [message], %{state | demand: state.demand - 1}}
      else
        {:reply, :ok, [], %{state | pending_events: state.pending_events ++ [message]}}
      end
    end

    def handle_call(:get_demand, _from, state) do
      {:reply, state.demand, [], state}
    end

    def handle_info({:send_batch_marker, batch_marker, partition_idx}, state) do
      # Send batch marker to ReorderBuffer directly with producer_partition_idx set
      marker_with_partition = %{batch_marker | producer_partition_idx: partition_idx}
      state = %{state | pending_markers: [marker_with_partition | state.pending_markers]}
      state = maybe_send_markers(state)

      {:noreply, [], state}
    end

    def handle_info(:maybe_send_batch_markers, state) do
      state = maybe_send_markers(state)

      {:noreply, [], state}
    end

    defp maybe_send_markers(state) do
      if state.pending_events == [] do
        Enum.each(Enum.reverse(state.pending_markers), fn marker ->
          :ok = ReorderBuffer.handle_batch_marker(ReorderBufferTest.reorder_buffer_id(), marker)
        end)

        %{state | pending_markers: []}
      else
        state
      end
    end
  end

  describe "ReorderBuffer with multiple producer partitions" do
    setup ctx do
      producers =
        if !ctx[:skip_start] do
          ctx |> Map.get(:buffer_opts, []) |> start_buffer() |> Map.new()
        end

      {:ok, %{producers: producers}}
    end

    test "flushes messages when all partitions send batch markers", %{
      producers: producers
    } do
      # Create messages for idx 0
      messages_by_partition =
        for {partition_idx, producer_pid} <- producers, into: %{} do
          messages = [
            ReplicationFactory.message(batch_idx: 0),
            ReplicationFactory.message(batch_idx: 0)
          ]

          # Send messages from this partition
          for message <- messages do
            :ok = TestProducer.send_message(producer_pid, message)
          end

          {partition_idx, messages}
        end

      # Create batch marker for idx 0
      batch_marker =
        ReplicationFactory.batch_marker(
          idx: 0,
          high_watermark_wal_cursor: %{commit_lsn: 1000, commit_idx: 0}
        )

      # Send batch markers from all partitions except the last one
      [{first_partition_idx, first_producer_pid} | rest] = Enum.to_list(producers)

      for {partition_idx, producer_pid} <- rest do
        :ok = TestProducer.send_batch_marker(producer_pid, batch_marker, partition_idx)
      end

      # Should not receive batch_ready yet
      refute_receive {:batch_ready, _, _, _}, 50

      # Send batch marker from the last partition
      :ok = TestProducer.send_batch_marker(first_producer_pid, batch_marker, first_partition_idx)

      # Should receive batch_ready
      assert_receive {:batch_ready,
                      %{
                        idx: received_idx,
                        high_watermark_wal_cursor: received_high_watermark,
                        messages: received_messages
                      }},
                     1000

      # Verify the batch marker
      assert received_idx == 0
      assert received_high_watermark.commit_lsn == 1000

      # Verify all messages are included
      all_expected_messages = messages_by_partition |> Map.values() |> List.flatten()
      assert length(received_messages) == length(all_expected_messages)

      # Verify all messages have the correct idx
      assert Enum.all?(received_messages, fn %Message{batch_idx: idx} -> idx == 0 end)

      # Should not receive another batch_ready message
      refute_receive {:batch_ready, _}, 50
    end

    test "orders messages by {commit_lsn, commit_idx} ascending", %{
      producers: producers
    } do
      # Create messages with specific LSN/idx values to test ordering
      for {partition_idx, producer_pid} <- producers do
        # Create messages with intentionally out-of-order LSN/idx
        messages = [
          ReplicationFactory.message(batch_idx: 0, commit_lsn: 2000 + partition_idx, commit_idx: 1),
          ReplicationFactory.message(batch_idx: 0, commit_lsn: 1000 + partition_idx, commit_idx: 2),
          ReplicationFactory.message(batch_idx: 0, commit_lsn: 2000 + partition_idx, commit_idx: 0),
          ReplicationFactory.message(batch_idx: 0, commit_lsn: 1000 + partition_idx, commit_idx: 1)
        ]

        # Send messages from this partition
        for message <- messages do
          :ok = TestProducer.send_message(producer_pid, message)
        end
      end

      # Create batch marker for idx 0
      batch_marker =
        ReplicationFactory.batch_marker(
          idx: 0,
          high_watermark_wal_cursor: %{commit_lsn: 3000, commit_idx: 0}
        )

      # Send batch markers from all partitions
      for {partition_idx, producer_pid} <- producers do
        :ok = TestProducer.send_batch_marker(producer_pid, batch_marker, partition_idx)
      end

      # Should receive batch_ready
      assert_receive {:batch_ready, %{messages: received_messages}}, 2000

      # Verify messages are ordered by {commit_lsn, commit_idx} ascending
      ordered_tuples =
        Enum.map(received_messages, fn %Message{commit_lsn: lsn, commit_idx: idx} -> {lsn, idx} end)

      expected_order = Enum.sort(ordered_tuples)
      assert ordered_tuples == expected_order

      # Verify we have the expected number of messages
      expected_count = map_size(producers) * 4
      assert length(received_messages) == expected_count
    end

    @tag capture_log: true
    test "raises error when batch markers complete out of order", %{
      producers: producers
    } do
      # Create messages for idxs 0 and 1
      for {_partition_idx, producer_pid} <- producers do
        messages_idx_0 = [
          ReplicationFactory.message(batch_idx: 0),
          ReplicationFactory.message(batch_idx: 0)
        ]

        messages_idx_1 = [
          ReplicationFactory.message(batch_idx: 1),
          ReplicationFactory.message(batch_idx: 1)
        ]

        # Send messages from this partition for both idxs
        for message <- messages_idx_0 ++ messages_idx_1 do
          :ok = TestProducer.send_message(producer_pid, message)
        end
      end

      # Create batch markers for both idxs
      batch_marker_0 =
        ReplicationFactory.batch_marker(idx: 0, high_watermark_wal_cursor: %{commit_lsn: 1000, commit_idx: 0})

      batch_marker_1 =
        ReplicationFactory.batch_marker(idx: 1, high_watermark_wal_cursor: %{commit_lsn: 2000, commit_idx: 0})

      # Complete idx 1 first (all partitions send markers for idx 1)
      for {partition_idx, producer_pid} <- producers do
        :ok = TestProducer.send_batch_marker(producer_pid, batch_marker_1, partition_idx)
      end

      # Because of o-o-o handle_info deliery issue with GenStage?
      Process.sleep(10)

      # Now complete idx 0 (all partitions send markers for idx 0)
      for {partition_idx, producer_pid} <- producers do
        :ok = TestProducer.send_batch_marker(producer_pid, batch_marker_0, partition_idx)
      end

      # The ReorderBuffer process should crash with our specific error
      assert_receive {:DOWN, _ref, :process, _pid,
                      {%RuntimeError{
                         message: "Batch idxs completed out-of-order: other_idx=0 min_ready_idx=1"
                       }, _stacktrace}},
                     1000
    end

    @tag buffer_opts: [setting_min_demand: 2, setting_max_demand: 5]
    test "manages demand correctly with low min/max demand settings", %{producers: producers} do
      # Create a few messages for idx 0
      test_messages = [
        ReplicationFactory.message(batch_idx: 0),
        ReplicationFactory.message(batch_idx: 0),
        ReplicationFactory.message(batch_idx: 0)
      ]

      # Send messages from first partition only
      [{_first_partition_idx, first_producer_pid} | _rest] = Enum.to_list(producers)

      for message <- test_messages do
        :ok = TestProducer.send_message(first_producer_pid, message)
      end

      # Send more messages to verify demand is being asked for more
      additional_messages = [
        ReplicationFactory.message(batch_idx: 0),
        ReplicationFactory.message(batch_idx: 0)
      ]

      for message <- additional_messages do
        :ok = TestProducer.send_message(first_producer_pid, message)
      end

      # Create batch marker for idx 0
      batch_marker =
        ReplicationFactory.batch_marker(idx: 0, high_watermark_wal_cursor: %{commit_lsn: 1000, commit_idx: 0})

      # Send batch markers from all partitions
      for {partition_idx, producer_pid} <- producers do
        :ok = TestProducer.send_batch_marker(producer_pid, batch_marker, partition_idx)
      end

      # Should receive batch_ready with all messages from the first partition
      assert_receive {:batch_ready, %{messages: received_messages}}, 1000

      assert length(received_messages) >= length(test_messages) + length(additional_messages)
    end

    @tag skip_start: true
    @tag capture_log: true
    test "implements back pressure when batch flushing fails" do
      test_pid = self()
      max_demand = 100
      # Use an agent as a global to control batch flushing
      {:ok, flush_control} = Agent.start_link(fn -> :fail end)

      flush_batch_fn = fn _id, batch ->
        case Agent.get(flush_control, & &1) do
          :fail ->
            {:error, :simulated_failure}

          :succeed ->
            send(test_pid, {:batch_ready, batch})
            :ok
        end
      end

      opts = [
        min_demand: 50,
        max_demand: max_demand,
        retry_flush_batch_interval: 10,
        flush_batch_fn: flush_batch_fn
      ]

      producers = start_buffer(opts)

      # Complete first batch (will fail to flush)
      batch_marker = ReplicationFactory.batch_marker(idx: 0)

      for {idx, producer_pid} <- producers do
        :ok = TestProducer.send_message(producer_pid, ReplicationFactory.message(batch_idx: 0))
        :ok = TestProducer.send_batch_marker(producer_pid, batch_marker, idx)
      end

      # Now, completely drain demand
      for _j <- 0..(max_demand * 2), {_idx, producer_pid} <- producers do
        :ok = TestProducer.send_message(producer_pid, ReplicationFactory.message(batch_idx: 1))
      end

      # Wait for back pressure - demand drains to 0
      assert_eventually Enum.all?(producers, fn {_idx, producer} -> TestProducer.get_demand(producer) == 0 end)

      # Allow flush to succeed
      Agent.update(flush_control, fn _ -> :succeed end)

      # Should receive batch and demand should recover
      assert_receive {:batch_ready, _}, 1000

      assert_eventually Enum.all?(producers, fn {_idx, producer} -> TestProducer.get_demand(producer) >= 50 end)
    end
  end

  defp start_buffer(opts) do
    producer_partitions = Enum.random(2..5)

    test_pid = self()

    default_flush_batch_fn = fn _id, batch ->
      send(test_pid, {:batch_ready, batch})
      :ok
    end

    reorder_buffer_opts =
      Keyword.merge(
        [
          id: @reorder_buffer_id,
          account_id: UUID.uuid4(),
          producer_partitions: producer_partitions,
          flush_batch_fn: default_flush_batch_fn
        ],
        opts
      )

    {:ok, reorder_buffer_pid} = start_supervised({ReorderBuffer, reorder_buffer_opts})
    Process.monitor(reorder_buffer_pid)

    for partition_idx <- 0..(producer_partitions - 1) do
      producer_opts = [partition_idx: partition_idx]

      {:ok, producer_pid} = start_supervised({TestProducer, producer_opts}, id: {:producer, partition_idx})

      # Subscribe ReorderBuffer to this producer
      {:ok, _subscription_tag} = GenStage.sync_subscribe(reorder_buffer_pid, to: producer_pid)

      {partition_idx, producer_pid}
    end
  end
end

defmodule Sequin.Postgres.BenchmarkSourceTest do
  @moduledoc """
  Tests for the BenchmarkSource WAL message generator.

  These tests verify that BenchmarkSource generates valid WAL messages that flow
  through SlotProducer correctly, without requiring a real Postgres connection.
  """
  use Sequin.Case, async: true

  alias Sequin.Postgres.BenchmarkSource
  alias Sequin.Postgres.VirtualBackend
  alias Sequin.Runtime.SlotProducer
  alias Sequin.Runtime.SlotProducer.ProcessorBehaviour

  defmodule TestProcessor do
    @moduledoc false
    @behaviour ProcessorBehaviour

    use GenStage

    def start_link(opts), do: GenStage.start_link(__MODULE__, opts)

    def get_messages(pid), do: GenStage.call(pid, :get_messages)

    @impl ProcessorBehaviour
    def handle_relation(server, relation), do: GenStage.call(server, {:handle_relation, relation})

    @impl ProcessorBehaviour
    def handle_batch_marker(server, batch_marker), do: GenStage.sync_info(server, {:handle_batch_marker, batch_marker})

    @impl GenStage
    def init(opts) do
      %{producer: producer, test_pid: test_pid, max_demand: max_demand, min_demand: min_demand} = opts

      state = %{
        test_pid: test_pid,
        messages: [],
        producer: nil
      }

      {:consumer, state, subscribe_to: [{producer, max_demand: max_demand, min_demand: min_demand}]}
    end

    @impl GenStage
    def handle_events(events, _from, state) do
      send(state.test_pid, {:messages_received, events})
      new_messages = state.messages ++ events

      {:noreply, [], %{state | messages: new_messages}}
    end

    @impl GenStage
    def handle_call(:get_messages, _from, state), do: {:reply, state.messages, [], state}

    def handle_call({:handle_relation, relation}, _from, state) do
      send(state.test_pid, {:relation_received, relation})
      {:reply, :ok, [], state}
    end

    @impl GenStage
    def handle_subscribe(:producer, _opts, producer, state) do
      {:automatic, %{state | producer: producer}}
    end

    @impl GenStage
    def handle_info({:handle_batch_marker, batch_marker}, state) do
      send(state.test_pid, {:batch_marker_received, batch_marker})
      {:noreply, [], state}
    end
  end

  setup do
    %{id: System.unique_integer([:positive])}
  end

  describe "WAL message generation" do
    test "generates valid WAL messages that decode through SlotProducer", %{id: id} do
      start_benchmark_source!(id, transaction_sizes: [{1.0, 5}])
      producer = start_producer!(id)
      start_processor!(producer)

      messages = receive_messages(50)

      first_msg = List.first(messages)
      assert %SlotProducer.Message{} = first_msg
      assert is_integer(first_msg.commit_lsn)
      assert is_integer(first_msg.commit_idx)
      assert first_msg.kind in [:insert, :update, :delete]
    end

    test "computes checksums per partition", %{id: id} do
      partition_count = 4
      start_benchmark_source!(id, partition_count: partition_count, transaction_sizes: [{1.0, 3}])
      producer = start_producer!(id)
      start_processor!(producer)

      receive_messages(100)

      checksums = BenchmarkSource.checksums(id)
      assert map_size(checksums) == partition_count

      total_count =
        Enum.reduce(checksums, 0, fn {_partition, {_checksum, count}}, acc -> acc + count end)

      assert total_count >= 100
    end

    test "tracks stats correctly", %{id: id} do
      start_benchmark_source!(id, transaction_sizes: [{1.0, 5}])
      producer = start_producer!(id)
      start_processor!(producer)

      receive_messages(50)

      stats = BenchmarkSource.stats(id)
      assert stats.total_messages >= 50
      assert stats.total_transactions >= 10
      assert stats.total_bytes > 0
    end
  end

  # Helpers

  defp start_benchmark_source!(id, opts) do
    start_supervised!({BenchmarkSource, Keyword.put(opts, :id, id)})
  end

  defp start_producer!(id, opts \\ []) do
    producer_opts =
      Keyword.merge(
        [
          id: id,
          database_id: id,
          account_id: id,
          slot_name: "test_slot",
          publication_name: "test_publication",
          connect_opts: [id: id, source_mod: BenchmarkSource],
          pg_major_version: 17,
          backend_mod: VirtualBackend,
          conn: nil,
          ack_interval: 1000,
          restart_wal_cursor_update_interval: 5000,
          restart_wal_cursor_fn: fn _id, cursor -> cursor end,
          on_connect_fail_fn: fn _state, _reason -> :ok end,
          consumer_mod: TestProcessor,
          test_pid: self()
        ],
        opts
      )

    start_supervised!({SlotProducer, producer_opts})
  end

  defp start_processor!(producer, opts \\ []) do
    processor_opts = %{
      producer: producer,
      test_pid: self(),
      max_demand: Keyword.get(opts, :max_demand, 10),
      min_demand: Keyword.get(opts, :min_demand, 5),
      target_count: Keyword.get(opts, :target_count, 100)
    }

    start_supervised!({TestProcessor, processor_opts})
  end

  defp receive_messages(count, acc \\ [])

  defp receive_messages(count, acc) when length(acc) >= count do
    Enum.take(acc, count)
  end

  defp receive_messages(count, acc) do
    receive do
      {:messages_received, messages} -> receive_messages(count, acc ++ messages)
    after
      100 -> acc
    end
  end
end

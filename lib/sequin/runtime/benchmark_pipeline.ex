defmodule Sequin.Runtime.BenchmarkPipeline do
  @moduledoc """
  A sink pipeline for benchmarking that tracks message checksums per partition.

  This pipeline acts as a no-op destination but maintains rolling checksums
  that can be compared against BenchmarkSource checksums to verify:
  1. All messages were delivered (count matches)
  2. Messages were delivered in correct order per partition (checksum matches)

  ## Checksum Strategy

  Uses the same CRC32-based rolling checksum as BenchmarkSource:
  - Partition computed from `group_id` using `:erlang.phash2/2`
  - Checksum: `crc32(<<prev_checksum::32, commit_lsn::64, commit_idx::32>>)`

  ## Stats Storage

  Checksums and metrics are stored via `Sequin.Benchmark.Stats`.

  ## Metrics

  Also emits to Prometheus for observability:
  - `sequin_benchmark_e2e_latency_us` - End-to-end latency histogram
  - `sequin_bytes_delivered_total` - Total bytes delivered (via payload size)
  """
  @behaviour Sequin.Runtime.SinkPipeline

  alias Sequin.Benchmark.Stats
  alias Sequin.Consumers.BenchmarkSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Prometheus
  alias Sequin.Runtime.SinkPipeline

  # ============================================================================
  # SinkPipeline Callbacks
  # ============================================================================

  @impl SinkPipeline
  def init(context, _opts) do
    consumer = context.consumer
    %SinkConsumer{sink: %BenchmarkSink{partition_count: partition_count}} = consumer

    Stats.init_for_owner(consumer.id, partition_count)

    context
  end

  @impl SinkPipeline
  def batchers_config(consumer) do
    %SinkConsumer{sink: %BenchmarkSink{partition_count: partition_count}} = consumer

    [
      default: [
        concurrency: partition_count,
        batch_size: 10,
        batch_timeout: 1
      ]
    ]
  end

  @impl SinkPipeline
  def partition_by_fn(_consumer) do
    fn msg -> :erlang.phash2(msg.data.group_id) end
  end

  @impl SinkPipeline
  def handle_message(broadway_message, context) do
    %{consumer: %SinkConsumer{sink: %BenchmarkSink{partition_count: partition_count}}} = context

    # Compute partition from group_id (same as BenchmarkSource)
    partition = Stats.partition(broadway_message.data.group_id, partition_count)

    # Tell Broadway to batch by partition - this ensures all messages in a batch
    # belong to the same partition, which is required for ordered checksum verification
    broadway_message = Broadway.Message.put_batch_key(broadway_message, partition)

    {:ok, broadway_message, context}
  end

  @impl SinkPipeline
  def handle_batch(:default, messages, %{batch_key: partition}, context) do
    %{consumer: consumer} = context
    # Also emit to Prometheus for observability
    total_bytes = messages |> Enum.map(& &1.data.payload_size_bytes) |> Enum.sum()

    if total_bytes > 0 do
      Prometheus.increment_bytes_delivered(consumer.id, consumer.name, total_bytes)
    end

    # Sort messages by (commit_lsn, commit_idx) to ensure correct order for rolling checksum
    sorted_messages = Enum.sort_by(messages, &{&1.data.commit_lsn, &1.data.commit_idx})

    # Record all stats via Stats module
    Enum.each(sorted_messages, fn msg ->
      created_at_us = extract_created_at(msg.data.data.record)

      Stats.message_received_for_group(%Stats.GroupMessage{
        owner_id: consumer.id,
        group_id: msg.data.group_id,
        commit_lsn: msg.data.commit_lsn,
        commit_idx: msg.data.commit_idx,
        partition: partition,
        byte_size: msg.data.payload_size_bytes,
        created_at_us: created_at_us
      })

      # Also emit to Prometheus for observability
      if created_at_us do
        now = :os.system_time(:microsecond)
        Prometheus.observe_benchmark_e2e_latency(consumer.id, now - created_at_us)
      end
    end)

    {:ok, messages, context}
  end

  # Extract created_at timestamp from record (stored as microseconds since epoch)
  defp extract_created_at(record) do
    case Map.get(record, "created_at") do
      created_at when is_integer(created_at) ->
        created_at

      created_at when is_binary(created_at) ->
        case Integer.parse(created_at) do
          {int, ""} -> int
          _ -> nil
        end

      _ ->
        nil
    end
  end
end

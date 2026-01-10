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

  ## ETS Table

  Checksums are stored in a public ETS table named `:benchmark_pipeline_checksums`.

  Keys: `{consumer_id, partition}`
  Values: `{checksum, count}`
  """
  @behaviour Sequin.Runtime.SinkPipeline

  alias Sequin.Consumers.BenchmarkSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Runtime.SinkPipeline

  require Logger

  @ets_table :benchmark_pipeline_checksums

  # ============================================================================
  # SinkPipeline Callbacks
  # ============================================================================

  @impl SinkPipeline
  def init(context, _opts) do
    consumer = context.consumer
    %SinkConsumer{sink: %BenchmarkSink{partition_count: partition_count}} = consumer

    ensure_ets_table()
    init_checksums(consumer.id, partition_count)

    context
  end

  @impl SinkPipeline
  def handle_batch(:default, messages, _batch_info, context) do
    %{consumer: %SinkConsumer{sink: %BenchmarkSink{partition_count: partition_count}} = consumer} = context

    # all messages should belong to same partition
    [{partition, messages}] =
      messages |> Enum.group_by(&:erlang.phash2(&1.data.group_id, partition_count)) |> Enum.to_list()

    Enum.each(messages, fn msg ->
      update_checksum(consumer.id, partition, msg.data.commit_lsn, msg.data.commit_idx)
    end)

    {:ok, messages, context}
  end

  # ============================================================================
  # Public API
  # ============================================================================

  @doc """
  Returns the current checksums for a consumer.

  Returns a map of `%{partition => {checksum, count}}`.
  """
  @spec checksums(String.t()) :: %{non_neg_integer() => {non_neg_integer(), non_neg_integer()}}
  def checksums(consumer_id) do
    ensure_ets_table()

    @ets_table
    |> :ets.match({{consumer_id, :"$1"}, :"$2"})
    |> Map.new(fn [partition, {checksum, count}] ->
      {partition, {checksum, count}}
    end)
  end

  @doc """
  Resets all checksums for a consumer to {0, 0}.
  """
  @spec reset_checksums(String.t()) :: :ok
  def reset_checksums(consumer_id) do
    ensure_ets_table()

    @ets_table
    |> :ets.match({{consumer_id, :"$1"}, :_})
    |> Enum.each(fn [partition] ->
      :ets.insert(@ets_table, {{consumer_id, partition}, {0, 0}})
    end)

    :ok
  end

  @doc """
  Deletes all checksums for a consumer.
  """
  @spec delete_checksums(String.t()) :: :ok
  def delete_checksums(consumer_id) do
    ensure_ets_table()

    @ets_table
    |> :ets.match({{consumer_id, :"$1"}, :_})
    |> Enum.each(fn [partition] ->
      :ets.delete(@ets_table, {consumer_id, partition})
    end)

    :ok
  end

  # ============================================================================
  # Private Functions
  # ============================================================================

  defp ensure_ets_table do
    case :ets.whereis(@ets_table) do
      :undefined ->
        # Create a public table that any process can write to
        # Use :set for O(1) lookups and updates
        :ets.new(@ets_table, [:set, :public, :named_table, {:write_concurrency, true}])

      _tid ->
        :ok
    end
  end

  defp init_checksums(consumer_id, partition_count) do
    Enum.each(0..(partition_count - 1), fn partition ->
      # Use insert_new to avoid overwriting existing checksums on restart
      :ets.insert_new(@ets_table, {{consumer_id, partition}, {0, 0}})
    end)
  end

  defp update_checksum(consumer_id, partition, commit_lsn, commit_idx) do
    key = {consumer_id, partition}

    # Use update_counter for atomic updates
    # Since we need to compute CRC32, we use a match-and-update pattern
    case :ets.lookup(@ets_table, key) do
      [{^key, {prev_checksum, count}}] ->
        new_checksum = :erlang.crc32(<<prev_checksum::32, commit_lsn::64, commit_idx::32>>)
        :ets.insert(@ets_table, {key, {new_checksum, count + 1}})

      [] ->
        # Partition not initialized, start fresh
        new_checksum = :erlang.crc32(<<0::32, commit_lsn::64, commit_idx::32>>)
        :ets.insert(@ets_table, {key, {new_checksum, 1}})
    end
  end
end

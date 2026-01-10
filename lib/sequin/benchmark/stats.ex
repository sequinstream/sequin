defmodule Sequin.Benchmark.Stats do
  @moduledoc """
  Benchmark statistics: rolling checksums, throughput metrics, and optional message tracking.

  Checksums use a chained CRC32:
  `crc32(<<prev_checksum::32, commit_lsn::64, commit_idx::32>>)`

  ETS tables:
  - `:benchmark_checksums` (checksums per owner + partition)
  - `:benchmark_metrics` (bytes delivered, latency sum/count per owner)
  - `:benchmark_tracked_messages` (optional message tracking)

  Tracking is controlled by:

      config :sequin, Sequin.Benchmark.Stats, track_messages: true
  """

  @checksums_table :benchmark_checksums
  @metrics_table :benchmark_metrics
  @tracking_table :benchmark_tracked_messages

  @type scope :: :pipeline | :source

  # ============================================================================
  # Public API
  # ============================================================================

  @doc """
  Computes partition from a group_id using consistent hashing.
  """
  @spec partition(term(), pos_integer()) :: non_neg_integer()
  def partition(group_id, partition_count) do
    :erlang.phash2(group_id, partition_count)
  end

  @doc """
  Initializes checksums and metrics tracking for an owner.
  """
  @spec init_for_owner(term(), pos_integer(), keyword()) :: :ok
  def init_for_owner(owner_id, partition_count, opts \\ []) do
    ensure_tables()
    owner_key = owner_key(owner_id, opts)

    # Initialize checksums for each partition
    Enum.each(0..(partition_count - 1), fn partition ->
      :ets.insert_new(@checksums_table, {{owner_key, partition}, 0, 0})
    end)

    # Initialize metrics
    :ets.insert_new(@metrics_table, {owner_key, 0, 0, 0})

    :ok
  end

  @doc """
  Records a message emitted by the source. Updates checksum only.

  Used by BenchmarkSource to track what was produced.
  """
  @spec message_emitted(term(), non_neg_integer(), integer(), integer(), keyword()) :: :ok
  def message_emitted(owner_id, partition, commit_lsn, commit_idx, opts \\ []) do
    update_checksum(owner_id, partition, commit_lsn, commit_idx, opts)
    :ok
  end

  @doc """
  Records a message received by the pipeline. Updates checksum and metrics.

  Used by the pipeline (BenchmarkPipeline or mock_flush_batch_fn) to track what was consumed.

  ## Options

    * `:byte_size` - Size of the message in bytes (optional)
    * `:created_at_us` - Creation timestamp in microseconds for latency calculation (optional)
    * `:scope` - `:pipeline` (default) or `:source`
  """
  @spec message_received(term(), non_neg_integer(), integer(), integer(), keyword()) :: :ok
  def message_received(owner_id, partition, commit_lsn, commit_idx, opts \\ []) do
    update_checksum(owner_id, partition, commit_lsn, commit_idx, opts)
    update_metrics(owner_id, opts)
    :ok
  end

  @doc """
  Returns the current checksums for an owner.
  """
  @spec checksums(term(), keyword()) :: %{non_neg_integer() => {non_neg_integer(), non_neg_integer()}}
  def checksums(owner_id, opts \\ []) do
    ensure_tables()
    owner_key = owner_key(owner_id, opts)

    @checksums_table
    |> :ets.match({{owner_key, :"$1"}, :"$2", :"$3"})
    |> Map.new(fn [partition, checksum, count] ->
      {partition, {checksum, count}}
    end)
  end

  @doc """
  Returns the current throughput and latency metrics for an owner.
  """
  @spec metrics(term(), keyword()) :: %{
          bytes: non_neg_integer(),
          latency_sum_us: non_neg_integer(),
          latency_count: non_neg_integer()
        }
  def metrics(owner_id, opts \\ []) do
    ensure_tables()
    owner_key = owner_key(owner_id, opts)

    case :ets.lookup(@metrics_table, owner_key) do
      [{^owner_key, bytes, latency_sum, latency_count}] ->
        %{bytes: bytes, latency_sum_us: latency_sum, latency_count: latency_count}

      [] ->
        %{bytes: 0, latency_sum_us: 0, latency_count: 0}
    end
  end

  @doc """
  Returns tracked messages as [{commit_lsn, commit_idx, partition}] in order.
  """
  @spec tracked_messages(term(), keyword()) :: [{integer(), integer(), integer()}]
  def tracked_messages(owner_id, opts \\ []) do
    if track_messages?() do
      ensure_tables()
      owner_key = owner_key(owner_id, opts)

      @tracking_table
      |> :ets.match({{owner_key, :"$1"}, :"$2", :"$3", :"$4"})
      |> Enum.sort_by(fn [seq, _lsn, _idx, _partition] -> seq end)
      |> Enum.map(fn [_seq, lsn, idx, partition] -> {lsn, idx, partition} end)
    else
      []
    end
  end

  @doc """
  Resets all data (checksums, metrics, tracked messages) for an owner.
  """
  @spec reset_for_owner(term(), keyword()) :: :ok
  def reset_for_owner(owner_id, opts \\ []) do
    ensure_tables()
    owner_key = owner_key(owner_id, opts)

    :ets.match_delete(@checksums_table, {{owner_key, :_}, :_, :_})
    :ets.delete(@metrics_table, owner_key)
    :ets.match_delete(@tracking_table, {{owner_key, :_}, :_, :_, :_})

    :ok
  end

  @doc """
  Returns whether message tracking is enabled.
  """
  @spec track_messages?() :: boolean()
  def track_messages? do
    :sequin
    |> Application.get_env(__MODULE__, [])
    |> Keyword.get(:track_messages, false)
  end

  # ============================================================================
  # Private Functions
  # ============================================================================

  defp update_checksum(owner_id, partition, commit_lsn, commit_idx, opts) do
    ensure_tables()
    owner_key = owner_key(owner_id, opts)
    key = {owner_key, partition}

    case :ets.lookup(@checksums_table, key) do
      [{^key, prev_checksum, count}] ->
        new_checksum = :erlang.crc32(<<prev_checksum::32, commit_lsn::64, commit_idx::32>>)
        :ets.insert(@checksums_table, {key, new_checksum, count + 1})
        maybe_track(owner_key, commit_lsn, commit_idx, partition)

      [] ->
        new_checksum = :erlang.crc32(<<0::32, commit_lsn::64, commit_idx::32>>)
        :ets.insert(@checksums_table, {key, new_checksum, 1})
        maybe_track(owner_key, commit_lsn, commit_idx, partition)
    end
  end

  defp update_metrics(owner_id, opts) do
    ensure_tables()
    owner_key = owner_key(owner_id, opts)

    # Record bytes if provided
    if byte_size = Keyword.get(opts, :byte_size) do
      try do
        :ets.update_counter(@metrics_table, owner_key, {2, byte_size})
      rescue
        ArgumentError ->
          :ets.insert(@metrics_table, {owner_key, byte_size, 0, 0})
      end
    end

    # Record latency if created_at provided
    if created_at_us = Keyword.get(opts, :created_at_us) do
      now = :os.system_time(:microsecond)
      latency_us = now - created_at_us

      try do
        :ets.update_counter(@metrics_table, owner_key, [{3, latency_us}, {4, 1}])
      rescue
        ArgumentError ->
          :ets.insert(@metrics_table, {owner_key, 0, latency_us, 1})
      end
    end
  end

  defp maybe_track(owner_key, commit_lsn, commit_idx, partition) do
    if track_messages?() do
      track_key = {owner_key, :erlang.unique_integer([:monotonic, :positive])}
      :ets.insert(@tracking_table, {track_key, commit_lsn, commit_idx, partition})
    end
  end

  defp owner_key(owner_id, opts) do
    scope = Keyword.get(opts, :scope, :pipeline)
    {scope, owner_id}
  end

  defp ensure_tables do
    case :ets.whereis(@checksums_table) do
      :undefined ->
        try do
          :ets.new(@checksums_table, [:set, :public, :named_table, {:write_concurrency, true}])
        rescue
          ArgumentError -> :ok
        end

      _tid ->
        :ok
    end

    case :ets.whereis(@metrics_table) do
      :undefined ->
        try do
          :ets.new(@metrics_table, [:set, :public, :named_table, {:write_concurrency, true}])
        rescue
          ArgumentError -> :ok
        end

      _tid ->
        :ok
    end

    case :ets.whereis(@tracking_table) do
      :undefined ->
        try do
          :ets.new(@tracking_table, [:ordered_set, :public, :named_table, {:write_concurrency, true}])
        rescue
          ArgumentError -> :ok
        end

      _tid ->
        :ok
    end
  end
end

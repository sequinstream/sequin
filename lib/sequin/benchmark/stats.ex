defmodule Sequin.Benchmark.Stats do
  @moduledoc """
  Benchmark statistics: rolling checksums, throughput metrics, and optional message tracking.

  Checksums use a chained CRC32:
  `crc32(<<prev_checksum::32, commit_lsn::64, commit_idx::32>>)`

  ETS tables:
  - `:benchmark_checksums` (checksums per owner + partition)
  - `:benchmark_group_checksums` (checksums per owner + group_id)
  - `:benchmark_metrics` (bytes delivered, latency sum/count per owner)
  - `:benchmark_message_counts` (total messages per owner)
  - `:benchmark_tracked_messages` (optional message tracking)

  Tracking is controlled by:

      config :sequin, Sequin.Benchmark.Stats, track_messages: true

  Per-group checksum sampling is controlled by:

      config :sequin, Sequin.Benchmark.Stats, checksum_sample_rate: 0.1
  """

  alias Sequin.Benchmark.Stats

  @checksums_table :benchmark_checksums
  @group_checksums_table :benchmark_group_checksums
  @metrics_table :benchmark_metrics
  @counts_table :benchmark_message_counts
  @tracking_table :benchmark_tracked_messages
  @default_checksum_sample_rate 0.1

  @type scope :: :pipeline | :source

  # ============================================================================
  # Public API
  # ============================================================================

  defmodule Message do
    @moduledoc false
    defstruct [:owner_id, :partition, :commit_lsn, :commit_idx, :byte_size, :created_at_us, :scope]

    @type t :: %__MODULE__{
            owner_id: term(),
            partition: non_neg_integer(),
            commit_lsn: integer(),
            commit_idx: integer(),
            byte_size: non_neg_integer() | nil,
            created_at_us: integer() | nil,
            scope: Stats.scope() | nil
          }
  end

  defmodule GroupMessage do
    @moduledoc false
    defstruct [:owner_id, :group_id, :commit_lsn, :commit_idx, :partition, :byte_size, :created_at_us, :scope]

    @type t :: %__MODULE__{
            owner_id: term(),
            group_id: term(),
            commit_lsn: integer(),
            commit_idx: integer(),
            partition: non_neg_integer() | nil,
            byte_size: non_neg_integer() | nil,
            created_at_us: integer() | nil,
            scope: Stats.scope() | nil
          }
  end

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
    :ets.insert_new(@counts_table, {owner_key, 0})

    :ok
  end

  @doc """
  Records a message emitted by the source. Updates checksum only.

  Used by BenchmarkSource to track what was produced.
  """
  @spec message_emitted(Message.t()) :: :ok
  def message_emitted(%Message{} = message) do
    update_checksum(message)
    :ok
  end

  @doc """
  Records a message emitted by the source for a specific group_id.
  Updates per-group checksum only (no metrics). Sampling applied internally.
  """
  @spec message_emitted_for_group(GroupMessage.t()) :: :ok
  def message_emitted_for_group(%GroupMessage{group_id: nil}), do: raise("group_id cannot be nil")

  def message_emitted_for_group(%GroupMessage{} = message) do
    if sample_group?(message.group_id), do: update_group_checksum(message)
    :ok
  end

  @doc """
  Records a message received by the pipeline. Updates checksum and metrics.
  """
  @spec message_received(Message.t()) :: :ok
  def message_received(%Message{} = message) do
    update_checksum(message)
    update_count(message)
    update_metrics(message)
    :ok
  end

  @doc """
  Records a message received by the pipeline for a specific group_id.
  Updates per-group checksum (sampled) and metrics. Also tracks for debugging if enabled.
  """
  @spec message_received_for_group(GroupMessage.t()) :: :ok
  def message_received_for_group(%GroupMessage{group_id: nil}), do: raise("group_id cannot be nil")

  def message_received_for_group(%GroupMessage{} = message) do
    if sample_group?(message.group_id), do: update_group_checksum(message)
    update_count(message)
    update_metrics(message)
    owner_key = owner_key(message.owner_id, scope: message.scope)
    maybe_track(owner_key, message)
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
  Returns the current per-group checksums for an owner.
  """
  @spec group_checksums(term(), keyword()) :: %{term() => {non_neg_integer(), non_neg_integer()}}
  def group_checksums(owner_id, opts \\ []) do
    ensure_tables()
    owner_key = owner_key(owner_id, opts)

    @group_checksums_table
    |> :ets.match({{owner_key, :"$1"}, :"$2", :"$3"})
    |> Map.new(fn [group_id, checksum, count] ->
      {group_id, {checksum, count}}
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
  Returns the total messages processed for an owner.
  """
  @spec message_count(term(), keyword()) :: non_neg_integer()
  def message_count(owner_id, opts \\ []) do
    ensure_tables()
    owner_key = owner_key(owner_id, opts)

    case :ets.lookup(@counts_table, owner_key) do
      [{^owner_key, count}] -> count
      [] -> 0
    end
  end

  @doc """
  Returns the configured checksum sample rate for per-group verification.
  """
  @spec checksum_sample_rate() :: float()
  def checksum_sample_rate do
    :sequin
    |> Application.get_env(__MODULE__, [])
    |> Keyword.get(:checksum_sample_rate, @default_checksum_sample_rate)
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
    :ets.match_delete(@group_checksums_table, {{owner_key, :_}, :_, :_})
    :ets.delete(@metrics_table, owner_key)
    :ets.delete(@counts_table, owner_key)
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

  defp update_checksum(%Message{} = msg) do
    ensure_tables()
    owner_key = owner_key(msg.owner_id, scope: msg.scope)
    key = {owner_key, msg.partition}
    new_checksum_data = <<msg.commit_lsn::64, msg.commit_idx::32>>

    case :ets.lookup(@checksums_table, key) do
      [{^key, prev_checksum, count}] ->
        new_checksum = :erlang.crc32(<<prev_checksum::32, new_checksum_data::binary>>)
        :ets.insert(@checksums_table, {key, new_checksum, count + 1})

      [] ->
        new_checksum = :erlang.crc32(<<0::32, new_checksum_data::binary>>)
        :ets.insert(@checksums_table, {key, new_checksum, 1})
    end

    maybe_track(owner_key, msg)
  end

  defp update_group_checksum(%GroupMessage{} = msg) do
    ensure_tables()
    owner_key = owner_key(msg.owner_id, scope: msg.scope)
    key = {owner_key, msg.group_id}
    new_checksum_data = <<msg.commit_lsn::64, msg.commit_idx::32>>

    case :ets.lookup(@group_checksums_table, key) do
      [{^key, prev_checksum, count}] ->
        new_checksum = :erlang.crc32(<<prev_checksum::32, new_checksum_data::binary>>)
        :ets.insert(@group_checksums_table, {key, new_checksum, count + 1})

      [] ->
        new_checksum = :erlang.crc32(<<0::32, new_checksum_data::binary>>)
        :ets.insert(@group_checksums_table, {key, new_checksum, 1})
    end
  end

  defp update_metrics(msg) do
    ensure_tables()
    owner_key = owner_key(msg.owner_id, scope: msg.scope)

    if msg.byte_size do
      try do
        :ets.update_counter(@metrics_table, owner_key, {2, msg.byte_size})
      rescue
        ArgumentError ->
          :ets.insert(@metrics_table, {owner_key, msg.byte_size, 0, 0})
      end
    end

    if msg.created_at_us do
      latency_us = :os.system_time(:microsecond) - msg.created_at_us

      try do
        :ets.update_counter(@metrics_table, owner_key, [{3, latency_us}, {4, 1}])
      rescue
        ArgumentError ->
          :ets.insert(@metrics_table, {owner_key, 0, latency_us, 1})
      end
    end
  end

  defp update_count(msg) do
    ensure_tables()
    owner_key = owner_key(msg.owner_id, scope: msg.scope)

    try do
      :ets.update_counter(@counts_table, owner_key, {2, 1})
    rescue
      ArgumentError ->
        :ets.insert(@counts_table, {owner_key, 1})
    end
  end

  defp maybe_track(_owner_key, %{partition: nil}), do: :ok

  defp maybe_track(owner_key, msg) do
    if track_messages?() do
      track_key = {owner_key, :erlang.unique_integer([:monotonic, :positive])}
      :ets.insert(@tracking_table, {track_key, msg.commit_lsn, msg.commit_idx, msg.partition})
    end
  end

  defp sample_group?(group_id) do
    sample_rate = checksum_sample_rate() |> min(1.0) |> max(0.0)
    threshold = trunc(sample_rate * 1_000_000)
    :erlang.phash2(group_id, 1_000_000) < threshold
  end

  defp owner_key(owner_id, opts) do
    scope = Keyword.get(opts, :scope) || :pipeline
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

    case :ets.whereis(@counts_table) do
      :undefined ->
        try do
          :ets.new(@counts_table, [:set, :public, :named_table, {:write_concurrency, true}])
        rescue
          ArgumentError ->
            :ok
        end

      _ ->
        :ok
    end

    case :ets.whereis(@group_checksums_table) do
      :undefined ->
        try do
          :ets.new(@group_checksums_table, [:set, :public, :named_table, {:write_concurrency, true}])
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

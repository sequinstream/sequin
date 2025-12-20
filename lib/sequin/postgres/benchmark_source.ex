defmodule Sequin.Postgres.BenchmarkSource do
  @moduledoc """
  A virtual WAL source for benchmarking the replication pipeline.

  Generates endless WAL protocol messages on demand with configurable behavior patterns:
  - Variable row sizes (including occasional large rows)
  - Variable transaction sizes
  - PK collision simulation
  - Back-to-back same-PK messages

  ## Usage

      {:ok, pid} = BenchmarkSource.start_link(
        id: some_id,
        row_sizes: [{0.99, 200}, {0.01, 1_000_000}],
        transaction_sizes: [{1.0, 10}],
        pk_collision_rate: 0.1,
        repeat_frequency: 0.05
      )

      # Get checksums for verification
      checksums = BenchmarkSource.checksums(id)
  """

  @behaviour Sequin.Postgres.Source

  use GenServer
  use TypedStruct

  alias Sequin.CircularBuffer
  alias Sequin.Postgres.Source

  require Logger

  # Postgres epoch: 2000-01-01 00:00:00 UTC
  @pg_epoch 946_684_800_000_000

  # Hardcoded tables - 3 tables with partition column
  @tables [
    %{oid: 16_384, name: "benchmark_events_1", schema: "public"},
    %{oid: 16_385, name: "benchmark_events_2", schema: "public"},
    %{oid: 16_386, name: "benchmark_events_3", schema: "public"}
  ]

  @columns [
    %{name: "id", type: :bigint, position: 1},
    %{name: "partition_key", type: :text, position: 2},
    %{name: "payload", type: :bytea, position: 3},
    %{name: "created_at", type: :timestamptz, position: 4}
  ]

  @num_columns length(@columns)

  defmodule Config do
    @moduledoc "Configuration for BenchmarkSource"
    use TypedStruct

    typedstruct do
      # [{fraction, size_bytes}] - distribution of payload sizes
      # e.g., [{0.99, 200}, {0.01, 1_000_000}] = 99% small, 1% 1MB
      field :row_sizes, [{float(), pos_integer()}], default: [{1.0, 200}]

      # [{fraction, count}] - messages per transaction
      # e.g., [{0.9, 10}, {0.1, 100}] = 90% have 10 msgs, 10% have 100
      field :transaction_sizes, [{float(), pos_integer()}], default: [{1.0, 10}]

      # 0.0-1.0 - probability of reusing a PK from recent pool
      field :pk_collision_rate, float(), default: 0.005

      # 0.0-1.0 - probability of emitting back-to-back same-PK messages
      field :repeat_frequency, float(), default: 0.01

      # Number of partitions for checksum tracking (matches consumer partitions)
      field :partition_count, pos_integer(), default: System.schedulers_online()

      # Size of circular buffer for recent PKs (for collision simulation)
      field :pk_pool_size, pos_integer(), default: 100_000
    end
  end

  defmodule State do
    @moduledoc false
    use TypedStruct

    typedstruct do
      # Unique identifier for this instance
      field :id, term(), enforce: true

      # The producer to send :tcp messages to
      field :producer, pid()

      field :config, Config.t(), enforce: true

      # Current LSN (monotonically increasing)
      field :current_lsn, pos_integer(), default: 1

      # Current transaction state
      field :current_xid, pos_integer(), default: 1
      field :transaction_commit_lsn, pos_integer()
      field :transaction_timestamp, pos_integer()
      field :transaction_messages_remaining, non_neg_integer(), default: 0
      field :in_transaction, boolean(), default: false

      # Checksums per partition: %{partition => {checksum, count}}
      field :checksums, %{non_neg_integer() => {non_neg_integer(), non_neg_integer()}}, enforce: true

      # Circular buffer of recent PKs for collision simulation
      field :recent_pks, CircularBuffer.t(), enforce: true

      # Stats
      field :total_messages, non_neg_integer(), default: 0
      field :total_bytes, non_neg_integer(), default: 0
      field :total_transactions, non_neg_integer(), default: 0
    end
  end

  # ============================================================================
  # Public API
  # ============================================================================

  def via_tuple(id) do
    {:via, :syn, {:replication, {__MODULE__, id}}}
  end

  @doc """
  Starts the BenchmarkSource server.

  ## Options

  - `:id` - Required. Unique identifier for this instance.

  See `Sequin.Postgres.BenchmarkSource.Config` for other available options.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    id = Keyword.fetch!(opts, :id)
    GenServer.start_link(__MODULE__, opts, name: via_tuple(id))
  end

  @doc """
  Registers a producer to receive :tcp messages from this source.

  Once set, will immediately send the first :tcp message to kick off
  the replication flow.
  """
  @impl Source
  def set_producer(id, producer) do
    GenServer.call(via_tuple(id), {:set_producer, producer})
  end

  @doc """
  Receives up to `max_count` WAL copy messages.

  Returns a list of binary messages ready to be processed by SlotProducer.
  Note: The actual count may exceed max_count slightly to complete transactions.

  After returning, will send another :tcp message to the producer
  to indicate more data is available.
  """
  @impl Source
  def recv_copies(id, max_count) do
    GenServer.call(via_tuple(id), {:recv_copies, max_count})
  end

  @doc """
  Handles an ack message from the replication protocol.

  Parses the LSN from the ack and tracks it.
  """
  @impl Source
  def handle_ack(id, ack_msg) do
    GenServer.cast(via_tuple(id), {:handle_ack, ack_msg})
    :ok
  end

  @doc """
  Returns the current checksums per partition.

  Format: %{partition => {checksum, count}}
  """
  @spec checksums(term()) :: %{non_neg_integer() => {non_neg_integer(), non_neg_integer()}}
  def checksums(id) do
    GenServer.call(via_tuple(id), :checksums)
  end

  @doc """
  Returns statistics about messages generated.
  """
  @spec stats(term()) :: map()
  def stats(id) do
    GenServer.call(via_tuple(id), :stats)
  end

  @doc """
  Returns the table definitions (for Relation messages).
  """
  @spec tables() :: [map()]
  def tables, do: @tables

  @doc """
  Returns the column definitions.
  """
  @spec columns() :: [map()]
  def columns, do: @columns

  # ============================================================================
  # GenServer Callbacks
  # ============================================================================

  @impl GenServer
  def init(opts) do
    id = Keyword.fetch!(opts, :id)
    config_opts = Keyword.delete(opts, :id)
    config = struct!(Config, config_opts)

    checksums =
      Map.new(0..(config.partition_count - 1), fn p -> {p, {0, 0}} end)

    state = %State{
      id: id,
      config: config,
      checksums: checksums,
      recent_pks: CircularBuffer.new(config.pk_pool_size)
    }

    {:ok, state}
  end

  @impl GenServer
  def handle_call({:set_producer, producer}, _from, state) do
    {:reply, :ok, %{state | producer: producer}, {:continue, :send_next_tcp}}
  end

  def handle_call({:recv_copies, max_count}, _from, state) do
    {copies, state} = generate_copies(state, max_count, [])
    {:reply, copies, state, {:continue, :send_next_tcp}}
  end

  def handle_call(:checksums, _from, state) do
    {:reply, state.checksums, state}
  end

  def handle_call(:stats, _from, state) do
    stats = %{
      total_messages: state.total_messages,
      total_bytes: state.total_bytes,
      total_transactions: state.total_transactions,
      current_lsn: state.current_lsn
    }

    {:reply, stats, state}
  end

  @impl GenServer
  def handle_cast({:handle_ack, _ack_msg}, state) do
    # TODO: Parse and track acked LSN
    {:noreply, state}
  end

  @impl GenServer
  def handle_continue(:send_next_tcp, %State{producer: nil} = state) do
    # No producer registered yet, nothing to do
    {:noreply, state}
  end

  def handle_continue(:send_next_tcp, %State{producer: producer} = state) do
    send(producer, {:tcp, :benchmark_source, :data_ready})
    {:noreply, state}
  end

  # ============================================================================
  # Message Generation
  # ============================================================================

  defp generate_copies(state, 0, acc) do
    {Enum.reverse(acc), state}
  end

  defp generate_copies(state, remaining, acc) do
    {copies, state} = generate_copy(state)
    # copies is a list (may include BEGIN, UPDATE, COMMIT)
    generate_copies(state, remaining - 1, Enum.reverse(copies, acc))
  end

  defp generate_copy(state) do
    {begin_copy, state} = maybe_start_transaction(state)
    {update_copy, state} = generate_change_message(state)
    {commit_copy, state} = maybe_end_transaction(state)

    copies = Enum.reject([begin_copy, update_copy, commit_copy], &is_nil/1)

    {copies, state}
  end

  defp maybe_start_transaction(%State{in_transaction: false} = state) do
    txn_size = pick_from_distribution(state.config.transaction_sizes)

    # The commit LSN will be after all the change messages
    commit_lsn = state.current_lsn + txn_size

    # Capture timestamp once for the entire transaction (BEGIN and COMMIT must match)
    timestamp = pg_timestamp()

    state = %{
      state
      | in_transaction: true,
        transaction_messages_remaining: txn_size,
        transaction_commit_lsn: commit_lsn,
        transaction_timestamp: timestamp,
        current_xid: state.current_xid + 1
    }

    begin_copy = encode_begin(commit_lsn, state.current_xid, timestamp)
    {begin_copy, state}
  end

  defp maybe_start_transaction(state), do: {nil, state}

  defp maybe_end_transaction(%State{transaction_messages_remaining: 1} = state) do
    commit_copy = encode_commit(state.transaction_commit_lsn, state.transaction_timestamp)

    state = %{
      state
      | in_transaction: false,
        transaction_messages_remaining: 0,
        transaction_commit_lsn: nil,
        transaction_timestamp: nil,
        total_transactions: state.total_transactions + 1
    }

    {commit_copy, state}
  end

  defp maybe_end_transaction(%State{transaction_messages_remaining: n} = state) when n > 1 do
    {nil, %{state | transaction_messages_remaining: n - 1}}
  end

  defp generate_change_message(state) do
    # Pick table (round-robin for now)
    table = Enum.at(@tables, rem(state.total_messages, length(@tables)))

    # Generate or pick PK
    {pk, partition_key, state} = generate_pk(state)

    # Pick row size
    row_size = pick_from_distribution(state.config.row_sizes)

    # Maybe repeat (back-to-back same PK)
    {pk, partition_key, state} = maybe_repeat_pk(state, pk, partition_key)

    # Compute partition for checksum
    partition = :erlang.phash2(partition_key, state.config.partition_count)

    # Compute commit_idx (0-based index within transaction)
    txn_size = pick_from_distribution(state.config.transaction_sizes)
    commit_idx = txn_size - state.transaction_messages_remaining

    # Update checksum for this partition
    state = update_checksum(state, partition, state.transaction_commit_lsn, commit_idx)

    # Generate payload
    payload = :binary.copy(<<0>>, row_size)
    timestamp = format_timestamp()

    # Encode the WAL UPDATE message
    copy = encode_update(table.oid, pk, partition_key, payload, timestamp, state.current_lsn)

    state = %{
      state
      | current_lsn: state.current_lsn + 1,
        total_messages: state.total_messages + 1,
        total_bytes: state.total_bytes + byte_size(copy),
        recent_pks: CircularBuffer.insert(state.recent_pks, {pk, partition_key})
    }

    {copy, state}
  end

  defp generate_pk(state) do
    if should_collide?(state) and not CircularBuffer.empty?(state.recent_pks) do
      # Pick a random PK from the recent pool
      recent_list = CircularBuffer.to_list(state.recent_pks)
      {pk, partition_key} = Enum.random(recent_list)
      {pk, partition_key, state}
    else
      # Generate new PK using fast unique integer
      pk = System.unique_integer([:positive])
      partition_key = "partition_#{rem(pk, 1000)}"
      {pk, partition_key, state}
    end
  end

  defp maybe_repeat_pk(state, pk, partition_key) do
    if should_repeat?(state) do
      # Use same PK again (will generate another message with same PK)
      {pk, partition_key, state}
    else
      {pk, partition_key, state}
    end
  end

  defp should_collide?(state) do
    state.config.pk_collision_rate > 0 and :rand.uniform() < state.config.pk_collision_rate
  end

  defp should_repeat?(state) do
    state.config.repeat_frequency > 0 and :rand.uniform() < state.config.repeat_frequency
  end

  defp update_checksum(state, partition, lsn, commit_idx) do
    {prev_checksum, count} = Map.fetch!(state.checksums, partition)
    new_checksum = :erlang.crc32(<<prev_checksum::32, lsn::64, commit_idx::32>>)

    %{state | checksums: Map.put(state.checksums, partition, {new_checksum, count + 1})}
  end

  defp pick_from_distribution([{_fraction, value}]) do
    value
  end

  defp pick_from_distribution(distribution) do
    random = :rand.uniform()

    distribution
    |> Enum.reduce_while({0.0, nil}, fn {fraction, value}, {cumulative, _} ->
      new_cumulative = cumulative + fraction

      if random <= new_cumulative do
        {:halt, {:found, value}}
      else
        {:cont, {new_cumulative, value}}
      end
    end)
    |> case do
      {:found, value} -> value
      {_, value} -> value
    end
  end

  # ============================================================================
  # WAL Protocol Encoding
  # ============================================================================

  # Wrap a message in the copy data format
  # Format: ?w + wal_start(64) + wal_end(64) + send_time(64) + message
  defp wrap_copy(msg, lsn) do
    send_time = pg_timestamp()
    <<?w, lsn::64, lsn::64, send_time::64, msg::binary>>
  end

  # BEGIN message
  # Format: ?B + final_lsn(64) + timestamp(64) + xid(32)
  defp encode_begin(commit_lsn, xid, timestamp) do
    lsn_binary = <<0::32, commit_lsn::32>>
    msg = <<"B", lsn_binary::binary, timestamp::64, xid::32>>
    wrap_copy(msg, commit_lsn)
  end

  # COMMIT message
  # Format: ?C + flags(8) + lsn(64) + end_lsn(64) + timestamp(64)
  defp encode_commit(commit_lsn, timestamp) do
    lsn_binary = <<0::32, commit_lsn::32>>
    msg = <<"C", 0::8, lsn_binary::binary, lsn_binary::binary, timestamp::64>>
    wrap_copy(msg, commit_lsn)
  end

  # UPDATE message (new tuple only, no old tuple)
  # Format: ?U + relation_id(32) + ?N + num_columns(16) + tuple_data
  defp encode_update(relation_id, pk, partition_key, payload, timestamp, lsn) do
    tuple_data = encode_tuple_data(pk, partition_key, payload, timestamp)
    msg = <<"U", relation_id::32, "N", @num_columns::16, tuple_data::binary>>
    wrap_copy(msg, lsn)
  end

  # Encode tuple data for our 4 columns: id, partition_key, payload, created_at
  # Each column: ?t + length(32) + value
  defp encode_tuple_data(pk, partition_key, payload, timestamp) do
    # Column 1: id (bigint as text)
    id_text = Integer.to_string(pk)
    id_col = <<"t", byte_size(id_text)::32, id_text::binary>>

    # Column 2: partition_key (text)
    pk_col = <<"t", byte_size(partition_key)::32, partition_key::binary>>

    # Column 3: payload (bytea as hex-escaped text, but we'll use raw for simplicity)
    # In real PG, bytea is sent as hex-escaped, but the decoder handles raw bytes too
    payload_col = <<"t", byte_size(payload)::32, payload::binary>>

    # Column 4: created_at (timestamptz as text)
    ts_col = <<"t", byte_size(timestamp)::32, timestamp::binary>>

    <<id_col::binary, pk_col::binary, payload_col::binary, ts_col::binary>>
  end

  # Get current time as Postgres timestamp (microseconds since 2000-01-01)
  defp pg_timestamp do
    System.os_time(:microsecond) - @pg_epoch
  end

  # Format timestamp as ISO8601 string
  defp format_timestamp do
    DateTime.to_iso8601(DateTime.utc_now())
  end
end

defmodule Sequin.Runtime.SlotMessageStore.State do
  @moduledoc false
  use TypedStruct

  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.DebouncedLogger
  alias Sequin.Error
  alias Sequin.Multiset
  alias Sequin.Replication
  alias Sequin.Runtime.SlotMessageStore.State

  require Logger

  @type message :: ConsumerEvent.t()
  @type cursor_tuple :: {commit_lsn :: non_neg_integer(), commit_idx :: non_neg_integer()}

  @default_setting_max_messages 50_000

  typedstruct do
    field :consumer, SinkConsumer.t()
    field :consumer_id, String.t()
    field :last_consumer_producer_pid, pid() | nil
    field :messages, %{cursor_tuple() => message()}, default: %{}
    field :ack_ids_to_cursor_tuples, %{SinkConsumer.ack_id() => cursor_tuple()}, default: %{}
    field :partition, non_neg_integer()
    field :produced_message_groups, Multiset.t(), default: %{}
    field :persisted_message_groups, Multiset.t(), default: %{}
    field :backfill_message_groups, Multiset.t(), default: %{}
    field :unpersisted_cursor_tuples_for_table_reader_batches, Multiset.t(), default: %{}
    field :setting_max_messages, non_neg_integer(), default: @default_setting_max_messages
    field :setting_system_max_memory_bytes, non_neg_integer() | nil
    field :setting_max_memory_check_interval, non_neg_integer(), default: to_timeout(minute: 5)
    field :max_memory_bytes, non_neg_integer()
    field :payload_size_bytes, non_neg_integer(), default: 0
    field :slot_processor_monitor_ref, reference() | nil
    field :table_reader_batch_id, String.t() | nil
    field :test_pid, pid() | nil
    field :last_logged_stats_at, non_neg_integer() | nil
    field :flush_interval, non_neg_integer()
    field :message_age_before_flush_ms, non_neg_integer()

    # Rescue messages stuck in produced state
    field :visibility_check_interval, non_neg_integer()
    field :max_time_since_delivered_ms, non_neg_integer()

    field :high_watermark_wal_cursor, {non_neg_integer(), Replication.wal_cursor()} | nil
  end

  @spec setup_ets(State.t()) :: :ok
  def setup_ets(%State{} = state) do
    cdc_table_name = ordered_cdc_cursors_table(state)
    backfill_table_name = ordered_backfill_cursors_table(state)

    :ets.new(cdc_table_name, [:ordered_set, :named_table, :protected])
    :ets.new(backfill_table_name, [:ordered_set, :named_table, :protected])

    :ok
  end

  @spec ordered_cdc_cursors_table(State.t()) :: atom()
  defp ordered_cdc_cursors_table(%State{} = state) do
    :"slot_message_store_state_ordered_cursors_consumer_#{state.consumer.seq}_partition_#{state.partition}"
  end

  @spec ordered_backfill_cursors_table(State.t()) :: atom()
  defp ordered_backfill_cursors_table(%State{} = state) do
    :"slot_message_store_state_ordered_backfill_cursors_consumer_#{state.consumer.seq}_partition_#{state.partition}"
  end

  @spec validate_put_messages(State.t(), list(message()) | Enumerable.t(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, Error.t()}
  def validate_put_messages(%State{} = state, messages, opts \\ []) do
    skip_limit_check? = Keyword.get(opts, :skip_limit_check?, false)

    incoming_payload_size_bytes = Enum.sum_by(messages, & &1.payload_size_bytes)

    bytes_exceeded? =
      state.payload_size_bytes + incoming_payload_size_bytes > state.max_memory_bytes

    messages_exceeded? = length(messages) + map_size(state.messages) > state.setting_max_messages

    cond do
      not skip_limit_check? and bytes_exceeded? ->
        {:error, Error.invariant(message: "Payload size limit exceeded", code: :payload_size_limit_exceeded)}

      not skip_limit_check? and messages_exceeded? ->
        {:error, Error.invariant(message: "Message count limit exceeded", code: :payload_size_limit_exceeded)}

      true ->
        {:ok, incoming_payload_size_bytes}
    end
  end

  @spec put_messages(State.t(), list(message()) | Enumerable.t(), keyword()) ::
          {:ok, State.t()} | {:error, Error.t()}
  def put_messages(%State{} = state, messages, opts \\ []) do
    messages = Enum.reject(messages, &message_exists?(state, &1))

    with {:ok, incoming_payload_size_bytes} <- validate_put_messages(state, messages, opts) do
      # Insert into ETS
      {cdc_messages, backfill_messages} = Enum.split_with(messages, &is_nil(&1.table_reader_batch_id))
      cdc_ets_keys = Enum.map(cdc_messages, fn msg -> {{msg.commit_lsn, msg.commit_idx}} end)
      backfill_ets_keys = Enum.map(backfill_messages, fn msg -> {{msg.commit_lsn, msg.commit_idx}} end)

      state
      |> ordered_cdc_cursors_table()
      |> :ets.insert(cdc_ets_keys)

      state
      |> ordered_backfill_cursors_table()
      |> :ets.insert(backfill_ets_keys)

      cursor_tuples_to_messages = Map.new(messages, fn msg -> {{msg.commit_lsn, msg.commit_idx}, msg} end)
      ack_ids_to_cursor_tuples = Map.new(messages, fn msg -> {msg.ack_id, {msg.commit_lsn, msg.commit_idx}} end)

      {:ok,
       %{
         state
         | messages: Map.merge(state.messages, cursor_tuples_to_messages),
           ack_ids_to_cursor_tuples: Map.merge(state.ack_ids_to_cursor_tuples, ack_ids_to_cursor_tuples),
           payload_size_bytes: state.payload_size_bytes + incoming_payload_size_bytes
       }}
    end
  end

  @spec message_exists?(State.t(), message()) :: boolean()
  def message_exists?(%State{} = state, message) do
    Map.has_key?(state.messages, {message.commit_lsn, message.commit_idx})
  end

  @spec put_persisted_messages(State.t(), list(message()) | Enumerable.t()) :: State.t()
  def put_persisted_messages(%State{} = state, messages) do
    persisted_message_groups =
      Enum.reduce(messages, state.persisted_message_groups, fn msg, acc ->
        Multiset.put(acc, group_id(msg), {msg.commit_lsn, msg.commit_idx})
      end)

    # This cannot fail because we `skip_limit_check?`
    {:ok, state} =
      put_messages(%{state | persisted_message_groups: persisted_message_groups}, messages, skip_limit_check?: true)

    state
  end

  @spec put_table_reader_batch(State.t(), list(message()), String.t()) ::
          {:ok, State.t()} | {:error, Error.t()}
  def put_table_reader_batch(%State{} = state, messages, batch_id) do
    messages = Enum.map(messages, &%{&1 | table_reader_batch_id: batch_id})
    cursor_tuples = Enum.map(messages, &{&1.commit_lsn, &1.commit_idx})

    unpersisted_cursor_tuples_for_table_reader_batches =
      Multiset.union(state.unpersisted_cursor_tuples_for_table_reader_batches, batch_id, MapSet.new(cursor_tuples))

    backfill_message_groups =
      Enum.reduce(messages, state.backfill_message_groups, fn msg, acc ->
        Multiset.put(acc, group_id(msg), {msg.commit_lsn, msg.commit_idx})
      end)

    case put_messages(state, messages, skip_limit_check?: true) do
      {:ok, %State{} = state} ->
        {:ok,
         %{
           state
           | table_reader_batch_id: batch_id,
             backfill_message_groups: backfill_message_groups,
             unpersisted_cursor_tuples_for_table_reader_batches: unpersisted_cursor_tuples_for_table_reader_batches
         }}

      error ->
        error
    end
  end

  @spec pop_messages(State.t(), list(cursor_tuple())) :: {list(message()), State.t()}
  def pop_messages(%State{} = state, cursor_tuples) do
    popped_messages = state.messages |> Map.take(cursor_tuples) |> Map.values()
    messages = Map.drop(state.messages, cursor_tuples)

    # Remove from ETS
    cdc_table = ordered_cdc_cursors_table(state)
    backfill_table = ordered_backfill_cursors_table(state)

    Enum.each(popped_messages, fn msg ->
      if is_nil(msg.table_reader_batch_id) do
        :ets.delete(cdc_table, {msg.commit_lsn, msg.commit_idx})
      else
        :ets.delete(backfill_table, {msg.commit_lsn, msg.commit_idx})
      end
    end)

    # Remove from backfill_message_groups
    backfill_message_groups =
      Enum.reduce(popped_messages, state.backfill_message_groups, fn msg, acc ->
        if is_nil(msg.table_reader_batch_id) do
          acc
        else
          Multiset.delete(acc, group_id(msg), {msg.commit_lsn, msg.commit_idx})
        end
      end)

    persisted_message_groups =
      Enum.reduce(popped_messages, state.persisted_message_groups, fn msg, acc ->
        Multiset.delete(acc, group_id(msg), {msg.commit_lsn, msg.commit_idx})
      end)

    produced_message_groups =
      Enum.reduce(popped_messages, state.produced_message_groups, fn msg, acc ->
        Multiset.delete(acc, group_id(msg), {msg.commit_lsn, msg.commit_idx})
      end)

    cursor_tuples_set = MapSet.new(cursor_tuples)

    unpersisted_cursor_tuples_for_table_reader_batches =
      state.unpersisted_cursor_tuples_for_table_reader_batches
      |> Multiset.keys()
      |> Enum.reduce(
        state.unpersisted_cursor_tuples_for_table_reader_batches,
        fn batch_id, acc ->
          Multiset.difference(acc, batch_id, cursor_tuples_set)
        end
      )

    popped_message_ack_ids = Enum.map(popped_messages, & &1.ack_id)

    next_payload_size_bytes = state.payload_size_bytes - Enum.sum_by(popped_messages, & &1.payload_size_bytes)

    next_payload_size_bytes =
      if map_size(messages) == 0 and next_payload_size_bytes > 0 do
        Logger.error("Popped messages bytes is greater than 0 when there are no messages in the state")
        0
      else
        next_payload_size_bytes
      end

    {popped_messages,
     %{
       state
       | messages: messages,
         ack_ids_to_cursor_tuples: Map.drop(state.ack_ids_to_cursor_tuples, popped_message_ack_ids),
         payload_size_bytes: next_payload_size_bytes,
         produced_message_groups: produced_message_groups,
         persisted_message_groups: persisted_message_groups,
         backfill_message_groups: backfill_message_groups,
         unpersisted_cursor_tuples_for_table_reader_batches: unpersisted_cursor_tuples_for_table_reader_batches
     }}
  end

  @spec pop_all_messages(State.t()) :: {list(message()), State.t()}
  def pop_all_messages(%State{} = state) do
    pop_messages(state, Map.keys(state.messages))
  end

  @spec message_group_persisted?(State.t(), non_neg_integer(), String.t()) :: boolean()
  # Messages without group_ids do not belong to any group
  def message_group_persisted?(%State{}, _table_oid, nil) do
    false
  end

  def message_group_persisted?(%State{} = state, table_oid, group_id) do
    Multiset.member?(state.persisted_message_groups, {table_oid, group_id})
  end

  @spec message_persisted?(State.t(), message()) :: boolean()
  def message_persisted?(%State{} = state, msg) do
    Multiset.value_member?(
      state.persisted_message_groups,
      group_id(msg),
      {msg.commit_lsn, msg.commit_idx}
    )
  end

  @doc """
  Clears all messages from produced_message_groups.

  Called whenever the ConsumerProducer changes (and therefore is presumed to have crashed.)
  """
  @spec nack_produced_messages(State.t()) :: {State.t(), non_neg_integer()}
  def nack_produced_messages(%State{} = state) do
    nacked_count = state.produced_message_groups |> Multiset.values() |> length()

    {%{state | produced_message_groups: Multiset.new()}, nacked_count}
  end

  @doc """
  Removes messages from produced_messagees that were delivered before ack_wait_ms ago.
  """
  @spec nack_stale_produced_messages(State.t()) :: State.t()
  def nack_stale_produced_messages(%State{} = state) do
    ack_wait_ms_ago = DateTime.add(Sequin.utc_now(), -state.consumer.ack_wait_ms, :millisecond)

    stale_cursor_tuples =
      state.messages
      |> Stream.filter(fn {cursor_tuple, msg} ->
        Multiset.value_member?(state.produced_message_groups, group_id(msg), cursor_tuple) and
          not is_nil(msg.last_delivered_at) and
          DateTime.before?(msg.last_delivered_at, ack_wait_ms_ago)
      end)
      |> Stream.map(fn {cursor_tuple, msg} -> {msg.table_oid, msg.group_id, cursor_tuple} end)
      |> Enum.to_list()

    produced_message_groups =
      Enum.reduce(stale_cursor_tuples, state.produced_message_groups, fn {table_oid, group_id, cursor_tuple}, acc ->
        Multiset.delete(acc, {table_oid, group_id}, cursor_tuple)
      end)

    %{state | produced_message_groups: produced_message_groups}
  end

  @spec reset_message_visibilities(State.t(), list(SinkConsumer.ack_id())) :: {State.t(), list(message())}
  def reset_message_visibilities(%State{} = state, ack_ids) do
    cursor_tuples = state.ack_ids_to_cursor_tuples |> Map.take(ack_ids) |> Map.values()
    do_reset_message_visibilities(state, cursor_tuples, [])
  end

  @spec reset_all_message_visibilities(State.t()) :: State.t()
  def reset_all_message_visibilities(%State{} = state) do
    cursor_tuples = Map.keys(state.messages)
    do_reset_all_message_visibilities(state, cursor_tuples)
  end

  defp do_reset_message_visibilities(%State{} = state, [], accumulated_messages) do
    {state, accumulated_messages}
  end

  defp do_reset_message_visibilities(%State{} = state, [cursor_tuple | cursor_tuples], accumulated_messages) do
    case Map.get(state.messages, cursor_tuple) do
      nil ->
        do_reset_message_visibilities(state, cursor_tuples, accumulated_messages)

      msg ->
        {state, reset_msg} = do_reset_message_visibility(state, msg)
        do_reset_message_visibilities(state, cursor_tuples, [reset_msg | accumulated_messages])
    end
  end

  defp do_reset_all_message_visibilities(%State{} = state, []), do: state

  defp do_reset_all_message_visibilities(%State{} = state, [cursor_tuple | cursor_tuples]) do
    case Map.get(state.messages, cursor_tuple) do
      nil ->
        do_reset_all_message_visibilities(state, cursor_tuples)

      msg ->
        {state, _} = do_reset_message_visibility(state, msg)
        do_reset_all_message_visibilities(state, cursor_tuples)
    end
  end

  defp do_reset_message_visibility(%State{} = state, msg) do
    msg = %{msg | not_visible_until: nil}
    cursor_tuple = {msg.commit_lsn, msg.commit_idx}

    state =
      %{
        state
        | messages: Map.put(state.messages, cursor_tuple, msg),
          produced_message_groups: Multiset.delete(state.produced_message_groups, group_id(msg), cursor_tuple)
      }

    {state, msg}
  end

  @spec min_unpersisted_wal_cursor(State.t()) :: Replication.wal_cursor() | nil
  def min_unpersisted_wal_cursor(%State{} = state) do
    persisted_message_cursor_tuples = state.persisted_message_groups |> Multiset.values() |> MapSet.new()

    min_messages_wal_cursor =
      state.messages
      |> Stream.reject(fn {cursor_tuple, _msg} ->
        MapSet.member?(persisted_message_cursor_tuples, cursor_tuple)
      end)
      |> Stream.map(fn {_k, msg} -> {msg.commit_lsn, msg.commit_idx} end)
      |> Enum.min(fn -> nil end)

    case {min_messages_wal_cursor, state.high_watermark_wal_cursor} do
      # No unpersisted messages and no high watermark cursor
      {nil, nil} ->
        nil

      # No unpersisted messages, use high watermark cursor
      {nil, {_batch_idx, high_watermark_cursor}} ->
        high_watermark_cursor

      # Has unpersisted messages regardless of watermark cursor, be conservative
      {{commit_lsn, commit_idx}, _} ->
        %{commit_lsn: commit_lsn, commit_idx: commit_idx}
    end
  end

  @spec ack_ids_to_cursor_tuples(State.t(), list(SinkConsumer.ack_id())) :: list(cursor_tuple())
  def ack_ids_to_cursor_tuples(%State{} = state, ack_ids) do
    ack_ids
    |> Stream.map(&Map.get(state.ack_ids_to_cursor_tuples, &1))
    |> Enum.filter(& &1)
  end

  @spec produce_messages(State.t(), non_neg_integer()) :: {list(message()), State.t()}
  def produce_messages(%State{} = state, _count) when state.consumer.status != :active do
    {[], state}
  end

  def produce_messages(%State{} = state, count) do
    now = Sequin.utc_now()

    messages =
      state
      |> deliverable_messages(count)
      |> Enum.map(fn msg ->
        %{msg | last_delivered_at: now}
      end)

    produced_message_groups =
      Enum.reduce(messages, state.produced_message_groups, fn msg, acc ->
        Multiset.put(acc, group_id(msg), {msg.commit_lsn, msg.commit_idx})
      end)

    {messages,
     %{
       state
       | produced_message_groups: produced_message_groups,
         # Replace messages in state to set last_delivered_at
         messages: Map.merge(state.messages, Map.new(messages, &{{&1.commit_lsn, &1.commit_idx}, &1}))
     }}
  end

  @spec unpersisted_table_reader_batch_ids(State.t()) :: list(String.t())
  def unpersisted_table_reader_batch_ids(%State{} = state) do
    Multiset.keys(state.unpersisted_cursor_tuples_for_table_reader_batches)
  end

  defp deliverable_messages(%State{} = state, count) do
    now = Sequin.utc_now()

    {_, _, deliverable_messages} =
      state
      |> sorted_message_stream()
      |> Enum.reduce_while({0, MapSet.new(), []}, fn
        _msg, {acc_count, _, deliverable_messages} when acc_count >= count ->
          {:halt, {acc_count, nil, deliverable_messages}}

        msg, {acc_count, delivered_message_group_ids, deliverable_messages} ->
          deliverable? =
            cond do
              # Messages without group_id are delivered independently
              not is_nil(msg.group_id) and Multiset.member?(state.produced_message_groups, group_id(msg)) ->
                false

              is_nil(msg.group_id) and
                  Multiset.value_member?(
                    state.produced_message_groups,
                    group_id(msg),
                    {msg.commit_lsn, msg.commit_idx}
                  ) ->
                false

              not is_nil(msg.group_id) and MapSet.member?(delivered_message_group_ids, group_id(msg)) ->
                false

              is_nil(msg.not_visible_until) ->
                true

              DateTime.before?(msg.not_visible_until, now) ->
                true

              true ->
                false
            end

          if deliverable? do
            {:cont, {acc_count + 1, MapSet.put(delivered_message_group_ids, group_id(msg)), [msg | deliverable_messages]}}
          else
            {:cont, {acc_count, delivered_message_group_ids, deliverable_messages}}
          end
      end)

    Enum.reverse(deliverable_messages)
  end

  def peek_messages(%State{} = state, count) when is_integer(count) do
    state
    |> sorted_message_stream()
    |> Enum.take(count)
  end

  def peek_messages(%State{} = state, cursor_tuples) when is_list(cursor_tuples) do
    state.messages
    |> Map.take(cursor_tuples)
    |> Map.values()
  end

  @doc """
  Returns messages with metadata only (data field set to nil).
  This is more memory efficient for listing messages in the UI.

  Options:
    - `order`: `:asc` (oldest first, default) or `:desc` (newest first)
  """
  def peek_messages_metadata(%State{} = state, count, opts \\ []) when is_integer(count) do
    order = Keyword.get(opts, :order, :asc)

    state
    |> sorted_message_stream_for_display(order)
    |> Enum.take(count)
    |> Enum.map(&drop_message_data/1)
  end

  def peek_message(%State{} = state, ack_id) do
    case Map.get(state.ack_ids_to_cursor_tuples, ack_id) do
      nil -> {:error, Error.not_found(entity: :message, params: %{ack_id: ack_id})}
      cursor_tuple -> {:ok, Map.get(state.messages, cursor_tuple)}
    end
  end

  defp drop_message_data(%ConsumerEvent{} = message) do
    %{message | data: nil}
  end

  # This function provides an optimized way to zipper merge two ordered ets sets: cdc and backfill cursor tuples.
  #
  # We prioritize CDC messages over backfill messages to ensure low latency CDC during backfills.
  #
  # We immediately switch to strictly ordered mode if any cdc messages have a conflict with backfill groups.
  # This is a simplified version of prioritization. We *could* prioritize CDC messages across groups
  # while still ensuring strict cursor tuple ordering within groups.
  defp sorted_message_stream(%State{} = state) do
    cdc_table = ordered_cdc_cursors_table(state)
    backfill_table = ordered_backfill_cursors_table(state)

    # Start in :prioritize_cdc mode
    Stream.unfold({:ets.first(cdc_table), :ets.first(backfill_table), :prioritize_cdc}, fn
      # Both tables are empty, end the stream
      {:"$end_of_table", :"$end_of_table", _} ->
        nil

      # Backfill table is empty, stream CDC messages
      {cdc_cursor_tuple, :"$end_of_table", mode} ->
        next_message = Map.get(state.messages, cdc_cursor_tuple)
        next_accumulator = {:ets.next(cdc_table, cdc_cursor_tuple), :"$end_of_table", mode}
        {next_message, next_accumulator}

      # CDC table is empty, stream backfill messages
      {:"$end_of_table", backfill_cursor_tuple, mode} ->
        next_message = Map.get(state.messages, backfill_cursor_tuple)
        next_accumulator = {:"$end_of_table", :ets.next(backfill_table, backfill_cursor_tuple), mode}
        {next_message, next_accumulator}

      {cdc_cursor_tuple, backfill_cursor_tuple, :prioritize_cdc} ->
        # if any cdc messages have a conflict with backfill groups, we immediately switch to non prioritized mode
        if message_has_backfill_group_conflict?(state, cdc_cursor_tuple) do
          DebouncedLogger.warning(
            "[SlotMessageStore] Message has backfill group conflict, switching to strictly ordered mode",
            %DebouncedLogger.Config{
              dedupe_key: {:message_group_conflict, state.consumer_id},
              debounce_interval_ms: to_timeout(second: 10)
            }
          )

          stream_strictly_ordered(state, cdc_cursor_tuple, backfill_cursor_tuple)
        else
          next_message = Map.get(state.messages, cdc_cursor_tuple)
          next_accumulator = {:ets.next(cdc_table, cdc_cursor_tuple), backfill_cursor_tuple, :prioritize_cdc}
          {next_message, next_accumulator}
        end

      {cdc_cursor_tuple, backfill_cursor_tuple, :strictly_ordered} ->
        stream_strictly_ordered(state, cdc_cursor_tuple, backfill_cursor_tuple)
    end)
  end

  # Simple sorted stream for display purposes (no CDC prioritization).
  # Merges cdc and backfill ETS tables in commit_lsn/commit_idx order.
  # O(log n) per element using ETS ordered_set traversal.
  defp sorted_message_stream_for_display(%State{} = state, order) do
    cdc_table = ordered_cdc_cursors_table(state)
    backfill_table = ordered_backfill_cursors_table(state)

    # Choose traversal functions based on order
    {first_fn, next_fn, compare_fn} =
      case order do
        :asc -> {&:ets.first/1, &:ets.next/2, fn a, b -> a < b end}
        :desc -> {&:ets.last/1, &:ets.prev/2, fn a, b -> a > b end}
      end

    Stream.unfold({first_fn.(cdc_table), first_fn.(backfill_table)}, fn
      # Both tables exhausted
      {:"$end_of_table", :"$end_of_table"} ->
        nil

      # Only CDC messages remain
      {cdc_cursor, :"$end_of_table"} ->
        message = Map.get(state.messages, cdc_cursor)
        {message, {next_fn.(cdc_table, cdc_cursor), :"$end_of_table"}}

      # Only backfill messages remain
      {:"$end_of_table", backfill_cursor} ->
        message = Map.get(state.messages, backfill_cursor)
        {message, {:"$end_of_table", next_fn.(backfill_table, backfill_cursor)}}

      # Both have messages - pick based on order
      {cdc_cursor, backfill_cursor} ->
        if compare_fn.(cdc_cursor, backfill_cursor) do
          message = Map.get(state.messages, cdc_cursor)
          {message, {next_fn.(cdc_table, cdc_cursor), backfill_cursor}}
        else
          message = Map.get(state.messages, backfill_cursor)
          {message, {cdc_cursor, next_fn.(backfill_table, backfill_cursor)}}
        end
    end)
  end

  defp message_has_backfill_group_conflict?(%State{} = state, cursor_tuple) do
    message = Map.get(state.messages, cursor_tuple)
    not is_nil(message) and Multiset.member?(state.backfill_message_groups, group_id(message))
  end

  # In strictly ordered mode, we stream the lowest cursor tuple from either ets set
  # For use in sorted_message_stream/1
  defp stream_strictly_ordered(%State{} = state, cdc_cursor_tuple, backfill_cursor_tuple) do
    if cdc_cursor_tuple < backfill_cursor_tuple do
      cdc_table = ordered_cdc_cursors_table(state)
      next_message = Map.get(state.messages, cdc_cursor_tuple)
      next_accumulator = {:ets.next(cdc_table, cdc_cursor_tuple), backfill_cursor_tuple, :strictly_ordered}
      {next_message, next_accumulator}
    else
      backfill_table = ordered_backfill_cursors_table(state)
      next_message = Map.get(state.messages, backfill_cursor_tuple)
      next_accumulator = {cdc_cursor_tuple, :ets.next(backfill_table, backfill_cursor_tuple), :strictly_ordered}
      {next_message, next_accumulator}
    end
  end

  @doc """
  Audits the state for consistency between state.messages and the ETS tables.
  This includes both messages that exist in state.messages but not in the ETS tables,
  and vice-versa.
  """
  def audit_state(%State{} = state) do
    cdc_table = ordered_cdc_cursors_table(state)
    backfill_table = ordered_backfill_cursors_table(state)
    message_cursor_tuples = Map.keys(state.messages)

    messages_not_in_ets =
      Enum.count(message_cursor_tuples, fn {commit_lsn, commit_idx} ->
        not :ets.member(cdc_table, {commit_lsn, commit_idx}) and not :ets.member(backfill_table, {commit_lsn, commit_idx})
      end)

    cdc_ets_not_in_messages =
      :ets.foldl(
        fn {{commit_lsn, commit_idx}} = _cursor_tuple, acc ->
          if Map.has_key?(state.messages, {commit_lsn, commit_idx}) do
            acc
          else
            acc + 1
          end
        end,
        0,
        cdc_table
      )

    backfill_ets_not_in_messages =
      :ets.foldl(
        fn {{commit_lsn, commit_idx}} = _cursor_tuple, acc ->
          if Map.has_key?(state.messages, {commit_lsn, commit_idx}) do
            acc
          else
            acc + 1
          end
        end,
        0,
        backfill_table
      )

    %{
      messages_not_in_ets: messages_not_in_ets,
      cdc_ets_not_in_messages: cdc_ets_not_in_messages,
      backfill_ets_not_in_messages: backfill_ets_not_in_messages
    }
  end

  @doc """
  Returns messages that are older than the configured age threshold and not yet persisted.
  This helps prevent slot advancement from being blocked by slow message processing (often
  due to slow group processing or sequin stream syncs.)
  """
  @spec messages_to_flush(State.t(), non_neg_integer()) :: list(message())
  def messages_to_flush(%State{} = state, limit \\ 2000) do
    now = Sequin.utc_now()
    age_threshold = DateTime.add(now, -state.message_age_before_flush_ms, :millisecond)

    # Find first N messages that are older than threshold and not persisted
    state
    |> sorted_message_stream()
    |> Stream.reject(&message_persisted?(state, &1))
    |> Stream.filter(&DateTime.before?(&1.ingested_at, age_threshold))
    |> Enum.take(limit)
  end

  @doc """
  Returns messages that are stuck in a delivering state - they are in produced_message_groups
  but were delivered over 1 minute ago. This helps recover messages that may have been
  stuck due to crashes or other issues.
  """
  @spec messages_to_make_visible(State.t()) :: list(message())
  def messages_to_make_visible(%State{} = state) do
    max_time_since_delivered = DateTime.add(Sequin.utc_now(), -state.max_time_since_delivered_ms, :millisecond)

    state.produced_message_groups
    |> Multiset.values()
    |> Stream.map(fn commit_tuple -> Map.get(state.messages, commit_tuple) end)
    |> Stream.filter(fn
      nil -> false
      msg -> not is_nil(msg.last_delivered_at) and DateTime.before?(msg.last_delivered_at, max_time_since_delivered)
    end)
    |> Enum.to_list()
  end

  defp group_id(msg), do: {msg.table_oid, msg.group_id}

  @doc """
  Returns all messages from the state.
  """
  @spec all_messages(State.t()) :: list(message())
  def all_messages(%State{} = state) do
    Map.values(state.messages)
  end

  @doc """
  Returns messages that are currently failing (have been delivered at least once).
  """
  @spec failing_messages(State.t()) :: list(message())
  def failing_messages(%State{} = state) do
    state.messages
    |> Map.values()
    |> Enum.filter(fn msg -> msg.deliver_count > 0 end)
  end
end

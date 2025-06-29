defmodule Sequin.Runtime.SlotMessageStore.State do
  @moduledoc false
  use TypedStruct

  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Error
  alias Sequin.Multiset
  alias Sequin.Replication
  alias Sequin.Runtime.SlotMessageStore.State

  require Logger

  @type message :: ConsumerRecord.t() | ConsumerEvent.t()
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

    field :high_watermark_wal_cursor, Replication.wal_cursor() | nil
  end

  @spec setup_ets(State.t()) :: :ok
  def setup_ets(%State{} = state) do
    table_name = ordered_cursors_table(state)

    :ets.new(table_name, [:ordered_set, :named_table, :protected])
    :ok
  end

  @spec ordered_cursors_table(State.t()) :: atom()
  defp ordered_cursors_table(%State{} = state) do
    :"slot_message_store_state_ordered_cursors_consumer_#{state.consumer.seq}_partition_#{state.partition}"
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
      ets_keys = Enum.map(messages, fn msg -> {{msg.commit_lsn, msg.commit_idx}} end)

      state
      |> ordered_cursors_table()
      |> :ets.insert(ets_keys)

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

    case put_messages(state, messages, skip_limit_check?: true) do
      {:ok, %State{} = state} ->
        {:ok,
         %{
           state
           | table_reader_batch_id: batch_id,
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
    table = ordered_cursors_table(state)

    Enum.each(popped_messages, fn msg ->
      :ets.delete(table, {msg.commit_lsn, msg.commit_idx})
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
      # No unpersisted messages and no epoch cursor
      {nil, nil} ->
        nil

      # No unpersisted messages, use epoch cursor
      {nil, epoch_cursor} ->
        epoch_cursor

      # Has unpersisted messages regardless of epoch cursor, be conservative
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

  def peek_message(%State{} = state, ack_id) do
    case Map.get(state.ack_ids_to_cursor_tuples, ack_id) do
      nil -> {:error, Error.not_found(entity: :message, params: %{ack_id: ack_id})}
      cursor_tuple -> {:ok, Map.get(state.messages, cursor_tuple)}
    end
  end

  # This function provides an optimized way to take the first N messages from a map,
  # using ETS ordered set to maintain sort order
  defp sorted_message_stream(%State{} = state) do
    table = ordered_cursors_table(state)

    Stream.unfold(:ets.first(table), fn
      :"$end_of_table" ->
        nil

      cursor_tuple ->
        next_cursor_tuple = :ets.next(table, cursor_tuple)
        {Map.get(state.messages, cursor_tuple), next_cursor_tuple}
    end)
  end

  @doc """
  Counts messages that are out of sync between state.messages and ordered_cursors_table.
  This includes both messages that exist in state.messages but not in the ETS table,
  and vice versa.
  """
  def count_unsynced_messages(%State{} = state) do
    table = ordered_cursors_table(state)
    message_keys = Map.keys(state.messages)

    # Count keys in messages that aren't in ETS
    messages_not_in_ets =
      Enum.count(message_keys, fn {commit_lsn, commit_idx} ->
        not :ets.member(table, {commit_lsn, commit_idx})
      end)

    # Count keys in ETS that aren't in messages
    ets_not_in_messages =
      :ets.foldl(
        fn {{commit_lsn, commit_idx}} = _cursor_tuple, acc ->
          if Map.has_key?(state.messages, {commit_lsn, commit_idx}) do
            acc
          else
            acc + 1
          end
        end,
        0,
        table
      )

    messages_not_in_ets + ets_not_in_messages
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
end

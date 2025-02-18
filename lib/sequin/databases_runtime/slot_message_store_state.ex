defmodule Sequin.DatabasesRuntime.SlotMessageStore.State do
  @moduledoc false
  use TypedStruct

  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.DatabasesRuntime.SlotMessageStore.State
  alias Sequin.Error
  alias Sequin.Multiset
  alias Sequin.Replication

  require Logger

  @type message :: ConsumerRecord.t() | ConsumerEvent.t()
  @type cursor_tuple :: {commit_lsn :: non_neg_integer(), commit_idx :: non_neg_integer()}

  @default_setting_max_messages 500_000

  typedstruct do
    field :consumer, SinkConsumer.t()
    field :consumer_id, String.t()
    field :last_consumer_producer_pid, pid() | nil
    field :messages, %{cursor_tuple() => message()}, default: %{}
    field :ack_ids_to_cursor_tuples, %{SinkConsumer.ack_id() => cursor_tuple()}, default: %{}
    field :produced_message_groups, Multiset.t(), default: %{}
    field :persisted_message_groups, Multiset.t(), default: %{}
    field :unpersisted_cursor_tuples_for_table_reader_batch, MapSet.t(), default: MapSet.new()
    field :setting_max_messages, non_neg_integer(), default: @default_setting_max_messages
    field :setting_system_max_memory_bytes, non_neg_integer() | nil
    field :max_memory_bytes, non_neg_integer()
    field :payload_size_bytes, non_neg_integer(), default: 0
    field :slot_processor_monitor_ref, reference() | nil
    field :table_reader_batch_id, String.t() | nil
    field :test_pid, pid() | nil
  end

  @spec setup_ets(Sequin.Consumers.SinkConsumer.t()) :: :ok
  def setup_ets(consumer) do
    table_name = ordered_cursors_table(consumer)

    :ets.new(table_name, [:ordered_set, :named_table, :protected])
    :ok
  end

  @spec ordered_cursors_table(Sequin.Consumers.SinkConsumer.t()) :: atom()
  defp ordered_cursors_table(consumer) do
    :"slot_message_store_state_ordered_cursors_consumer_#{consumer.seq}"
  end

  @spec put_messages(State.t(), list(message()) | Enumerable.t()) ::
          {:ok, State.t()} | {:error, Error.t()}
  def put_messages(%State{} = state, messages, opts \\ []) do
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
        messages = Map.new(messages, &{{&1.commit_lsn, &1.commit_idx}, &1})

        # Insert into ETS
        ets_keys = Enum.map(messages, fn {commit_tuple, _msg} -> {commit_tuple} end)

        state.consumer
        |> ordered_cursors_table()
        |> :ets.insert(ets_keys)

        ack_ids_to_cursor_tuples =
          Map.new(messages, fn {_commit_tuple, msg} -> {msg.ack_id, {msg.commit_lsn, msg.commit_idx}} end)

        state =
          %{
            state
            | messages: Map.merge(state.messages, messages),
              ack_ids_to_cursor_tuples: Map.merge(state.ack_ids_to_cursor_tuples, ack_ids_to_cursor_tuples),
              payload_size_bytes: state.payload_size_bytes + incoming_payload_size_bytes
          }

        {:ok, state}
    end
  end

  @spec put_persisted_messages(State.t(), list(message()) | Enumerable.t()) :: {:ok, State.t()} | {:error, Error.t()}
  def put_persisted_messages(%State{} = state, messages) do
    persisted_message_groups =
      Enum.reduce(messages, state.persisted_message_groups, fn msg, acc ->
        Multiset.put(acc, msg.group_id, {msg.commit_lsn, msg.commit_idx})
      end)

    put_messages(%{state | persisted_message_groups: persisted_message_groups}, messages, skip_limit_check?: true)
  end

  @spec put_table_reader_batch(State.t(), list(message()), String.t()) ::
          {:ok, State.t()} | {:error, Error.t()}
  def put_table_reader_batch(%State{} = state, messages, batch_id) do
    messages = Enum.map(messages, &%{&1 | table_reader_batch_id: batch_id})

    cursor_tuples = Enum.map(messages, &{&1.commit_lsn, &1.commit_idx})

    unpersisted_cursor_tuples_for_table_reader_batch =
      MapSet.union(state.unpersisted_cursor_tuples_for_table_reader_batch, MapSet.new(cursor_tuples))

    case put_messages(state, messages) do
      {:ok, %State{} = state} ->
        {:ok,
         %{
           state
           | table_reader_batch_id: batch_id,
             unpersisted_cursor_tuples_for_table_reader_batch: unpersisted_cursor_tuples_for_table_reader_batch
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
    table = ordered_cursors_table(state.consumer)

    Enum.each(popped_messages, fn msg ->
      :ets.delete(table, {msg.commit_lsn, msg.commit_idx})
    end)

    persisted_message_groups =
      Enum.reduce(popped_messages, state.persisted_message_groups, fn msg, acc ->
        Multiset.delete(acc, msg.group_id, {msg.commit_lsn, msg.commit_idx})
      end)

    produced_message_groups =
      Enum.reduce(popped_messages, state.produced_message_groups, fn msg, acc ->
        Multiset.delete(acc, msg.group_id, {msg.commit_lsn, msg.commit_idx})
      end)

    unpersisted_cursor_tuples_for_table_reader_batch =
      MapSet.difference(state.unpersisted_cursor_tuples_for_table_reader_batch, MapSet.new(cursor_tuples))

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
         unpersisted_cursor_tuples_for_table_reader_batch: unpersisted_cursor_tuples_for_table_reader_batch
     }}
  end

  @spec pop_blocked_messages(State.t()) :: {list(message()), State.t()}
  def pop_blocked_messages(%State{} = state) do
    blocked_cursor_tuples =
      state.messages
      |> Stream.filter(fn {cursor_tuple, msg} ->
        is_message_group_persisted?(state, msg.group_id) and
          not Multiset.value_member?(state.persisted_message_groups, msg.group_id, cursor_tuple)
      end)
      |> Stream.map(fn {cursor_tuple, _msg} -> cursor_tuple end)
      |> Enum.to_list()

    pop_messages(state, blocked_cursor_tuples)
  end

  @spec pop_all_messages(State.t()) :: {list(message()), State.t()}
  def pop_all_messages(%State{} = state) do
    pop_messages(state, Map.keys(state.messages))
  end

  @spec is_message_group_persisted?(State.t(), String.t()) :: boolean()
  # Messages without group_ids do not belong to any group
  def is_message_group_persisted?(%State{}, nil) do
    false
  end

  def is_message_group_persisted?(%State{} = state, group_id) do
    Multiset.member?(state.persisted_message_groups, group_id)
  end

  @spec is_message_persisted?(State.t(), message()) :: boolean()
  def is_message_persisted?(%State{} = state, msg) do
    Multiset.value_member?(state.persisted_message_groups, msg.group_id, {msg.commit_lsn, msg.commit_idx})
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
        Multiset.value_member?(state.produced_message_groups, msg.group_id, cursor_tuple) and
          not is_nil(msg.last_delivered_at) and
          DateTime.before?(msg.last_delivered_at, ack_wait_ms_ago)
      end)
      |> Stream.map(fn {cursor_tuple, msg} -> {msg.group_id, cursor_tuple} end)
      |> Enum.to_list()

    produced_message_groups =
      Enum.reduce(stale_cursor_tuples, state.produced_message_groups, fn {group_id, cursor_tuple}, acc ->
        Multiset.delete(acc, group_id, cursor_tuple)
      end)

    %{state | produced_message_groups: produced_message_groups}
  end

  @spec reset_message_visibilities(State.t(), list(cursor_tuple())) :: State.t()
  def reset_message_visibilities(%State{} = state, []) do
    state
  end

  def reset_message_visibilities(%State{} = state, [cursor_tuple | cursor_tuples]) do
    case Map.get(state.messages, cursor_tuple) do
      nil ->
        reset_message_visibilities(state, cursor_tuples)

      msg ->
        msg = %{msg | not_visible_until: nil}

        state = %{
          state
          | messages: Map.put(state.messages, cursor_tuple, msg),
            produced_message_groups: Multiset.delete(state.produced_message_groups, msg.group_id, cursor_tuple)
        }

        reset_message_visibilities(state, cursor_tuples)
    end
  end

  @spec reset_all_message_visibilities(State.t()) :: State.t()
  def reset_all_message_visibilities(%State{} = state) do
    cursor_tuples = Map.keys(state.messages)
    reset_message_visibilities(state, cursor_tuples)
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

    case min_messages_wal_cursor do
      nil -> nil
      {commit_lsn, commit_idx} -> %{commit_lsn: commit_lsn, commit_idx: commit_idx}
    end
  end

  @spec ack_ids_to_cursor_tuples(State.t(), list(SinkConsumer.ack_id())) :: list(cursor_tuple())
  def ack_ids_to_cursor_tuples(%State{} = state, ack_ids) do
    ack_ids
    |> Stream.map(&Map.get(state.ack_ids_to_cursor_tuples, &1))
    |> Enum.filter(& &1)
  end

  @spec produce_messages(State.t(), non_neg_integer()) :: {list(message()), State.t()}
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
        Multiset.put(acc, msg.group_id, {msg.commit_lsn, msg.commit_idx})
      end)

    {messages,
     %{
       state
       | produced_message_groups: produced_message_groups,
         # Replace messages in state to set last_delivered_at
         messages: Map.merge(state.messages, Map.new(messages, &{{&1.commit_lsn, &1.commit_idx}, &1}))
     }}
  end

  @spec unpersisted_table_reader_messages?(State.t()) :: boolean()
  def unpersisted_table_reader_messages?(%State{} = state) do
    MapSet.size(state.unpersisted_cursor_tuples_for_table_reader_batch) != 0
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
              not is_nil(msg.group_id) and Multiset.member?(state.produced_message_groups, msg.group_id) ->
                false

              is_nil(msg.group_id) and
                  Multiset.value_member?(state.produced_message_groups, msg.group_id, {msg.commit_lsn, msg.commit_idx}) ->
                false

              not is_nil(msg.group_id) and MapSet.member?(delivered_message_group_ids, msg.group_id) ->
                false

              is_nil(msg.not_visible_until) ->
                true

              DateTime.before?(msg.not_visible_until, now) ->
                true

              true ->
                false
            end

          if deliverable? do
            {:cont, {acc_count + 1, MapSet.put(delivered_message_group_ids, msg.group_id), [msg | deliverable_messages]}}
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

  # This function provides an optimized way to take the first N messages from a map,
  # using ETS ordered set to maintain sort order
  defp sorted_message_stream(%State{} = state) do
    table = ordered_cursors_table(state.consumer)

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
    table = ordered_cursors_table(state.consumer)
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
end

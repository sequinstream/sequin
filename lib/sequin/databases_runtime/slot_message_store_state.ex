defmodule Sequin.DatabasesRuntime.SlotMessageStore.State do
  @moduledoc false
  use TypedStruct

  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.DatabasesRuntime.KeysetCursor
  alias Sequin.DatabasesRuntime.SlotMessageStore.State
  alias Sequin.DatabasesRuntime.SlotMessageStore.State.BackfillState
  alias Sequin.DatabasesRuntime.SlotMessageStore.State.BackfillState.FetchingIds
  alias Sequin.DatabasesRuntime.SlotMessageStore.State.BackfillState.FetchingMessages
  alias Sequin.DatabasesRuntime.SlotMessageStore.State.BackfillState.Idle
  alias Sequin.DatabasesRuntime.SlotMessageStore.State.BackfillState.ReadyToFlush
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
    field :slot_processor_monitor_ref, reference() | nil

    # Resource utilization controls
    field :setting_max_messages, non_neg_integer(), default: @default_setting_max_messages
    field :setting_system_max_memory_bytes, non_neg_integer() | nil
    field :max_memory_bytes, non_neg_integer()
    field :payload_size_bytes, non_neg_integer(), default: 0

    # Backfills
    field :backfill_cursor, KeysetCursor.cursor() | nil
    field :backfill_state, BackfillState.t()
    field :table_reader_mod, module()

    field :table_reader_batch_id, String.t() | nil
    field :unpersisted_cursor_tuples_for_table_reader_batch, MapSet.t(), default: MapSet.new()

    # Test
    field :test_pid, pid() | nil
  end

  defmodule BackfillState do
    @moduledoc false
    alias Sequin.DatabasesRuntime.SlotMessageStore.State

    @type t :: Idle.t() | FetchingIds.t() | FetchingMessages.t()
    typedstruct module: Idle do
    end

    typedstruct module: FetchingIds do
    end

    typedstruct module: FetchingMessages do
      field :emitted_batch_lsn, non_neg_integer() | nil
      field :logical_message_lsn, non_neg_integer() | nil
      field :primary_keys_being_fetched, MapSet.t()
      field :messages, [State.message()] | nil
    end

    typedstruct module: ReadyToFlush do
      field :messages, [State.message()]
    end
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

    popped_messages_bytes = Enum.sum_by(popped_messages, & &1.payload_size_bytes)

    {popped_messages,
     %{
       state
       | messages: messages,
         ack_ids_to_cursor_tuples: Map.drop(state.ack_ids_to_cursor_tuples, popped_message_ack_ids),
         payload_size_bytes: state.payload_size_bytes - popped_messages_bytes,
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

  @spec backfill_fetch_ids_started(State.t()) :: State.t()
  def backfill_fetch_ids_started(%State{backfill_state: %Idle{}} = state) do
    %{state | backfill_state: %FetchingIds{}}
  end

  def backfill_fetch_ids_completed(%State{backfill_state: %FetchingIds{}} = state, primary_keys) do
    %{state | backfill_state: %FetchingMessages{primary_keys_being_fetched: MapSet.new(primary_keys)}}
  end

  @spec backfill_fetch_messages_completed(State.t(), list(message()), non_neg_integer()) :: State.t()
  def backfill_fetch_messages_completed(
        %State{backfill_state: %FetchingMessages{logical_message_lsn: nil}} = state,
        messages,
        emitted_batch_lsn
      ) do
    backfill_state = %FetchingMessages{state.backfill_state | emitted_batch_lsn: emitted_batch_lsn, messages: messages}

    %{state | backfill_state: backfill_state}
  end

  def backfill_fetch_messages_completed(
        %State{backfill_state: %FetchingMessages{logical_message_lsn: lsn}} = state,
        messages,
        lsn
      ) do
    %FetchingMessages{
      primary_keys_being_fetched: primary_keys_being_fetched
    } = state.backfill_state

    messages_to_flush =
      Enum.filter(messages, fn message ->
        MapSet.member?(primary_keys_being_fetched, message.record_pks)
      end)

    %{state | backfill_state: %ReadyToFlush{messages: messages_to_flush}}
  end

  def backfill_fetch_messages_completed(
        %State{backfill_state: %FetchingMessages{logical_message_lsn: logical_lsn}} = state,
        messages,
        emitted_batch_lsn
      )
      when emitted_batch_lsn > logical_lsn do
    %{
      state
      | backfill_state: %FetchingMessages{
          state.backfill_state
          | emitted_batch_lsn: emitted_batch_lsn,
            logical_message_lsn: nil,
            messages: messages
        }
    }
  end

  def backfill_fetch_messages_completed(
        %State{backfill_state: %FetchingMessages{logical_message_lsn: logical_lsn}},
        _messages,
        emitted_batch_lsn
      )
      when emitted_batch_lsn < logical_lsn do
    raise Error.invariant(
            message:
              "Previously received a logical message with a commit LSN that is higher than the one emitted by the fetch message Task"
          )
  end

  def backfill_fetch_messages_completed(state, _messages, emitted_batch_lsn) do
    raise Error.invariant(
            message: """
            backfill_fetch_messages_completed/3 called with unexpected state:
              backfill_state: #{inspect(Map.drop(state.backfill_state, [:messages, :primary_keys_being_fetched]), pretty: true)}
              emitted_batch_lsn: #{emitted_batch_lsn}
            """
          )
  end

  # Logical messages received when we aren't expecting them are likely from a previous backfill
  def backfill_logical_message_received(%State{backfill_state: %Idle{}} = state, _logical_message_lsn) do
    state
  end

  # Logical messages received when we aren't expecting them are likely from a previous backfill
  def backfill_logical_message_received(%State{backfill_state: %FetchingIds{}} = state, _logical_message_lsn) do
    state
  end

  # emitted_batch_lsn is nil because we get the logical message before we receive the fetch_messages response
  def backfill_logical_message_received(
        %State{backfill_state: %FetchingMessages{emitted_batch_lsn: nil}} = state,
        logical_message_lsn
      ) do
    %{state | backfill_state: %FetchingMessages{state.backfill_state | logical_message_lsn: logical_message_lsn}}
  end

  # here the expected_lsn is equal to the commit_lsn of the logical message, so we can flush
  def backfill_logical_message_received(
        %State{backfill_state: %FetchingMessages{emitted_batch_lsn: batch_lsn}} = state,
        batch_lsn
      ) do
    %FetchingMessages{
      messages: messages,
      primary_keys_being_fetched: primary_keys_being_fetched
    } = state.backfill_state

    messages_to_flush =
      Enum.filter(messages, fn message ->
        MapSet.member?(primary_keys_being_fetched, message.record_pks)
      end)

    %{state | backfill_state: %ReadyToFlush{messages: messages_to_flush}}
  end

  # We have an emitted batch LSN from a fetch_messages Task. So any logical message with a commit_lsn
  # less than the emitted batch LSN is for a batch that we have already processed.
  def backfill_logical_message_received(
        %State{backfill_state: %FetchingMessages{emitted_batch_lsn: emitted_batch_lsn}} = state,
        logical_message_lsn
      )
      when logical_message_lsn < emitted_batch_lsn do
    state
  end

  def backfill_logical_message_received(
        %State{backfill_state: %FetchingMessages{emitted_batch_lsn: emitted_batch_lsn}},
        logical_message_lsn
      )
      when logical_message_lsn > emitted_batch_lsn and is_integer(logical_message_lsn) and is_integer(emitted_batch_lsn) do
    raise Error.invariant(message: "Received logical message with commit_lsn greater than expected commit_lsn")
  end

  def backfill_logical_message_received(%State{backfill_state: %Idle{}} = state, _logical_message_lsn) do
    state
  end

  def backfill_logical_message_received(state, logical_message_lsn) do
    raise Error.invariant(
            message: """
            backfill_logical_message_received/2 called with unexpected state:
              backfill_state: #{inspect(Map.drop(state.backfill_state, [:messages, :primary_keys_being_fetched]), pretty: true)}
              logical_message_lsn: #{logical_message_lsn}
            """
          )
  end

  def backfill_should_flush?(%State{backfill_state: %ReadyToFlush{}}), do: true
  def backfill_should_flush?(%State{backfill_state: _}), do: false

  def backfill_flush_messages(%State{backfill_state: %ReadyToFlush{messages: messages}} = state) do
    {:ok, state} = State.put_messages(state, messages, skip_limit_check?: true)
    %State{state | backfill_state: %Idle{}}
  end

  def backfill_completed(%State{backfill_state: %FetchingIds{}} = state) do
    %{state | backfill_state: %Idle{}}
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

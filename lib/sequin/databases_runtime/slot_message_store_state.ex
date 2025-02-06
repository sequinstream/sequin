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

  # 1GB
  @default_setting_max_accumulated_payload_bytes 1 * 1024 * 1024 * 1024
  @default_setting_max_messages 100_000

  typedstruct do
    field :consumer, SinkConsumer.t()
    field :consumer_id, String.t()
    field :last_consumer_producer_pid, pid() | nil
    field :messages, %{SinkConsumer.ack_id() => message()}, default: %{}
    field :produced_message_groups, Multiset.t(), default: %{}
    field :persisted_message_groups, Multiset.t(), default: %{}

    field :setting_max_accumulated_payload_bytes, non_neg_integer(),
      default: @default_setting_max_accumulated_payload_bytes

    field :setting_max_messages, non_neg_integer(), default: @default_setting_max_messages

    field :payload_size_bytes, non_neg_integer(), default: 0
    field :slot_processor_monitor_ref, reference() | nil
    field :table_reader_batch_id, String.t() | nil
    field :test_pid, pid() | nil
  end

  @spec put_messages(State.t(), list(message()) | Enumerable.t()) ::
          {:ok, State.t()} | {:error, Error.t()}
  def put_messages(%State{} = state, messages) do
    now = DateTime.utc_now()

    incoming_payload_size_bytes = Enum.sum_by(messages, & &1.payload_size_bytes)

    bytes_exceeded? =
      state.payload_size_bytes + incoming_payload_size_bytes > state.setting_max_accumulated_payload_bytes

    messages_exceeded? = length(messages) + map_size(state.messages) > state.setting_max_messages

    cond do
      bytes_exceeded? ->
        {:error, Error.invariant(message: "Payload size limit exceeded", code: :payload_size_limit_exceeded)}

      messages_exceeded? ->
        {:error, Error.invariant(message: "Message count limit exceeded", code: :payload_size_limit_exceeded)}

      true ->
        messages =
          messages
          |> Stream.map(fn msg -> %{msg | ack_id: Sequin.uuid4(), ingested_at: now} end)
          |> Map.new(&{&1.ack_id, &1})

        state =
          %{
            state
            | messages: Map.merge(state.messages, messages),
              payload_size_bytes: state.payload_size_bytes + incoming_payload_size_bytes
          }

        {:ok, state}
    end
  end

  @spec put_persisted_messages(State.t(), list(message()) | Enumerable.t()) :: {:ok, State.t()} | {:error, Error.t()}
  def put_persisted_messages(%State{} = state, messages) do
    incoming_payload_size_bytes = Enum.sum_by(messages, & &1.payload_size_bytes)
    messages = Map.new(messages, &{&1.ack_id, &1})

    persisted_message_groups =
      Enum.reduce(messages, state.persisted_message_groups, fn {_ack_id, msg}, acc ->
        Multiset.put(acc, msg.group_id, msg.ack_id)
      end)

    state = %{
      state
      | messages: Map.merge(state.messages, messages),
        persisted_message_groups: persisted_message_groups,
        payload_size_bytes: state.payload_size_bytes + incoming_payload_size_bytes
    }

    {:ok, state}
  end

  @spec put_table_reader_batch(State.t(), list(message()), String.t()) ::
          {:ok, State.t()} | {:error, Error.t()}
  def put_table_reader_batch(%State{} = state, messages, batch_id) do
    messages = Enum.map(messages, &%{&1 | table_reader_batch_id: batch_id})

    case put_messages(state, messages) do
      {:ok, %State{} = state} ->
        {:ok, %{state | table_reader_batch_id: batch_id}}

      error ->
        error
    end
  end

  @spec pop_messages(State.t(), list(SinkConsumer.ack_id())) :: {list(message()), State.t()}
  def pop_messages(%State{} = state, ack_ids) do
    popped_messages = state.messages |> Map.take(ack_ids) |> Map.values()
    messages = Map.drop(state.messages, ack_ids)

    persisted_message_groups =
      Enum.reduce(popped_messages, state.persisted_message_groups, fn msg, acc ->
        Multiset.delete(acc, msg.group_id, msg.ack_id)
      end)

    produced_message_groups =
      Enum.reduce(popped_messages, state.produced_message_groups, fn msg, acc ->
        Multiset.delete(acc, msg.group_id, msg.ack_id)
      end)

    popped_messages_bytes = Enum.sum_by(popped_messages, & &1.payload_size_bytes)

    {popped_messages,
     %{
       state
       | messages: messages,
         payload_size_bytes: state.payload_size_bytes - popped_messages_bytes,
         produced_message_groups: produced_message_groups,
         persisted_message_groups: persisted_message_groups
     }}
  end

  @spec pop_blocked_messages(State.t()) :: {list(message()), State.t()}
  def pop_blocked_messages(%State{} = state) do
    blocked_message_ack_ids =
      state.messages
      |> Stream.filter(fn {ack_id, msg} ->
        Multiset.member?(state.persisted_message_groups, msg.group_id) and
          not Multiset.value_member?(state.persisted_message_groups, msg.group_id, ack_id)
      end)
      |> Stream.map(fn {ack_id, _msg} -> ack_id end)
      |> Enum.to_list()

    pop_messages(state, blocked_message_ack_ids)
  end

  @spec is_message_group_persisted?(State.t(), String.t()) :: boolean()
  def is_message_group_persisted?(%State{} = state, group_id) do
    Multiset.member?(state.persisted_message_groups, group_id)
  end

  @spec is_message_persisted?(State.t(), message()) :: boolean()
  def is_message_persisted?(%State{} = state, msg) do
    Multiset.value_member?(state.persisted_message_groups, msg.group_id, msg.ack_id)
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

    stale_ack_ids =
      state.messages
      |> Stream.filter(fn {ack_id, msg} ->
        Multiset.value_member?(state.produced_message_groups, msg.group_id, ack_id) and
          not is_nil(msg.last_delivered_at) and
          DateTime.before?(msg.last_delivered_at, ack_wait_ms_ago)
      end)
      |> Stream.map(fn {ack_id, msg} -> {msg.group_id, ack_id} end)
      |> Enum.to_list()

    produced_message_groups =
      Enum.reduce(stale_ack_ids, state.produced_message_groups, fn {group_id, ack_id}, acc ->
        Multiset.delete(acc, group_id, ack_id)
      end)

    %{state | produced_message_groups: produced_message_groups}
  end

  @spec reset_message_visibilities(State.t(), list(SinkConsumer.ack_id())) :: State.t()
  def reset_message_visibilities(%State{} = state, []) do
    state
  end

  def reset_message_visibilities(%State{} = state, [ack_id | ack_ids]) do
    case Map.get(state.messages, ack_id) do
      nil ->
        reset_message_visibilities(state, ack_ids)

      msg ->
        msg = %{msg | not_visible_until: nil}

        state = %{
          state
          | messages: Map.put(state.messages, ack_id, msg),
            produced_message_groups: Multiset.delete(state.produced_message_groups, msg.group_id, ack_id)
        }

        reset_message_visibilities(state, ack_ids)
    end
  end

  @spec reset_all_message_visibilities(State.t()) :: State.t()
  def reset_all_message_visibilities(%State{} = state) do
    ack_ids = Map.keys(state.messages)
    reset_message_visibilities(state, ack_ids)
  end

  @spec min_unpersisted_wal_cursor(State.t()) :: Replication.wal_cursor() | nil
  def min_unpersisted_wal_cursor(%State{} = state) do
    persisted_message_ack_ids = state.persisted_message_groups |> Multiset.values() |> MapSet.new()

    min_messages_wal_cursor =
      state.messages
      |> Stream.reject(fn {ack_id, _msg} ->
        MapSet.member?(persisted_message_ack_ids, ack_id)
      end)
      |> Stream.map(fn {_k, msg} -> {msg.commit_lsn, msg.commit_idx} end)
      |> Enum.min(fn -> nil end)

    case min_messages_wal_cursor do
      nil -> nil
      {commit_lsn, commit_idx} -> %{commit_lsn: commit_lsn, commit_idx: commit_idx}
    end
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
        Multiset.put(acc, msg.group_id, msg.ack_id)
      end)

    {messages,
     %{
       state
       | produced_message_groups: produced_message_groups,
         # Replace messages in state to set last_delivered_at
         messages: Map.merge(state.messages, Map.new(messages, &{&1.ack_id, &1}))
     }}
  end

  def batch_progress(%State{} = state, batch_id) do
    cond do
      state.table_reader_batch_id == nil ->
        Logger.warning("[SlotMessageStore] No batch in progress")
        {:error, Error.invariant(message: "No batch in progress")}

      state.table_reader_batch_id != batch_id ->
        Logger.warning(
          "[SlotMessageStore] Batch mismatch",
          expected_batch_id: batch_id,
          actual_batch_id: state.table_reader_batch_id
        )

        {:error, Error.invariant(message: "Batch mismatch. Expected #{batch_id} but got #{state.table_reader_batch_id}")}

      has_unpersisted_batch_messages?(state, batch_id) ->
        Logger.info(
          "[SlotMessageStore] Batch is in progress",
          batch_id: state.table_reader_batch_id
        )

        {:ok, :in_progress}

      true ->
        Logger.info("[SlotMessageStore] Batch is completed", batch_id: state.table_reader_batch_id)
        {:ok, :completed}
    end
  end

  defp has_unpersisted_batch_messages?(%State{} = state, batch_id) do
    persisted_message_ack_ids = state.persisted_message_groups |> Multiset.values() |> MapSet.new()

    state.messages
    |> Stream.reject(fn {_k, msg} -> MapSet.member?(persisted_message_ack_ids, msg.ack_id) end)
    |> Stream.map(fn {_k, v} -> v end)
    |> Enum.any?(&(&1.table_reader_batch_id == batch_id))
  end

  defp deliverable_messages(%State{} = state, count) do
    now = Sequin.utc_now()

    {_, _, deliverable_messages} =
      state.messages
      |> sorted_message_stream()
      |> Enum.reduce_while({0, MapSet.new(), []}, fn
        _msg, {acc_count, _, deliverable_messages} when acc_count >= count ->
          {:halt, {acc_count, nil, deliverable_messages}}

        msg, {acc_count, delivered_message_group_ids, deliverable_messages} ->
          deliverable? =
            cond do
              Multiset.member?(state.produced_message_groups, msg.group_id) ->
                false

              MapSet.member?(delivered_message_group_ids, msg.group_id) ->
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
    state.messages
    |> sorted_message_stream()
    |> Enum.take(count)
  end

  def peek_messages(%State{} = state, ack_ids) when is_list(ack_ids) do
    state.messages
    |> Map.take(ack_ids)
    |> Map.values()
  end

  # This function provides an optimized way to take the first N messages from a map,
  # without blowing up memory consumption.
  defp sorted_message_stream(messages) do
    ack_ids =
      messages
      |> Stream.map(fn {ack_id, message} -> {ack_id, {message.commit_lsn, message.commit_idx}} end)
      |> Enum.sort_by(fn {_ack_id, {commit_lsn, commit_idx}} -> {commit_lsn, commit_idx} end)
      |> Enum.map(fn {ack_id, {_commit_lsn, _commit_idx}} -> ack_id end)

    Stream.map(ack_ids, fn ack_id -> Map.get(messages, ack_id) end)
  end
end

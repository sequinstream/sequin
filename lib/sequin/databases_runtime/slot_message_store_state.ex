defmodule Sequin.DatabasesRuntime.SlotMessageStore.State do
  @moduledoc false
  use TypedStruct

  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.DatabasesRuntime.SlotMessageStore.State
  alias Sequin.Error

  require Logger

  typedstruct do
    field :consumer, SinkConsumer.t()
    field :consumer_id, String.t()
    field :flush_batch_size, non_neg_integer()
    field :flush_interval, non_neg_integer()
    field :flush_wait_ms, non_neg_integer()
    field :messages, %{SinkConsumer.ack_id() => ConsumerRecord.t() | ConsumerEvent.t()}, default: %{}
    # Set to false in tests to disable Postgres reads/writes (ie. load messages on boot and flush messages on interval)
    field :persisted_mode?, boolean(), default: true
    field :slot_processor_monitor_ref, reference() | nil
    field :table_reader_batch_id, String.t() | nil
    field :test_pid, pid() | nil
    # TODO: rename pulled_lsn when we go pull based
    field :put_lsn_high_water_mark, non_neg_integer() | nil
  end

  def max_messages_in_memory, do: 10_000

  def put_messages(%State{} = state, messages, put_lsn_high_water_mark) do
    now = DateTime.utc_now()

    messages =
      messages
      |> Stream.map(fn msg -> %{msg | ack_id: Sequin.uuid4(), dirty: true, ingested_at: now} end)
      |> Stream.reject(fn
        %ConsumerRecord{deleted: true} -> true
        _ -> false
      end)
      |> Map.new(&{&1.ack_id, &1})

    %{state | messages: Map.merge(state.messages, messages), put_lsn_high_water_mark: put_lsn_high_water_mark}
  end

  def put_table_reader_batch(%State{} = state, messages, batch_id) do
    messages = Enum.map(messages, &%{&1 | table_reader_batch_id: batch_id})
    state = put_messages(state, messages)

    %{state | table_reader_batch_id: batch_id}
  end

  @spec ack(%State{}, list(SinkConsumer.ack_id())) :: {%State{}, non_neg_integer(), non_neg_integer()}
  def ack(%State{} = state, ack_ids) do
    initial_count = map_size(state.messages)
    {dropped_messages, messages} = Map.split(state.messages, ack_ids)
    dropped_messages = Map.values(dropped_messages)
    final_count = map_size(messages)

    {%{state | messages: messages}, dropped_messages, initial_count - final_count}
  end

  @spec nack(%State{}, %{SinkConsumer.ack_id() => SinkConsumer.not_visible_until()}) :: {%State{}, non_neg_integer()}
  def nack(%State{} = state, ack_ids_with_not_visible_until) do
    {updated_messages, nacked_count} =
      Enum.reduce(ack_ids_with_not_visible_until, {state.messages, 0}, fn {ack_id, not_visible_until},
                                                                          {acc_msgs, nacked_count} ->
        if msg = Map.get(acc_msgs, ack_id) do
          msg = %{msg | not_visible_until: not_visible_until, state: :available, dirty: true}
          {Map.replace(acc_msgs, ack_id, msg), nacked_count + 1}
        else
          {acc_msgs, nacked_count}
        end
      end)

    {%{state | messages: updated_messages}, nacked_count}
  end

  @spec reset_message_visibility(%State{}, SinkConsumer.ack_id()) :: {%State{}, ConsumerRecord.t() | ConsumerEvent.t()}
  def reset_message_visibility(%State{} = state, ack_id) do
    case Map.get(state.messages, ack_id) do
      nil ->
        {state, nil}

      msg ->
        updated_msg = %{msg | not_visible_until: nil, state: :available, dirty: true}
        {update_messages(state, [updated_msg]), updated_msg}
    end
  end

  @spec reset_all_visibility(%State{}) :: {%State{}, list(ConsumerRecord.t() | ConsumerEvent.t())}
  def reset_all_visibility(%State{} = state) do
    updated_messages =
      state.messages
      |> Stream.map(fn
        {_ack_id, %{not_visible_until: nil, state: :available}} -> nil
        {_ack_id, msg} -> %{msg | not_visible_until: nil, state: :available, dirty: true}
      end)
      |> Enum.filter(& &1)

    {update_messages(state, updated_messages), updated_messages}
  end

  @spec min_unflushed_commit_lsn(%State{}, reference()) :: non_neg_integer() | nil
  def min_unflushed_commit_lsn(%State{slot_processor_monitor_ref: ref1} = state, ref2) do
    if ref1 == ref2 do
      state.messages
      |> Map.values()
      |> Stream.filter(&is_nil(&1.flushed_at))
      |> Enum.min_by(& &1.commit_lsn, fn -> nil end)
      |> case do
        nil -> nil
        %ConsumerRecord{commit_lsn: commit_lsn} -> commit_lsn
        %ConsumerEvent{commit_lsn: commit_lsn} -> commit_lsn
      end
    else
      raise "Monitor ref mismatch. Expected #{inspect(ref1)} but got #{inspect(ref2)}"
    end
  end

  def deliverable_messages(%State{} = state, count) do
    now = DateTime.utc_now()

    undeliverable_group_ids =
      state.messages
      |> Map.values()
      |> Enum.reduce(MapSet.new(), fn msg, acc ->
        if not is_nil(msg.not_visible_until) and DateTime.after?(msg.not_visible_until, now) do
          MapSet.put(acc, msg.group_id)
        else
          acc
        end
      end)

    state.messages
    |> Map.values()
    |> Enum.sort_by(&{&1.commit_lsn, &1.commit_idx})
    |> Sequin.Enum.take_until(count, fn msg ->
      not MapSet.member?(undeliverable_group_ids, msg.group_id)
    end)
  end

  def deliver_messages(%State{} = state, messages) do
    %SinkConsumer{} = consumer = state.consumer
    now = DateTime.utc_now()
    not_visible_until = DateTime.add(now, consumer.ack_wait_ms, :millisecond)

    messages =
      Enum.map(messages, fn msg ->
        %{
          msg
          | not_visible_until: not_visible_until,
            deliver_count: msg.deliver_count + 1,
            last_delivered_at: now,
            state: :delivered,
            dirty: true
        }
      end)

    # Replace old messages with updated ones in state
    state = update_messages(state, messages)

    {state, messages}
  end

  def messages_to_flush(%State{messages: messages, flush_batch_size: flush_batch_size, flush_wait_ms: flush_wait_ms}) do
    flush_all? = map_size(messages) > max_messages_in_memory()

    messages
    |> Map.values()
    |> Stream.filter(& &1.dirty)
    |> Stream.filter(fn msg ->
      # Only flush messages that were ingested before the flush wait time
      # This gives the ConsumerProducer time to pull, deliver, and ack messages
      # before we flush them to Postgres
      flush_all? or Sequin.Time.before_ms_ago?(msg.ingested_at, flush_wait_ms)
    end)
    |> Enum.sort_by(& &1.flushed_at, &compare_flushed_at/2)
    |> Enum.take(flush_batch_size)
  end

  def flush_messages(%State{persisted_mode?: true} = state, messages) do
    flushed_at = DateTime.utc_now()
    messages = Enum.map(messages, fn msg -> %{msg | flushed_at: flushed_at, dirty: false} end)

    update_messages(state, messages)
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

      state.messages |> Map.values() |> Enum.any?(&(is_nil(&1.flushed_at) and &1.table_reader_batch_id == batch_id)) ->
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

  @spec peek_messages(%State{}, non_neg_integer()) :: list(ConsumerRecord.t() | ConsumerEvent.t())
  def peek_messages(%State{} = state, count) do
    state.messages
    |> Map.values()
    |> Enum.sort_by(&{&1.commit_lsn, &1.commit_idx})
    |> Enum.take(count)
  end

  @spec safe_ack_lsn(%State{}) :: non_neg_integer() | nil
  def safe_ack_lsn(%State{} = state) do
    state.messages
    |> Stream.map(fn {_ack_id, msg} -> Map.take(msg, [:commit_lsn, :flushed_at]) end)
    |> Enum.group_by(& &1.commit_lsn, & &1.flushed_at)
    |> Enum.sort_by(fn {commit_lsn, _flushed_at_values} -> commit_lsn end)
    |> case do
      [] ->
        # If there are no messages in state, we return the last pulled lsn
        # This will allow us to safely advance the slot even when there are no messages
        # Either because there are no messages for this sink consumer, or because
        # the sink is processing messages faster than we can flush them.
        state.put_lsn_high_water_mark

      commit_lsn_with_flushed_at_values ->
        Enum.reduce_while(commit_lsn_with_flushed_at_values, nil, fn {commit_lsn, flushed_at_values}, safe_ack_lsn ->
          if Enum.any?(flushed_at_values, &is_nil/1) do
            {:halt, safe_ack_lsn}
          else
            {:cont, commit_lsn}
          end
        end)
    end
  end

  defp update_messages(%State{} = state, messages) do
    messages = Map.new(messages, &{&1.ack_id, &1})

    %{state | messages: Map.merge(state.messages, messages)}
  end

  # Helper function to compare flushed_at values where nil is "smaller" than any DateTime
  # nil is "smaller"
  defp compare_flushed_at(nil, _), do: true
  # anything is "larger" than nil
  defp compare_flushed_at(_, nil), do: false
  defp compare_flushed_at(a, b), do: DateTime.compare(a, b) in [:lt, :eq]
end

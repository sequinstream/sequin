defmodule Sequin.DatabasesRuntime.SlotMessageStore.State do
  @moduledoc false
  use TypedStruct

  alias Sequin.Consumers
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.DatabasesRuntime.SlotMessageStore.State
  alias Sequin.Error

  require Logger

  @disk_overflow_allow_list ~w(
    c4192100-f55e-4203-9836-3a00398d2876
    b4558a5c-ef60-4d4e-8950-237ac53efbee
  )

  defguardp event_messages?(state) when state.consumer.message_kind == :event
  defguardp record_messages?(state) when state.consumer.message_kind == :record

  typedstruct do
    field :consumer, SinkConsumer.t()
    field :consumer_id, String.t()
    field :disk_overflow_mode?, boolean(), default: false
    field :flush_batch_size, non_neg_integer()
    field :flush_interval, non_neg_integer()
    field :flush_wait_ms, non_neg_integer()
    field :max_messages_in_memory, non_neg_integer(), default: 10_000
    field :messages, %{SinkConsumer.ack_id() => ConsumerRecord.t() | ConsumerEvent.t()}, default: %{}
    # Set to false in tests to disable Postgres reads/writes (ie. load messages on boot and flush messages on interval)
    field :persisted_mode?, boolean(), default: true
    field :slot_processor_monitor_ref, reference() | nil
    field :table_reader_batch_id, String.t() | nil
    field :test_pid, pid() | nil
  end

  def init_from_postgres(%State{persisted_mode?: false} = state), do: {:ok, state}

  def init_from_postgres(%State{persisted_mode?: true} = state) do
    case Consumers.get_sink_consumer(state.consumer_id) do
      {:ok, consumer} ->
        Logger.metadata(account_id: consumer.account_id)

        Logger.info("[SlotMessageStore] Loading messages...")
        {time, state} = :timer.tc(fn -> load_messages(%{state | consumer: consumer}) end)
        Logger.info("[SlotMessageStore] Loaded messages", count: map_size(state.messages), time_ms: div(time, 1000))

        {:ok, state}

      {:error, %Error.NotFoundError{entity: :consumer} = error} ->
        Logger.error("[SlotMessageStore] Consumer not found", consumer_id: state.consumer_id)
        {:error, error}
    end
  end

  def put_messages(%State{} = state, messages) do
    now = DateTime.utc_now()

    messages =
      messages
      |> Stream.reject(fn
        %ConsumerRecord{deleted: true} -> true
        _ -> false
      end)
      |> Enum.map(fn msg -> %{msg | ack_id: Sequin.uuid4(), dirty: true, ingested_at: now} end)

    cond do
      state.disk_overflow_mode? ->
        count = flush_messages_in_batches!(state, messages)
        Logger.debug("[SlotMessageStore] Flushed incoming messages to disk", count: count)
        state

      not state.disk_overflow_mode? and state.consumer_id in @disk_overflow_allow_list and
          map_size(state.messages) + length(messages) > state.max_messages_in_memory ->
        count = flush_messages_in_batches!(state, messages)
        Logger.debug("[SlotMessageStore] Flushed incoming messages to disk", count: count)
        Logger.info("[SlotMessageStore] Entering disk overflow mode")
        %{state | disk_overflow_mode?: true}

      true ->
        messages = Map.new(messages, &{&1.ack_id, &1})
        %{state | messages: Map.merge(state.messages, messages)}
    end
  end

  def put_table_reader_batch(%State{} = state, messages, batch_id) do
    cond do
      state.disk_overflow_mode? ->
        count = flush_messages_in_batches!(state, messages)
        Logger.debug("[SlotMessageStore] Flushed incoming table reader batch to disk", count: count)
        %{state | table_reader_batch_id: batch_id}

      not state.disk_overflow_mode? and state.consumer_id in @disk_overflow_allow_list and
          map_size(state.messages) + length(messages) > state.max_messages_in_memory ->
        count = flush_messages_in_batches!(state, messages)
        Logger.debug("[SlotMessageStore] Flushed incoming table reader batch to disk", count: count)
        Logger.info("[SlotMessageStore] Entering disk overflow mode")
        %{state | table_reader_batch_id: batch_id, disk_overflow_mode?: true}

      true ->
        messages = Enum.map(messages, &%{&1 | table_reader_batch_id: batch_id})
        %{put_messages(state, messages) | table_reader_batch_id: batch_id}
    end
  end

  @spec ack(%State{}, list(SinkConsumer.ack_id())) :: {%State{}, non_neg_integer()}
  def ack(%State{} = state, ack_ids) do
    initial_count = map_size(state.messages)
    messages = Map.drop(state.messages, ack_ids)
    final_count = map_size(messages)

    {%{state | messages: messages}, initial_count - final_count}
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
      # TODO: handle disk overflow?
      nil ->
        {state, nil}

      msg ->
        updated_msg = %{msg | not_visible_until: nil, state: :available, dirty: true}
        {update_messages(state, [updated_msg]), updated_msg}
    end
  end

  @spec reset_all_visibility(%State{}) :: {%State{}, list(ConsumerRecord.t() | ConsumerEvent.t())}
  def reset_all_visibility(%State{} = state) do
    # TODO handle disk overflow?
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

  def deliver_messages(%State{} = state, count) do
    state =
      if state.disk_overflow_mode? and map_size(state.messages) < div(state.max_messages_in_memory, 2) do
        Logger.debug("[SlotMessageStore] Re-loading messages from disk for delivery")

        # First we flush any dirty messages to disk
        state.messages
        |> Map.values()
        |> Stream.filter(& &1.dirty)
        |> then(&flush_messages_in_batches!(state, &1))

        # Then we re-load messages from disk
        load_messages(state)
      else
        state
      end

    %SinkConsumer{} = consumer = state.consumer
    messages = deliverable_messages(state, count)
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

    state = update_messages(state, messages)

    {state, messages}
  end

  defp deliverable_messages(%State{} = state, count) do
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
    |> Enum.sort_by(& &1.seq)
    |> Sequin.Enum.take_until(count, fn msg ->
      not MapSet.member?(undeliverable_group_ids, msg.group_id)
    end)
  end

  def flush_messages(%State{persisted_mode?: true} = state) do
    flushed_at = DateTime.utc_now()
    {messages, more?} = messages_to_flush(state)
    _count = flush_messages_to_postgres!(state, messages)
    messages = Enum.map(messages, fn msg -> %{msg | flushed_at: flushed_at, dirty: false} end)

    {update_messages(state, messages), more?}
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

  def peek_messages(%State{} = state, count) do
    state.messages
    |> Map.values()
    |> Enum.sort_by(& &1.seq)
    |> Enum.take(count)
  end

  def count_messages(%State{messages: messages}), do: map_size(messages)

  defp update_messages(%State{} = state, messages) do
    messages = Map.new(messages, &{&1.ack_id, &1})

    %{state | messages: Map.merge(state.messages, messages)}
  end

  defp load_messages(%State{} = state) do
    {:ok, messages} =
      Sequin.Repo.transaction(fn ->
        now = DateTime.utc_now()
        params = load_params(state)

        state
        |> list_events_or_records(params, timeout: :timer.seconds(45))
        |> Stream.map(fn msg -> %{msg | flushed_at: msg.updated_at, dirty: false, ingested_at: now} end)
        |> Map.new(&{&1.ack_id, &1})
      end)

    disk_overflow_mode? =
      state.consumer_id in @disk_overflow_allow_list and map_size(messages) == state.max_messages_in_memory

    case {state.disk_overflow_mode?, disk_overflow_mode?} do
      {true, false} ->
        Logger.info("[SlotMessageStore] Exiting disk overflow mode")

      {false, true} ->
        Logger.info("[SlotMessageStore] Entering disk overflow mode")

      _ ->
        :ok
    end

    %{state | messages: messages, disk_overflow_mode?: disk_overflow_mode?}
  end

  defp list_events_or_records(%State{} = state, params, opts) when record_messages?(state) do
    Consumers.stream_consumer_records_for_consumer(state.consumer.id, params, opts)
  end

  defp list_events_or_records(%State{} = state, params, opts) when event_messages?(state) do
    Consumers.stream_consumer_events_for_consumer(state.consumer.id, params, opts)
  end

  defp load_params(%State{consumer: %SinkConsumer{id: id}} = state) when id in @disk_overflow_allow_list do
    [limit: state.max_messages_in_memory, order_by: {:asc, :seq}]
  end

  defp load_params(%State{consumer: %SinkConsumer{}}), do: []

  defp messages_to_flush(%State{
         messages: messages,
         flush_batch_size: flush_batch_size,
         flush_wait_ms: flush_wait_ms,
         max_messages_in_memory: max_messages_in_memory
       }) do
    flush_all? = map_size(messages) > max_messages_in_memory

    to_flush =
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

    {to_flush, length(to_flush) == flush_batch_size}
  end

  defp flush_messages_in_batches!(%State{} = state, messages) do
    messages
    |> Stream.chunk_every(state.flush_batch_size)
    |> Enum.reduce(0, fn batch, acc ->
      count = flush_messages_to_postgres!(state, batch)
      acc + count
    end)
  end

  defp flush_messages_to_postgres!(%State{persisted_mode?: true} = state, messages) when event_messages?(state) do
    {:ok, count} = Consumers.upsert_consumer_events(messages)
    count
  end

  defp flush_messages_to_postgres!(%State{persisted_mode?: true} = state, messages) when record_messages?(state) do
    {:ok, count} = Consumers.upsert_consumer_records(messages)
    count
  end

  # Helper function to compare flushed_at values where nil is "smaller" than any DateTime
  # nil is "smaller"
  defp compare_flushed_at(nil, _), do: true
  # anything is "larger" than nil
  defp compare_flushed_at(_, nil), do: false
  defp compare_flushed_at(a, b), do: DateTime.compare(a, b) in [:lt, :eq]
end

defmodule Sequin.DatabasesRuntime.SlotMessageStore.State do
  @moduledoc false
  use TypedStruct

  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.DatabasesRuntime.SlotMessageStore.State
  alias Sequin.Error
  alias Sequin.Replication

  require Logger

  # 1GB
  @default_setting_max_accumulated_payload_bytes 1 * 1024 * 1024 * 1024

  typedstruct do
    field :consumer, SinkConsumer.t()
    field :consumer_id, String.t()
    field :last_consumer_producer_pid, pid() | nil
    field :messages, %{SinkConsumer.ack_id() => ConsumerRecord.t() | ConsumerEvent.t()}, default: %{}
    field :produced_messages, %{SinkConsumer.ack_id() => ConsumerRecord.t() | ConsumerEvent.t()}, default: %{}

    field :setting_max_accumulated_payload_bytes, non_neg_integer(),
      default: @default_setting_max_accumulated_payload_bytes

    field :payload_size_bytes, non_neg_integer(), default: 0
    field :slot_processor_monitor_ref, reference() | nil
    field :table_reader_batch_id, String.t() | nil
    field :test_pid, pid() | nil
  end

  @spec put_messages(%State{}, list(ConsumerRecord.t() | ConsumerEvent.t()) | Enumerable.t()) ::
          {:ok, %State{}} | {:error, Error.t()}
  def put_messages(%State{} = state, messages) do
    now = DateTime.utc_now()

    incoming_payload_size_bytes = Enum.sum_by(messages, & &1.payload_size_bytes)

    if state.payload_size_bytes + incoming_payload_size_bytes > state.setting_max_accumulated_payload_bytes do
      {:error, Error.invariant(message: "Payload size limit exceeded", code: :payload_size_limit_exceeded)}
    else
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

  @spec put_table_reader_batch(%State{}, list(ConsumerRecord.t() | ConsumerEvent.t()), String.t()) ::
          {:ok, %State{}} | {:error, Error.t()}
  def put_table_reader_batch(%State{} = state, messages, batch_id) do
    messages = Stream.map(messages, &%{&1 | table_reader_batch_id: batch_id})

    case put_messages(state, messages) do
      {:ok, %State{} = state} ->
        {:ok, %{state | table_reader_batch_id: batch_id}}

      error ->
        error
    end
  end

  @spec ack(t(), [SinkConsumer.ack_id()]) :: {t(), non_neg_integer(), non_neg_integer()}
  def ack(%State{} = state, ack_ids) do
    initial_count = map_size(state.produced_messages)
    {dropped_messages, produced_messages} = Map.split(state.produced_messages, ack_ids)
    dropped_messages = Map.values(dropped_messages)
    dropped_payload_size_bytes = Enum.sum_by(dropped_messages, & &1.payload_size_bytes)
    final_count = map_size(produced_messages)

    {%{
       state
       | produced_messages: produced_messages,
         payload_size_bytes: state.payload_size_bytes - dropped_payload_size_bytes
     }, dropped_messages, initial_count - final_count}
  end

  @doc """
  Moves all messages from produced_messages to messages.

  Called whenever the ConsumerProducer changes (and therefore is presumed to have crashed.)
  """
  @spec nack_produced_messages(%State{}) :: {%State{}, non_neg_integer()}
  def nack_produced_messages(%State{} = state) do
    nacked_count = map_size(state.produced_messages)
    updated_messages = Map.merge(state.messages, state.produced_messages)

    {%{state | messages: updated_messages, produced_messages: %{}}, nacked_count}
  end

  @spec min_wal_cursor(%State{}) :: Replication.wal_cursor() | nil
  def min_wal_cursor(%State{} = state) do
    min_messages_wal_cursor =
      state.messages
      |> Stream.map(fn {_k, msg} -> {msg.commit_lsn, msg.commit_idx} end)
      |> Enum.min(fn -> nil end)

    min_produced_messages_wal_cursor =
      state.produced_messages
      |> Stream.map(fn {_k, msg} -> {msg.commit_lsn, msg.commit_idx} end)
      |> Enum.min(fn -> nil end)

    min =
      [min_messages_wal_cursor, min_produced_messages_wal_cursor]
      |> Enum.filter(& &1)
      |> Enum.min(fn -> nil end)

    case min do
      nil -> nil
      {commit_lsn, commit_idx} -> %{commit_lsn: commit_lsn, commit_idx: commit_idx}
    end
  end

  def produce_messages(%State{} = state, count) do
    now = Sequin.utc_now()

    messages =
      state
      |> deliverable_messages(count)
      |> Enum.map(fn msg ->
        %{msg | last_delivered_at: now}
      end)

    # Move messages to produced_messages and remove from messages
    produced_messages =
      Map.merge(
        state.produced_messages,
        Map.new(messages, &{&1.ack_id, &1})
      )

    remaining_messages = Map.drop(state.messages, Enum.map(messages, & &1.ack_id))

    state = %{state | messages: remaining_messages, produced_messages: produced_messages}

    {state, messages}
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

      has_batch_messages?(state.produced_messages, batch_id) or has_batch_messages?(state.messages, batch_id) ->
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

  defp deliverable_messages(%State{} = state, count) do
    undeliverable_group_ids =
      state.produced_messages
      |> Stream.map(fn {_k, v} -> v end)
      |> Enum.reduce(MapSet.new(), fn msg, acc ->
        MapSet.put(acc, msg.group_id)
      end)

    state.messages
    |> sorted_message_stream()
    |> Sequin.Enum.take_until(count, fn msg ->
      not MapSet.member?(undeliverable_group_ids, msg.group_id)
    end)
  end

  defp has_batch_messages?(messages, batch_id) do
    messages
    |> Stream.map(fn {_k, v} -> v end)
    |> Enum.any?(&(&1.table_reader_batch_id == batch_id))
  end

  def peek_messages(%State{} = state, count) do
    produced_messages =
      state.produced_messages
      |> sorted_message_stream()
      |> Enum.take(count)

    messages =
      state.messages
      |> sorted_message_stream()
      |> Enum.take(count)

    (produced_messages ++ messages)
    |> Enum.sort_by(&{&1.commit_lsn, &1.commit_idx})
    |> Enum.take(count)
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

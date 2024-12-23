defmodule Sequin.ConsumersRuntime.ConsumerMessageStore do
  @moduledoc """
  A GenServer that manages an in-memory message store for a sink consumer.

  The ConsumerMessageStore serves as a buffer between the replication slot (SlotProcessor) and SinkConsumers. It:

  - Stores messages in memory to improve performance
  - Periodically flushes messages to Postgres for durability
  - Tracks message delivery status and flush state
  - Helps maintain the WAL low watermark by storing undeliverable messages
  - Provides message lookup and filtering capabilities

  Messages are tagged with update and flush events to track their state. The store
  performs both upsert and delete flush operations to Postgres on a regular basis.

  TODOS:
  - [ ] Trap exits, elegant drain
  """
  use GenServer

  alias Sequin.Consumers
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Error

  require Logger

  defguardp event_messages?(state) when state.consumer.message_kind == :event
  defguardp record_messages?(state) when state.consumer.message_kind == :record

  defmodule State do
    @moduledoc false
    use TypedStruct

    alias Sequin.Consumers.ConsumerEvent
    alias Sequin.Consumers.ConsumerRecord

    defguardp event_messages?(state) when state.consumer.message_kind == :event
    defguardp record_messages?(state) when state.consumer.message_kind == :record

    typedstruct do
      field :consumer_id, String.t()
      field :consumer, SinkConsumer.t()
      field :messages, %{[String.t()] => ConsumerRecord.t() | ConsumerEvent.t()}
      # field :flush_timer_ref, reference() | nil
      field :flush_interval, non_neg_integer()
      field :flush_batch_size, non_neg_integer()
    end

    def put_messages(%State{} = state, messages) when event_messages?(state) do
      %{state | messages: Map.merge(state.messages, messages)}
    end

    def put_messages(%State{} = state, messages) when record_messages?(state) do
      {_deletes, upserts} = messages |> Map.values() |> Enum.split_with(& &1.deleted)

      # Split messages into updates and inserts based on existing record_pks
      {updates, inserts} =
        Enum.split_with(upserts, fn msg ->
          Map.has_key?(state.messages, msg.record_pks)
        end)

      updates_by_pk = Map.new(updates, &{&1.record_pks, &1})
      inserts_by_pk = Map.new(inserts, &{&1.record_pks, &1})

      # Update existing messages while preserving state for delivered/pending_redelivery
      updated_messages =
        state.messages
        |> Map.new(fn {pk, existing} ->
          case Map.get(updates_by_pk, pk) do
            nil ->
              {pk, existing}

            updated ->
              # Preserve state if delivered/pending_redelivery, otherwise use new state
              state =
                case existing.state do
                  state when state in [:delivered, :pending_redelivery] ->
                    :pending_redelivery

                  _ ->
                    updated.state || :available
                end

              {pk,
               %{
                 existing
                 | # Most important bit is to update the state. That way when we ack,
                   # if pending_redelivery, we'll redeliver it.
                   # We update commit_lsn and seq to match the latest version of the message
                   state: state,
                   commit_lsn: updated.commit_lsn,
                   seq: updated.seq,

                   # Mark as dirty for next flush
                   dirty: true
               }}
          end
        end)
        # Add new messages
        |> Map.merge(inserts_by_pk)

      %{state | messages: updated_messages}
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
      |> Enum.sort_by(& &1.seq)
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

    def messages_to_flush(%State{messages: messages, flush_batch_size: flush_batch_size}) do
      messages
      |> Map.values()
      |> Enum.filter(& &1.dirty)
      |> Enum.sort_by(& &1.flushed_at, &compare_flushed_at/2)
      |> Enum.take(flush_batch_size)
    end

    def update_messages(%State{} = state, messages) do
      messages = Map.new(messages, &{&1.record_pks, &1})

      %{state | messages: Map.merge(state.messages, messages)}
    end

    # Helper function to compare flushed_at values where nil is "smaller" than any DateTime
    # nil is "smaller"
    defp compare_flushed_at(nil, _), do: true
    # anything is "larger" than nil
    defp compare_flushed_at(_, nil), do: false
    defp compare_flushed_at(a, b), do: DateTime.compare(a, b) in [:lt, :eq]
  end

  @type consumer_id :: String.t()
  @type ack_id :: String.t()
  @type not_visible_until :: DateTime.t()

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    consumer_id = Keyword.fetch!(opts, :consumer_id)
    GenServer.start_link(__MODULE__, opts, name: via_tuple(consumer_id))
  end

  @spec via_tuple(consumer_id()) :: {:via, :syn, {:replication, {module(), consumer_id()}}}
  def via_tuple(consumer_id) do
    {:via, :syn, {:replication, {__MODULE__, consumer_id}}}
  end

  @doc """
  Stores new messages in the message store.
  """
  @spec put_messages(consumer_id(), list(ConsumerRecord.t() | ConsumerEvent.t())) :: :ok
  def put_messages(consumer_id, messages) do
    GenServer.call(via_tuple(consumer_id), {:messages, messages})
  end

  @doc """
  Produces the next batch of deliverable messages, up to the specified count.
  Returns `{:ok, messages}` where messages is a list of deliverable messages.
  """
  @spec produce(consumer_id(), pos_integer()) :: {:ok, list(ConsumerRecord.t() | ConsumerEvent.t())}
  def produce(consumer_id, count) do
    GenServer.call(via_tuple(consumer_id), {:produce, count})
  end

  @doc """
  Acknowledges messages as successfully processed using their ack_ids.
  """
  @spec ack(consumer_id(), list(ack_id())) :: :ok
  def ack(consumer_id, ack_ids) do
    GenServer.call(via_tuple(consumer_id), {:ack, ack_ids})
  end

  @doc """
  Negative acknowledges messages, making them available for redelivery after
  the specified not_visible_until timestamps.
  """
  @spec nack(consumer_id(), %{ack_id() => not_visible_until()}) :: :ok
  def nack(consumer_id, ack_ids_with_not_visible_until) do
    GenServer.call(via_tuple(consumer_id), {:nack, ack_ids_with_not_visible_until})
  end

  @impl GenServer
  def init(opts) do
    state = %State{
      consumer_id: Keyword.fetch!(opts, :consumer_id),
      messages: %{},
      flush_interval: Keyword.get(opts, :flush_interval, :timer.seconds(10)),
      flush_batch_size: Keyword.get(opts, :flush_batch_size, 10_000)
    }

    {:ok, state, {:continue, :init}}
  end

  @impl GenServer
  def handle_continue(:init, %State{} = state) do
    schedule_flush(state)

    case Consumers.get_consumer(state.consumer_id) do
      {:ok, consumer} ->
        messages = load_messages(consumer)
        {:noreply, %{state | messages: messages, consumer: consumer}}

      {:error, %Error.NotFoundError{entity: :consumer}} ->
        Logger.error("[ConsumerMessageStore] Consumer #{state.consumer_id} not found")
        {:stop, :normal, state}
    end
  end

  @impl GenServer
  def handle_call({:messages, messages}, _from, %State{} = state) do
    messages =
      messages
      |> Enum.map(fn msg -> %{msg | dirty: true} end)
      |> Map.new(&{&1.record_pks, &1})

    state = State.put_messages(state, messages)
    :syn.publish(:consumers, {:messages_ingested, state.consumer.id}, :messages_ingested)

    {:reply, :ok, state}
  end

  def handle_call({:produce, count}, _from, %State{} = state) do
    deliverable_messages = State.deliverable_messages(state, count)
    {state, messages} = State.deliver_messages(state, deliverable_messages)

    {:reply, {:ok, messages}, state}
  end

  def handle_call({:ack, ack_ids}, _from, state) do
    # Delete from database right away
    # TODO: We need to respect pending_redelivery on consumer records
    # TODO: We can greatly simplify this call now by just deleting events and records
    Consumers.ack_messages(state.consumer, ack_ids)

    # Delete from in-memory store
    id_set = MapSet.new(ack_ids)

    messages =
      state.messages
      |> Map.values()
      |> Enum.filter(&(not MapSet.member?(id_set, &1.ack_id)))
      |> Map.new(&{&1.record_pks, &1})

    {:reply, :ok, %{state | messages: messages}}
  end

  def handle_call({:nack, ack_ids_with_not_visible_until}, _from, state) do
    # Update messages with matching ack_ids to set their not_visible_until and state
    updated_messages =
      Map.new(state.messages, fn {record_pks, msg} ->
        if not_visible_until = ack_ids_with_not_visible_until[msg.ack_id] do
          msg = %{msg | not_visible_until: not_visible_until, state: :available, dirty: true}
          {record_pks, msg}
        else
          {record_pks, msg}
        end
      end)

    {:reply, :ok, %{state | messages: updated_messages}}
  end

  @impl GenServer
  def handle_info(:flush, state) do
    to_flush = State.messages_to_flush(state)
    more? = length(to_flush) == state.flush_batch_size

    state = flush_messages(state, to_flush)

    if more? do
      Process.send_after(self(), :flush, 0)
    else
      schedule_flush(state)
    end

    {:noreply, state}
  end

  defp flush_messages(%State{} = state, messages) when event_messages?(state) do
    Consumers.upsert_consumer_events(messages)
    messages = Enum.map(messages, fn msg -> %{msg | flushed_at: DateTime.utc_now(), dirty: false} end)
    state = State.update_messages(state, messages)
    state
  end

  defp flush_messages(%State{} = state, messages) when record_messages?(state) do
    Consumers.upsert_consumer_records(messages)
    messages = Enum.map(messages, fn msg -> %{msg | flushed_at: DateTime.utc_now(), dirty: false} end)
    state = State.update_messages(state, messages)
    state
  end

  defp schedule_flush(%State{} = state) do
    Process.send_after(self(), :flush, state.flush_interval)
  end

  defp load_messages(%SinkConsumer{message_kind: :event, id: id}) do
    id
    |> Consumers.list_consumer_events_for_consumer()
    |> Enum.map(fn msg -> %{msg | flushed_at: DateTime.utc_now(), dirty: false} end)
    |> Map.new(&{&1.record_pks, &1})
  end

  defp load_messages(%SinkConsumer{message_kind: :record, id: id}) do
    id
    |> Consumers.list_consumer_records_for_consumer()
    |> Enum.map(fn msg -> %{msg | flushed_at: DateTime.utc_now(), dirty: false} end)
    |> Map.new(&{&1.record_pks, &1})
  end
end

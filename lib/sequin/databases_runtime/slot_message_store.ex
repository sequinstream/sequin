defmodule Sequin.DatabasesRuntime.SlotMessageStore do
  @moduledoc """
  A GenServer that manages an in-memory message store for a sink consumer.

  The SlotMessageStore serves as a buffer between the replication slot (SlotProcessor) and SinkConsumers. It:

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
  alias Sequin.Health
  alias Sequin.Metrics

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
      field :test_pid, pid() | nil
    end

    def put_messages(%State{} = state, messages) when event_messages?(state) do
      %{state | messages: Map.merge(state.messages, messages)}
    end

    def put_messages(%State{} = state, messages) when record_messages?(state) do
      {_deletes, upserts} = messages |> Map.values() |> Enum.split_with(& &1.deleted)

      existing_record_pks_to_ack_id =
        Map.new(state.messages, fn {ack_id, %ConsumerRecord{record_pks: record_pks}} ->
          {record_pks, ack_id}
        end)

      # Split messages into updates and inserts based on existing ack_ids
      {existing_messages, new_messages} =
        Enum.split_with(upserts, &Map.has_key?(existing_record_pks_to_ack_id, &1.record_pks))

      existing_messages =
        existing_messages
        |> Enum.group_by(& &1.record_pks)
        |> Enum.map(fn {_record_pks, updates} ->
          Enum.max_by(updates, & &1.commit_lsn)
        end)

      new_messages =
        new_messages
        |> Enum.group_by(& &1.record_pks)
        |> Map.new(fn {_record_pks, inserts} ->
          last_insert = Enum.max_by(inserts, & &1.commit_lsn)
          {last_insert.ack_id, last_insert}
        end)

      # Update existing messages while preserving state for delivered/pending_redelivery
      updated_messages =
        existing_messages
        |> Enum.reduce(state.messages, fn updated, acc_msgs ->
          ack_id = Map.fetch!(existing_record_pks_to_ack_id, updated.record_pks)
          existing = Map.fetch!(acc_msgs, ack_id)

          # Preserve state if delivered/pending_redelivery, otherwise use new state
          state =
            case existing.state do
              state when state in [:delivered, :pending_redelivery] ->
                :pending_redelivery

              _ ->
                updated.state || :available
            end

          updated_message = %ConsumerRecord{
            existing
            | # Most important bit is to update the state. That way when we ack,
              # if pending_redelivery, we'll redeliver it.
              # We update commit_lsn and seq to match the latest version of the message
              state: state,
              data: updated.data,
              commit_lsn: updated.commit_lsn,
              seq: updated.seq,

              # Mark as dirty for next flush
              dirty: true
          }

          Map.replace!(acc_msgs, ack_id, updated_message)
        end)
        |> Map.merge(new_messages)

      %{state | messages: updated_messages}
    end

    # TODO: implement group_id for message_kind: :event
    def deliverable_messages(%State{} = state, count) when event_messages?(state) do
      now = DateTime.utc_now()

      state.messages
      |> Map.values()
      |> Enum.sort_by(& &1.seq)
      |> Sequin.Enum.take_until(count, fn msg ->
        is_nil(msg.not_visible_until) or DateTime.before?(msg.not_visible_until, now)
      end)
    end

    def deliverable_messages(%State{} = state, count) when record_messages?(state) do
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
    GenServer.call(via_tuple(consumer_id), {:put_messages, messages})
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
  @spec ack(SinkConsumer.t(), list(ack_id())) :: :ok
  def ack(consumer, ack_ids) do
    # Delete from database right away
    # TODO: We need to respect pending_redelivery on consumer records
    # TODO: We can greatly simplify this call now by just deleting events and records
    Consumers.ack_messages(consumer, ack_ids)

    GenServer.call(via_tuple(consumer.id), {:ack, ack_ids})
  end

  @doc """
  Negative acknowledges messages, making them available for redelivery after
  the specified not_visible_until timestamps.
  """
  @spec nack(consumer_id(), %{ack_id() => not_visible_until()}) :: :ok
  def nack(consumer_id, ack_ids_with_not_visible_until) do
    GenServer.call(via_tuple(consumer_id), {:nack, ack_ids_with_not_visible_until})
  end

  def child_spec(opts) do
    %{
      id: via_tuple(opts[:consumer_id]),
      start: {__MODULE__, :start_link, [opts]}
    }
  end

  @impl GenServer
  def init(opts) do
    state = %State{
      consumer_id: Keyword.fetch!(opts, :consumer_id),
      messages: %{},
      flush_interval: Keyword.get(opts, :flush_interval, :timer.seconds(10)),
      flush_batch_size: Keyword.get(opts, :flush_batch_size, 1_000),
      test_pid: Keyword.get(opts, :test_pid)
    }

    {:ok, state, {:continue, :init}}
  end

  @impl GenServer
  def handle_continue(:init, %State{} = state) do
    # Allow test process to access the database connection
    if state.test_pid do
      Ecto.Adapters.SQL.Sandbox.allow(Sequin.Repo, state.test_pid, self())
    end

    schedule_flush(state)

    case Consumers.get_consumer(state.consumer_id) do
      {:ok, consumer} ->
        Logger.info("[SlotMessageStore] Loading messages...")
        {time, messages} = :timer.tc(fn -> load_messages(consumer) end)
        Logger.info("[SlotMessageStore] Loaded #{map_size(messages)} messages in #{div(time, 1000)}ms")

        {:noreply, %{state | messages: messages, consumer: consumer}}

      {:error, %Error.NotFoundError{entity: :consumer}} ->
        Logger.error("[SlotMessageStore] Consumer #{state.consumer_id} not found")
        {:stop, :normal, state}
    end
  end

  @impl GenServer
  def handle_call({:put_messages, messages}, _from, %State{} = state) do
    {time, state} =
      :timer.tc(fn ->
        messages =
          messages
          |> Stream.map(fn msg -> %{msg | ack_id: UUID.uuid4(), dirty: true} end)
          |> Map.new(&{&1.ack_id, &1})

        _state = State.put_messages(state, messages)
      end)

    Logger.info(
      "[SlotMessageStore] Put #{length(messages)} messages in #{div(time, 1000)}ms (message_count=#{map_size(state.messages)})"
    )

    Health.update(state.consumer, :ingestion, :healthy)
    :syn.publish(:consumers, {:messages_ingested, state.consumer.id}, :messages_ingested)

    {:reply, :ok, state}
  end

  def handle_call({:produce, count}, _from, %State{} = state) do
    {time, {state, messages}} =
      :timer.tc(fn ->
        deliverable_messages = State.deliverable_messages(state, count)
        {_state, _messages} = State.deliver_messages(state, deliverable_messages)
      end)

    Logger.info(
      "[SlotMessageStore] Produced #{length(messages)} messages in #{div(time, 1000)}ms (message_count=#{map_size(state.messages)})"
    )

    Health.update(state.consumer, :receive, :healthy)

    {:reply, {:ok, messages}, state}
  end

  def handle_call({:ack, []}, _from, state) do
    {:reply, :ok, state}
  end

  def handle_call({:ack, ack_ids}, _from, state) do
    {time, {messages, acked_count}} =
      :timer.tc(fn ->
        {messages, acked_count} =
          Enum.reduce(ack_ids, {state.messages, 0}, fn ack_id, {acc_msgs, acked_count} ->
            case Map.get(acc_msgs, ack_id) do
              nil ->
                {acc_msgs, acked_count}

              %ConsumerRecord{state: :pending_redelivery} = msg ->
                {Map.replace!(acc_msgs, ack_id, %{
                   msg
                   | state: :available,
                     not_visible_until: nil,
                     dirty: true
                 }), acked_count + 1}

              _ ->
                {Map.delete(acc_msgs, ack_id), acked_count + 1}
            end
          end)

        Health.update(state.consumer, :acknowledge, :healthy)
        Metrics.incr_consumer_messages_processed_count(state.consumer, acked_count)
        Metrics.incr_consumer_messages_processed_throughput(state.consumer, acked_count)

        {messages, acked_count}
      end)

    Logger.debug(
      "[SlotMessageStore] Acked #{acked_count} messages in #{div(time, 1000)}ms (message_count=#{map_size(messages)})"
    )

    {:reply, :ok, %{state | messages: messages}}
  end

  def handle_call({:nack, ack_ids_with_not_visible_until}, _from, state) do
    updated_messages =
      Enum.reduce(ack_ids_with_not_visible_until, state.messages, fn {ack_id, not_visible_until}, acc_msgs ->
        if msg = Map.get(acc_msgs, ack_id) do
          msg = %{msg | not_visible_until: not_visible_until, state: :available, dirty: true}
          Map.replace(acc_msgs, ack_id, msg)
        else
          acc_msgs
        end
      end)

    {:reply, :ok, %{state | messages: updated_messages}}
  end

  @impl GenServer
  def handle_info(:flush, state) do
    to_flush = State.messages_to_flush(state)
    more? = length(to_flush) == state.flush_batch_size

    state = flush_messages(state, to_flush)
    Logger.info("[SlotMessageStore] Flushed #{length(to_flush)} messages")

    if more? do
      Process.send_after(self(), :flush, 0)
    else
      Logger.info("[SlotMessageStore] No more messages to flush")
      schedule_flush(state)
    end

    {:noreply, state}
  end

  defp flush_messages(%State{} = state, messages) when event_messages?(state) do
    {:ok, _count} = Consumers.upsert_consumer_events(messages)

    flushed_at = DateTime.utc_now()
    messages = Enum.map(messages, fn msg -> %{msg | flushed_at: flushed_at, dirty: false} end)

    State.update_messages(state, messages)
  rescue
    error in [Postgrex.Error] ->
      # unique violation
      if error.postgres.code == :unique_violation do
        # Extract the ack_id from the error message
        ack_id = extract_ack_id_from_error(error)

        # Get in-memory message state
        in_memory_msg = Map.get(state.messages, ack_id)

        # Query Postgres for message state
        db_msg = Consumers.get_consumer_event(state.consumer_id, ack_id: ack_id)

        Logger.error("""
        Duplicate key violation during message flush
        ack_id: #{ack_id}
        in_memory_state: #{inspect(in_memory_msg)}
        database_state: #{inspect(db_msg)}
        error: #{inspect(error)}
        """)
      end

      reraise error, __STACKTRACE__
  end

  defp flush_messages(%State{} = state, messages) when record_messages?(state) do
    {:ok, _count} = Consumers.upsert_consumer_records(messages)

    flushed_at = DateTime.utc_now()
    messages = Enum.map(messages, fn msg -> %{msg | flushed_at: flushed_at, dirty: false} end)

    State.update_messages(state, messages)
  end

  defp schedule_flush(%State{} = state) do
    Process.send_after(self(), :flush, state.flush_interval)
  end

  defp load_messages(%SinkConsumer{message_kind: :event, id: id}) do
    id
    |> Consumers.list_consumer_events_for_consumer()
    |> Enum.map(fn msg -> %{msg | flushed_at: DateTime.utc_now(), dirty: false} end)
    |> Map.new(&{&1.ack_id, &1})
  end

  defp load_messages(%SinkConsumer{message_kind: :record, id: id}) do
    id
    |> Consumers.list_consumer_records_for_consumer()
    |> Enum.map(fn msg -> %{msg | flushed_at: DateTime.utc_now(), dirty: false} end)
    |> Map.new(&{&1.ack_id, &1})
  end

  # Helper to extract ack_id from Postgres error message
  defp extract_ack_id_from_error(error) do
    case Regex.run(~r/ack_id\)=\([^,]+, ([^)]+)\)/, Exception.message(error)) do
      [_, ack_id] -> String.trim(ack_id)
      _ -> "unknown"
    end
  end
end

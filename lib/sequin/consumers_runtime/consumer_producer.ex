defmodule Sequin.ConsumersRuntime.ConsumerProducer do
  @moduledoc false
  @behaviour Broadway.Producer

  use GenStage

  alias Broadway.Message
  alias Ecto.Adapters.SQL.Sandbox
  alias Sequin.Consumers
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerEventData
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.ConsumerRecordData
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.ConsumersRuntime.MessageLedgers
  alias Sequin.DatabasesRuntime.SlotMessageStore
  alias Sequin.Health
  alias Sequin.Health.Event
  alias Sequin.Postgres
  alias Sequin.Repo

  require Logger

  @min_log_time_ms 100

  @impl GenStage
  def init(opts) do
    consumer = Keyword.fetch!(opts, :consumer)
    slot_message_store_mod = Keyword.get(opts, :slot_message_store_mod, SlotMessageStore)
    Logger.metadata(consumer_id: consumer.id)
    Logger.info("Initializing consumer producer")

    if test_pid = Keyword.get(opts, :test_pid) do
      Sandbox.allow(Sequin.Repo, test_pid, self())
      Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
      Mox.allow(Sequin.DatabasesRuntime.SlotMessageStoreMock, test_pid, self())
    end

    :syn.join(:consumers, {:messages_ingested, consumer.id}, self())

    state = %{
      demand: 0,
      consumer: consumer,
      receive_timer: nil,
      trim_timer: nil,
      batch_size: Keyword.get(opts, :batch_size, 10),
      batch_timeout: Keyword.get(opts, :batch_timeout, :timer.seconds(10)),
      test_pid: test_pid,
      scheduled_handle_demand: false,
      slot_message_store_mod: slot_message_store_mod
    }

    Process.send_after(self(), :init, 0)

    {:producer, state}
  end

  @impl GenStage
  def handle_demand(incoming_demand, %{demand: demand} = state) do
    new_state = maybe_schedule_demand(state)
    new_state = %{new_state | demand: demand + incoming_demand}

    {:noreply, [], new_state}
  end

  @impl GenStage
  def handle_info(:init, state) do
    consumer = Repo.lazy_preload(state.consumer, postgres_database: [:replication_slot])

    state =
      state
      |> schedule_receive_messages()
      |> schedule_trim_idempotency()

    {:noreply, [], %{state | consumer: consumer}}
  end

  @impl GenStage
  def handle_info(:handle_demand, state) do
    handle_receive_messages(%{state | scheduled_handle_demand: false})
  end

  @impl GenStage
  def handle_info(:receive_messages, state) do
    new_state = schedule_receive_messages(state)
    handle_receive_messages(new_state)
  end

  @impl GenStage
  def handle_info(:messages_ingested, state) do
    new_state = maybe_schedule_demand(state)
    {:noreply, [], new_state}
  end

  @impl GenStage
  def handle_info(:trim_idempotency, state) do
    %SinkConsumer{} = consumer = state.consumer

    case Postgres.confirmed_flush_lsn(consumer.postgres_database) do
      {:ok, nil} ->
        :ok

      {:ok, lsn} ->
        MessageLedgers.trim_delivered_cursors_set(state.consumer.id, %{commit_lsn: lsn, commit_idx: 0})

      {:error, error} when is_exception(error) ->
        Logger.error("Error trimming idempotency seqs", error: Exception.message(error))

      {:error, error} ->
        Logger.error("Error trimming idempotency seqs", error: inspect(error))
    end

    {:noreply, [], schedule_trim_idempotency(state)}
  end

  defp handle_receive_messages(%{demand: demand} = state) when demand > 0 do
    desired_count = demand * state.batch_size * 10
    {time, messages} = :timer.tc(fn -> produce_messages(state, desired_count) end)
    more_upstream_messages? = length(messages) == desired_count

    if div(time, 1000) > @min_log_time_ms do
      Logger.warning(
        "[ConsumerProducer] produce_messages took longer than expected",
        count: length(messages),
        demand: demand,
        batch_size: state.batch_size,
        duration_ms: div(time, 1000)
      )
    end

    {time, messages} = :timer.tc(fn -> reject_delivered_messages(state, messages) end)

    if div(time, 1000) > @min_log_time_ms do
      Logger.warning(
        "[ConsumerProducer] reject_delivered_messages took longer than expected",
        duration_ms: div(time, 1000),
        message_count: length(messages)
      )
    end

    broadway_messages =
      messages
      |> Enum.chunk_every(state.batch_size)
      |> Enum.map(fn batch ->
        %Message{
          data: batch,
          acknowledger: {__MODULE__, {state.consumer.id, state.test_pid, state.slot_message_store_mod}, nil}
        }
      end)

    new_demand = demand - length(broadway_messages)
    new_demand = if new_demand < 0, do: 0, else: new_demand
    state = %{state | demand: new_demand}

    if new_demand > 0 and more_upstream_messages? do
      {:noreply, broadway_messages, maybe_schedule_demand(state)}
    else
      {:noreply, broadway_messages, state}
    end
  end

  defp handle_receive_messages(state) do
    {:noreply, [], state}
  end

  defp produce_messages(state, count) do
    consumer = state.consumer

    case state.slot_message_store_mod.produce(consumer.id, count, self()) do
      {:ok, messages} ->
        unless messages == [] do
          Health.put_event(consumer, %Event{slug: :messages_pending_delivery, status: :success})
        end

        messages

      {:error, _error} ->
        []
    end
  end

  defp reject_delivered_messages(state, messages) do
    wal_cursors_to_deliver =
      messages
      |> Stream.reject(fn
        # We don't enforce idempotency for read actions
        %ConsumerEvent{data: %ConsumerEventData{action: :read}} -> true
        %ConsumerRecord{data: %ConsumerRecordData{action: :read}} -> true
        # We only recently added :action to ConsumerRecordData, so we need to ignore
        # any messages that don't have it for backwards compatibility
        %ConsumerRecord{data: %ConsumerRecordData{action: nil}} -> true
        _ -> false
      end)
      |> Enum.map(fn message -> %{commit_lsn: message.commit_lsn, commit_idx: message.commit_idx} end)

    {:ok, delivered_wal_cursors} =
      MessageLedgers.filter_delivered_wal_cursors(state.consumer.id, wal_cursors_to_deliver)

    delivered_cursor_set = MapSet.new(delivered_wal_cursors)

    {delivered_messages, filtered_messages} =
      Enum.split_with(messages, fn message ->
        MapSet.member?(delivered_cursor_set, %{commit_lsn: message.commit_lsn, commit_idx: message.commit_idx})
      end)

    if delivered_messages == [] do
      filtered_messages
    else
      Logger.info(
        "[ConsumerProducer] Rejected messages for idempotency",
        rejected_message_count: length(delivered_messages),
        commits: delivered_wal_cursors,
        message_count: length(filtered_messages)
      )

      state.slot_message_store_mod.messages_already_succeeded(
        state.consumer.id,
        Enum.map(delivered_messages, & &1.ack_id)
      )

      filtered_messages
    end
  end

  defp schedule_receive_messages(state) do
    receive_timer = Process.send_after(self(), :receive_messages, state.batch_timeout)
    %{state | receive_timer: receive_timer}
  end

  defp schedule_trim_idempotency(state) do
    trim_timer = Process.send_after(self(), :trim_idempotency, :timer.seconds(30))
    %{state | trim_timer: trim_timer}
  end

  @impl Broadway.Producer
  def prepare_for_draining(%{receive_timer: receive_timer, trim_timer: trim_timer} = state) do
    if receive_timer, do: Process.cancel_timer(receive_timer)
    if trim_timer, do: Process.cancel_timer(trim_timer)
    {:noreply, [], %{state | receive_timer: nil, trim_timer: nil}}
  end

  @spec ack({SinkConsumer.t(), pid(), slot_message_store_mod :: atom()}, list(Message.t()), list(Message.t())) ::
          :ok
  def ack({consumer_id, test_pid, slot_message_store_mod}, successful, failed) do
    successful_messages = Enum.flat_map(successful, & &1.data)
    failed_messages = Enum.flat_map(failed, & &1.data)

    # TODO: Can we remove, this should be happening in the processor?
    if test_pid do
      Sandbox.allow(Sequin.Repo, test_pid, self())
    end

    # First, handle the WAL cursors for successful messages
    wal_cursors =
      Enum.map(successful_messages, fn message -> %{commit_lsn: message.commit_lsn, commit_idx: message.commit_idx} end)

    MessageLedgers.wal_cursors_reached_checkpoint(consumer_id, "consumer_producer.ack", wal_cursors)

    :ok = MessageLedgers.wal_cursors_delivered(consumer_id, wal_cursors)

    successful_ack_ids = Enum.map(successful_messages, & &1.ack_id)

    # Ack all messages in SlotMessageStore to remove from buffer
    unless successful_ack_ids == [] do
      {:ok, _count} = slot_message_store_mod.messages_succeeded(consumer_id, successful_ack_ids)
    end

    failed_message_metadatas =
      failed_messages
      |> Stream.map(&Consumers.advance_delivery_state_for_failure/1)
      |> Enum.map(&%{&1 | data: nil})

    unless failed_message_metadatas == [] do
      :ok = slot_message_store_mod.messages_failed(consumer_id, failed_message_metadatas)
    end

    if test_pid do
      successful_ids = successful |> Stream.flat_map(& &1.data) |> Enum.map(& &1.ack_id)
      failed_ids = failed |> Stream.flat_map(& &1.data) |> Enum.map(& &1.ack_id)
      send(test_pid, {__MODULE__, :ack_finished, successful_ids, failed_ids})
    end

    :ok
  end

  defp maybe_schedule_demand(%{scheduled_handle_demand: false} = state) do
    Process.send_after(self(), :handle_demand, 10)
    %{state | scheduled_handle_demand: true}
  end

  defp maybe_schedule_demand(state), do: state
end

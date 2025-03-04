defmodule Sequin.Runtime.ConsumerProducer do
  @moduledoc false
  @behaviour Broadway.Producer

  use GenStage

  alias Broadway.Message
  alias Ecto.Adapters.SQL.Sandbox
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerEventData
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.ConsumerRecordData
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Health
  alias Sequin.Health.Event
  alias Sequin.Postgres
  alias Sequin.Repo
  alias Sequin.Runtime.MessageLedgers
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Runtime.SlotMessageStore

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
      Mox.allow(Sequin.Runtime.SlotMessageStoreMock, test_pid, self())
    end

    :syn.join(:consumers, {:messages_ingested, consumer.id}, self())

    state = %{
      demand: 0,
      consumer: consumer,
      receive_timer: nil,
      trim_timer: nil,
      batch_timeout: Keyword.get(opts, :batch_timeout, :timer.seconds(10)),
      test_pid: test_pid,
      scheduled_handle_demand: false,
      slot_message_store_mod: slot_message_store_mod,
      last_logged_stats_at: nil
    }

    Process.send_after(self(), :init, 0)
    schedule_process_logging(0)

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

  @impl GenStage
  def handle_info(:process_logging, state) do
    now = System.monotonic_time(:millisecond)
    interval_ms = if state.last_logged_stats_at, do: now - state.last_logged_stats_at

    info =
      Process.info(self(), [
        :memory,
        :message_queue_len
      ])

    metadata = [
      memory_mb: Float.round(info[:memory] / 1_024 / 1_024, 2),
      message_queue_len: info[:message_queue_len],
      demand: state.demand
    ]

    # Get all timing metrics from process dictionary
    timing_metrics =
      Process.get()
      |> Enum.filter(fn {key, _} ->
        key |> to_string() |> String.ends_with?("_total_ms")
      end)
      |> Keyword.new()

    # Log all timing metrics as histograms with operation tag
    Enum.each(timing_metrics, fn {key, value} ->
      operation = key |> to_string() |> String.replace("_total_ms", "")

      Sequin.Statsd.histogram("sequin.consumer_producer.operation_time_ms", value,
        tags: %{
          consumer_id: state.consumer.id,
          operation: operation
        }
      )
    end)

    unaccounted_ms =
      if interval_ms do
        # Calculate total accounted time
        total_accounted_ms = Enum.reduce(timing_metrics, 0, fn {_key, value}, acc -> acc + value end)

        # Calculate unaccounted time
        max(0, interval_ms - total_accounted_ms)
      end

    if unaccounted_ms do
      # Log unaccounted time with same metric but different operation tag
      Sequin.Statsd.histogram("sequin.consumer_producer.operation_time_ms", unaccounted_ms,
        tags: %{
          consumer_id: state.consumer.id,
          operation: "unaccounted"
        }
      )
    end

    metadata =
      metadata
      |> Keyword.merge(timing_metrics)
      |> Sequin.Keyword.put_if_present(:unaccounted_total_ms, unaccounted_ms)

    Logger.info("[ConsumerProducer] Process metrics", metadata)

    # Clear timing metrics after logging
    timing_metrics
    |> Keyword.keys()
    |> Enum.each(&clear_counter/1)

    schedule_process_logging()
    {:noreply, [], %{state | last_logged_stats_at: now}}
  end

  defp handle_receive_messages(%{demand: demand} = state) when demand > 0 do
    execute_timed(:handle_receive_messages, fn ->
      {time, messages} = :timer.tc(fn -> produce_messages(state, demand) end)
      more_upstream_messages? = length(messages) == demand

      if div(time, 1000) > @min_log_time_ms do
        Logger.warning(
          "[ConsumerProducer] produce_messages took longer than expected",
          count: length(messages),
          demand: demand,
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

      # Cut this struct down as it will be passed to each process
      # Processes already have the consumer in context, but this is for the acknowledger. When we
      # consolidate pipelines, we can `configure_ack` to add the consumer to the acknowledger context.
      bare_consumer =
        Map.drop(state.consumer, [
          :source_tables,
          :active_backfill,
          :sequence,
          :postgres_database,
          :replication_slot,
          :account
        ])

      broadway_messages =
        Enum.map(messages, fn message ->
          %Message{
            data: message,
            acknowledger: {SinkPipeline, {bare_consumer, state.test_pid, state.slot_message_store_mod}, nil}
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
    end)
  end

  defp handle_receive_messages(state) do
    {:noreply, [], state}
  end

  defp produce_messages(state, count) do
    execute_timed(:produce_messages, fn ->
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
    end)
  end

  defp reject_delivered_messages(state, messages) do
    execute_timed(:reject_delivered_messages, fn ->
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

      :ok = MessageLedgers.wal_cursors_delivered(state.consumer.id, delivered_wal_cursors)

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
    end)
  end

  defp schedule_receive_messages(state) do
    receive_timer = Process.send_after(self(), :receive_messages, state.batch_timeout)
    %{state | receive_timer: receive_timer}
  end

  defp schedule_trim_idempotency(state) do
    trim_timer = Process.send_after(self(), :trim_idempotency, :timer.seconds(10))
    %{state | trim_timer: trim_timer}
  end

  @impl Broadway.Producer
  def prepare_for_draining(%{receive_timer: receive_timer, trim_timer: trim_timer} = state) do
    if receive_timer, do: Process.cancel_timer(receive_timer)
    if trim_timer, do: Process.cancel_timer(trim_timer)
    {:noreply, [], %{state | receive_timer: nil, trim_timer: nil}}
  end

  defp maybe_schedule_demand(%{scheduled_handle_demand: false} = state) do
    Process.send_after(self(), :handle_demand, 10)
    %{state | scheduled_handle_demand: true}
  end

  defp maybe_schedule_demand(state), do: state

  defp schedule_process_logging(interval \\ :timer.seconds(30)) do
    Process.send_after(self(), :process_logging, interval)
  end

  defp incr_counter(name, amount) do
    current = Process.get(name, 0)
    Process.put(name, current + amount)
  end

  defp clear_counter(name) do
    Process.delete(name)
  end

  defp execute_timed(name, fun) do
    {time, result} = :timer.tc(fun)
    # Convert microseconds to milliseconds
    incr_counter(:"#{name}_total_ms", div(time, 1000))
    result
  end
end

defmodule Sequin.Runtime.SlotMessageHandler do
  @moduledoc """
  A GenServer implementation of the MessageHandler that processes messages in partitions.

  This module implements the MessageHandlerBehaviour interface and is designed to be
  started in multiple partitions to distribute the processing load.

  TODO:
  - Needs to be able to enter a "back off" state where you can't push any more messages to it.
  """

  use GenServer

  alias Sequin.Error
  alias Sequin.Health
  alias Sequin.Replication
  alias Sequin.Repo
  alias Sequin.Runtime.MessageHandler
  alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.LogicalMessage
  alias Sequin.Runtime.SlotProcessor.Message
  alias Sequin.Runtime.TableReaderServer

  require Logger

  @callback before_handle_messages(context :: any(), messages :: [Message.t()]) ::
              :ok | {:error, reason :: any()}

  @callback handle_messages(slot_id :: String.t(), processor_count :: non_neg_integer(), messages :: [Message.t()]) ::
              :ok | {:error, reason :: any()}

  @callback handle_logical_message(context :: any(), seq :: non_neg_integer(), message :: LogicalMessage.t()) ::
              :ok | {:error, reason :: any()}

  @callback reload_entities(slot_id :: String.t(), processor_count :: non_neg_integer()) ::
              :ok | {:error, reason :: any()}

  @callback flush_messages(slot_id :: String.t(), processor_count :: non_neg_integer()) ::
              :ok | {:error, reason :: any()}

  defdelegate before_handle_messages(context, messages), to: MessageHandler
  defdelegate handle_logical_message(context, seq, message), to: MessageHandler

  defmodule State do
    @moduledoc false
    use TypedStruct

    alias Sequin.Consumers.SinkConsumer
    alias Sequin.Databases.PostgresDatabase
    alias Sequin.Replication.PostgresReplicationSlot
    alias Sequin.Replication.WalPipeline

    typedstruct do
      field :id, PostgresReplicationSlot.id()
      field :processor_idx, non_neg_integer()
      field :processor_count, non_neg_integer()
      field :last_logged_stats_at, non_neg_integer()

      # Entities
      field :replication_slot, PostgresReplicationSlot.t()
      field :consumers, [SinkConsumer.t()], default: []
      field :wal_pipelines, [WalPipeline.t()], default: []
      field :postgres_database, PostgresDatabase.t()

      field :table_reader_mod, module()
    end
  end

  # Client API

  @doc """
  Starts a new SlotMessageHandler process.

  ## Options

  * `:context` - The MessageHandler.Context to use
  * `:processor_idx` - The ID of this partition (0-based)
  * `:processor_count` - The total number of partitions
  """
  def start_link(opts) do
    replication_slot_id = Keyword.fetch!(opts, :replication_slot_id)
    processor_idx = Keyword.fetch!(opts, :processor_idx)

    name = via_tuple(replication_slot_id, processor_idx)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Returns a child specification for starting a SlotMessageHandler under a supervisor.
  """
  def child_spec(opts) do
    _id = Keyword.fetch!(opts, :replication_slot_id)
    processor_idx = Keyword.fetch!(opts, :processor_idx)

    %{
      id: {__MODULE__, processor_idx},
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
      shutdown: 5000,
      type: :worker
    }
  end

  @doc """
  Returns the via tuple for the given replication_slot_id and processor_idx.
  """
  def via_tuple(replication_slot_id, processor_idx) do
    {:via, :syn, {:replication, {__MODULE__, replication_slot_id, processor_idx}}}
  end

  @doc """
  Handles a batch of messages.

  Implements the MessageHandlerBehaviour.handle_messages/2 callback.
  """
  def handle_messages(_slot_id, _processor_count, []) do
    :ok
  end

  def handle_messages(slot_id, processor_count, messages) do
    # Determine which partition to send to based on a hash of the message
    messages_by_partition = partition_messages(messages, processor_count)

    # Send messages to each partition
    Sequin.Enum.reduce_while_ok(messages_by_partition, fn {processor_idx, partition_messages} ->
      name = via_tuple(slot_id, processor_idx)
      GenServer.call(name, {:handle_messages, partition_messages}, :timer.seconds(30))
    end)
  end

  def reload_entities(slot_id, processor_count) do
    Sequin.Enum.reduce_while_ok(0..(processor_count - 1), fn processor_idx ->
      name = via_tuple(slot_id, processor_idx)
      GenServer.call(name, :load_entities)
    end)
  end

  @doc """
  Flushes all message handlers for the given slot.

  This ensures that all SlotMessageHandlers have finished processing their messages.
  Upstream callers can use this to ensure all handlers are done processing.
  """
  def flush_messages(slot_id, processor_count) do
    Sequin.Enum.reduce_while_ok(0..(processor_count - 1), fn processor_idx ->
      name = via_tuple(slot_id, processor_idx)
      GenServer.call(name, :flush)
    end)
  end

  # Server callbacks

  @impl GenServer
  def init(opts) do
    replication_slot_id = Keyword.fetch!(opts, :replication_slot_id)
    processor_idx = Keyword.fetch!(opts, :processor_idx)

    Logger.metadata(
      replication_slot_id: replication_slot_id,
      processor_idx: processor_idx
    )

    Logger.info("[SlotMessageHandler] Starting partition #{processor_idx} for slot #{replication_slot_id}")

    state = %State{
      id: replication_slot_id,
      processor_idx: processor_idx,
      table_reader_mod: Keyword.get(opts, :table_reader_mod, TableReaderServer)
    }

    schedule_process_logging(0)

    {:ok, state, {:continue, :load_entities}}
  end

  @impl GenServer
  def handle_continue(:load_entities, %State{} = state) do
    state = load_entities(state)
    {:noreply, state}
  end

  @impl GenServer
  def handle_call(:load_entities, _from, %State{} = state) do
    state = load_entities(state)
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_call(:flush, _from, %State{} = state) do
    Logger.debug("[SlotMessageHandler] Flushing message handler for slot #{state.id}, partition #{state.processor_idx}")
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_call({:handle_messages, messages}, from, %State{} = state) do
    # Reply immediately with a placeholder result
    # This allows the caller to continue without waiting for processing
    GenServer.reply(from, :ok)

    # Process messages asynchronously
    Logger.info("[SlotMessageHandler] Handling #{length(messages)} message(s) in partition #{state.processor_idx}")

    case MessageHandler.handle_messages(context(state), messages) do
      {:ok, _count} ->
        {:noreply, state}

      {:error, %Error.InvariantError{code: :payload_size_limit_exceeded}} ->
        error =
          Error.service(
            message:
              "One or more of your sinks has exceeded memory limitations for buffered messages. Sequin has stopped processing new messages from your replication slot until the sink(s) drain messages.",
            service: :postgres_replication_slot,
            code: :payload_size_limit_exceeded
          )

        Logger.warning(Exception.message(error))

        Health.put_event(
          state.replication_slot,
          %Health.Event{slug: :replication_message_processed, status: :fail, error: error}
        )

        {:noreply, state}

      error ->
        Logger.error("[SlotMessageHandler] Error handling messages: #{inspect(error)}", error: error)
        raise error
    end
  end

  @impl GenServer
  def handle_info(:load_entities, %State{} = state) do
    # Load entities from the database
    state = load_entities(state)
    {:noreply, state}
  end

  @impl GenServer
  def handle_info(:process_logging, %State{} = state) do
    now = System.monotonic_time(:millisecond)
    interval_ms = if state.last_logged_stats_at, do: now - state.last_logged_stats_at

    info =
      Process.info(self(), [
        # Total memory used by process in bytes
        :memory,
        # Number of messages in queue
        :message_queue_len
      ])

    metadata = [
      memory_mb: Float.round(info[:memory] / 1_024 / 1_024, 2),
      message_queue_len: info[:message_queue_len],
      processor_idx: state.processor_idx,
      processor_count: state.processor_count,
      replication_slot_id: state.id
    ]

    # Get all timing metrics from process dictionary
    timing_metrics =
      Process.get()
      |> Enum.filter(fn {key, _} ->
        key |> to_string() |> String.ends_with?("_total_ms")
      end)
      |> Keyword.new()
      |> IO.inspect()

    # Log all timing metrics as histograms with operation tag
    Enum.each(timing_metrics, fn {key, value} ->
      operation = key |> to_string() |> String.replace("_total_ms", "")

      Sequin.Statsd.histogram("sequin.slot_message_handler.operation_time_ms", value,
        tags: %{
          replication_slot_id: state.id,
          processor_idx: state.processor_idx,
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
      Sequin.Statsd.histogram("sequin.slot_message_handler.operation_time_ms", unaccounted_ms,
        tags: %{
          replication_slot_id: state.id,
          processor_idx: state.processor_idx,
          operation: "unaccounted"
        }
      )
    end

    timing_metrics
    |> Keyword.put(:unaccounted_ms, unaccounted_ms)
    |> IO.inspect(label: "timing_metrics")

    metadata =
      metadata
      |> Keyword.merge(timing_metrics)
      |> Sequin.Keyword.put_if_present(:unaccounted_total_ms, unaccounted_ms)

    Logger.info("[SlotMessageHandler] Process metrics", metadata)

    # Clear timing metrics after logging
    timing_metrics
    |> Keyword.keys()
    |> Enum.each(&clear_counter/1)

    schedule_process_logging()
    {:noreply, %{state | last_logged_stats_at: now}}
  end

  # Private functions

  defp schedule_process_logging(interval \\ :timer.seconds(30)) do
    Process.send_after(self(), :process_logging, interval)
  end

  defp partition_messages(messages, processor_count) do
    Enum.group_by(messages, &message_to_processor_idx(&1, processor_count))
  end

  defp message_to_processor_idx(%Message{} = message, processor_count) do
    # Use table_oid and ids to determine partition
    # This ensures related messages go to the same partition
    hash_input = "#{message.table_oid}:#{Enum.join(message.ids, ",")}"
    :erlang.phash2(hash_input, processor_count)
  end

  defp load_entities(%State{} = state) do
    slot = Replication.get_pg_replication!(state.id)

    %{postgres_database: db, not_disabled_sink_consumers: consumers, wal_pipelines: pipelines} =
      Repo.preload(slot, [
        :postgres_database,
        :wal_pipelines,
        not_disabled_sink_consumers: [:sequence, :postgres_database]
      ])

    %{
      state
      | postgres_database: db,
        consumers: consumers,
        wal_pipelines: pipelines,
        processor_count: slot.processor_count
    }
  end

  defp context(%State{} = state) do
    %MessageHandler.Context{
      consumers: state.consumers,
      wal_pipelines: state.wal_pipelines,
      postgres_database: state.postgres_database,
      replication_slot_id: state.id,
      table_reader_mod: state.table_reader_mod
    }
  end

  defp clear_counter(name) do
    Process.delete(name)
  end

  # defp incr_counter(name, amount \\ 1) do
  #   current = Process.get(name, 0)
  #   Process.put(name, current + amount)
  # end

  # defp execute_timed(name, fun) do
  #   {time, result} = :timer.tc(fun)
  #   # Convert microseconds to milliseconds
  #   incr_counter(:"#{name}_total_ms", div(time, 1000))
  #   result
  # end
end

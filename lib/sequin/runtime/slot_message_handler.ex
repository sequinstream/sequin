defmodule Sequin.Runtime.SlotMessageHandler do
  @moduledoc """
  A GenServer implementation of the MessageHandler that processes messages in partitions.

  This GenServer/module wraps MessageHandlera and is designed to be
  started in multiple partitions to distribute the processing load.
  """

  use GenServer

  alias Ecto.Adapters.SQL.Sandbox
  alias Sequin.Error
  alias Sequin.Health
  alias Sequin.Replication
  alias Sequin.Repo
  alias Sequin.Runtime.MessageHandler
  alias Sequin.Runtime.MessageHandler.Context
  alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.LogicalMessage
  alias Sequin.Runtime.SlotProcessor.Message
  alias Sequin.Runtime.TableReaderServer

  require Logger

  @callback before_handle_messages(Context.t(), messages :: [Message.t()]) ::
              :ok | {:error, reason :: any()}

  @callback handle_messages(context :: Context.t(), messages :: [Message.t()]) ::
              :ok | {:error, reason :: any()}

  @callback handle_logical_message(Context.t(), seq :: non_neg_integer(), message :: LogicalMessage.t()) ::
              :ok | {:error, reason :: any()}

  @callback reload_entities(context :: Context.t()) ::
              :ok | {:error, reason :: any()}

  @callback flush_messages(context :: Context.t()) ::
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
      field :partition_count, non_neg_integer()
      field :last_logged_stats_at, non_neg_integer()

      # Entities
      field :replication_slot, PostgresReplicationSlot.t()
      field :consumers, [SinkConsumer.t()], default: []
      field :wal_pipelines, [WalPipeline.t()], default: []
      field :postgres_database, PostgresDatabase.t()

      field :table_reader, module()
      field :message_handler, module()
    end
  end

  # Client API

  @doc """
  Starts a new SlotMessageHandler process.

  ## Options

  * `:context` - The MessageHandler.Context to use
  * `:processor_idx` - The ID of this partition (0-based)
  * `:partition_count` - The total number of partitions
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
    slot_id = Keyword.fetch!(opts, :replication_slot_id)
    processor_idx = Keyword.fetch!(opts, :processor_idx)

    %{
      id: {__MODULE__, slot_id, processor_idx},
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
  def handle_messages(_ctx, []) do
    :ok
  end

  def handle_messages(%Context{} = ctx, messages) do
    %{replication_slot_id: slot_id, partition_count: partition_count} = ctx
    # Determine which partition to send to based on a hash of the message
    messages_by_partition = partition_messages(messages, partition_count)

    # Send messages to each partition
    Sequin.Enum.reduce_while_ok(messages_by_partition, fn {processor_idx, partition_messages} ->
      name = via_tuple(slot_id, processor_idx)
      GenServer.call(name, {:handle_messages, partition_messages}, :timer.seconds(30))
    end)
  end

  def reload_entities(%Context{} = ctx) do
    %{replication_slot_id: slot_id, partition_count: partition_count} = ctx

    Sequin.Enum.reduce_while_ok(0..(partition_count - 1), fn processor_idx ->
      name = via_tuple(slot_id, processor_idx)
      GenServer.call(name, :load_entities)
    end)
  end

  @doc """
  Flushes all message handlers for the given slot.

  This ensures that all SlotMessageHandlers have finished processing their messages.
  Upstream callers can use this to ensure all handlers are done processing.
  """
  def flush_messages(%Context{} = ctx) do
    %{replication_slot_id: slot_id, partition_count: partition_count} = ctx

    Sequin.Enum.reduce_while_ok(0..(partition_count - 1), fn processor_idx ->
      name = via_tuple(slot_id, processor_idx)
      GenServer.call(name, :flush)
    end)
  end

  @doc """
  Determines which partition a message should be routed to.

  Uses a consistent hash of the message's table_oid and ids to ensure that
  related messages (those affecting the same table and records) are always
  routed to the same partition. This ensures ordering and consistency when
  processing related database changes.

  ## Parameters

    * `message` - The message to determine partition for
    * `partition_count` - The total number of available partitions

  ## Returns

    A zero-based partition index between 0 and partition_count-1
  """
  def message_partition_idx(%Message{} = message, partition_count) do
    # Use table_oid and ids to determine partition
    # This ensures related messages go to the same partition
    hash_input = {message.table_oid, message.ids}
    :erlang.phash2(hash_input, partition_count)
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

    if test_pid = Keyword.get(opts, :test_pid) do
      Mox.allow(Sequin.TestSupport.DateTimeMock, test_pid, self())
      Mox.allow(Sequin.Runtime.MessageHandlerMock, test_pid, self())
      Sandbox.allow(Sequin.Repo, test_pid, self())
    end

    Logger.info("[SlotMessageHandler] Starting partition #{processor_idx} for slot #{replication_slot_id}")

    state = %State{
      id: replication_slot_id,
      processor_idx: processor_idx,
      table_reader: Keyword.get(opts, :table_reader, TableReaderServer),
      message_handler: Keyword.get(opts, :message_handler, MessageHandler)
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

    case state.message_handler.handle_messages(context(state), messages) do
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

        raise error

      error ->
        Logger.error("[SlotMessageHandler] Error handling messages: #{inspect(error)}", error: error)
        raise error
    end
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
      partition_count: state.partition_count,
      replication_slot_id: state.id
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

  defp partition_messages(messages, partition_count) do
    Enum.group_by(messages, &message_partition_idx(&1, partition_count))
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
        partition_count: slot.partition_count,
        replication_slot: slot
    }
  end

  defp context(%State{} = state) do
    %MessageHandler.Context{
      consumers: state.consumers,
      wal_pipelines: state.wal_pipelines,
      postgres_database: state.postgres_database,
      replication_slot_id: state.id,
      table_reader_mod: state.table_reader
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

defmodule Sequin.Runtime.SlotMessageHandler do
  @moduledoc """
  A GenServer implementation of the MessageHandler that processes messages in partitions.

  This GenServer/module wraps MessageHandlera and is designed to be
  started in multiple partitions to distribute the processing load.
  """

  use GenServer

  use Sequin.ProcessMetrics,
    metric_prefix: "sequin.slot_message_handler",
    interval: :timer.seconds(30)

  use Sequin.ProcessMetrics.Decorator

  alias Ecto.Adapters.SQL.Sandbox
  alias Sequin.Error
  alias Sequin.ProcessMetrics
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
      field :outbox_messages, [Message.t()], default: nil

      # Entities
      field :replication_slot, PostgresReplicationSlot.t()
      field :consumers, [SinkConsumer.t()], default: []
      field :wal_pipelines, [WalPipeline.t()], default: []
      field :postgres_database, PostgresDatabase.t()

      field :table_reader, module()
      field :message_handler, module()

      field :setting_retry_deliver_interval, non_neg_integer(), default: :timer.seconds(1)
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

    Sequin.TaskSupervisor
    |> Task.Supervisor.async_stream(
      0..(partition_count - 1),
      fn processor_idx ->
        name = via_tuple(slot_id, processor_idx)
        GenServer.call(name, :flush)
      end,
      max_concurrency: partition_count,
      timeout: :timer.seconds(10)
    )
    |> Sequin.Enum.reduce_while_ok(fn
      {:ok, :ok} -> :ok
      error -> error
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
    retry_deliver_interval = Keyword.get(opts, :setting_retry_deliver_interval, :timer.seconds(1))

    Logger.metadata(
      replication_id: replication_slot_id,
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
      message_handler: Keyword.get(opts, :message_handler, MessageHandler),
      setting_retry_deliver_interval: retry_deliver_interval
    }

    # Add dynamic tags for metrics
    ProcessMetrics.metadata(%{
      replication_slot_id: replication_slot_id,
      processor_idx: processor_idx
    })

    ProcessMetrics.start()

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
  def handle_call({:handle_messages, _messages}, _from, %State{outbox_messages: messages} = state)
      when is_list(messages) do
    # We're still trying to deliver outbox messages.
    {:reply, {:error, Error.invariant(code: :payload_size_limit_exceeded, message: "Payload size limit exceeded")}, state}
  end

  def handle_call({:handle_messages, messages}, from, %State{} = state) do
    # Reply immediately with a placeholder result
    # This allows the caller to continue without waiting for processing
    GenServer.reply(from, :ok)

    state = deliver_messages(state, messages)
    {:noreply, state}
  end

  @impl GenServer
  def handle_info(:retry_deliver, %State{} = state) do
    state = deliver_messages(state, state.outbox_messages)
    {:noreply, state}
  end

  # Private functions

  @decorate track_metrics("deliver_messages")
  defp deliver_messages(%State{} = state, messages) do
    # Process messages asynchronously
    Logger.debug("[SlotMessageHandler] Handling #{length(messages)} message(s) in partition #{state.processor_idx}")

    case state.message_handler.handle_messages(context(state), messages) do
      {:ok, count} ->
        # Track the number of messages processed
        ProcessMetrics.increment_throughput("messages_processed", count)
        %{state | outbox_messages: nil}

      {:error, %Error.InvariantError{code: :payload_size_limit_exceeded}} ->
        # We've already replied to the caller. We'll put the messages in the outbox
        # and schedule a retry delivery.
        state = %{state | outbox_messages: messages}
        schedule_retry_deliver(state)
        state

      error ->
        Logger.error("[SlotMessageHandler] Error handling messages: #{inspect(error)}", error: error)
        raise error
    end
  end

  defp schedule_retry_deliver(%State{} = state) do
    Process.send_after(self(), :retry_deliver, state.setting_retry_deliver_interval)
  end

  defp partition_messages(messages, partition_count) do
    Enum.group_by(messages, &message_partition_idx(&1, partition_count))
  end

  @decorate track_metrics("load_entities")
  defp load_entities(%State{} = state) do
    slot = Replication.get_pg_replication!(state.id)
    Logger.metadata(partition_count: slot.partition_count)

    %{postgres_database: db, not_disabled_sink_consumers: consumers, wal_pipelines: pipelines} =
      Repo.preload(slot, [
        :wal_pipelines,
        [postgres_database: :sequences],
        not_disabled_sink_consumers: [:sequence, :postgres_database, :filter]
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
    MessageHandler.Context.set_indices(%MessageHandler.Context{
      consumers: state.consumers,
      wal_pipelines: state.wal_pipelines,
      postgres_database: state.postgres_database,
      replication_slot_id: state.id,
      table_reader_mod: state.table_reader
    })
  end
end

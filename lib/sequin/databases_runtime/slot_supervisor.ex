defmodule Sequin.DatabasesRuntime.SlotSupervisor do
  @moduledoc false
  use DynamicSupervisor

  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.DatabasesRuntime.SlotMessageStore
  alias Sequin.DatabasesRuntime.SlotProcessor
  alias Sequin.DatabasesRuntime.SlotProcessor.MessageHandler
  alias Sequin.Replication.PostgresReplicationSlot
  alias Sequin.Repo

  require Logger

  def via_tuple(id) do
    {:via, :syn, {:replication, __MODULE__, id}}
  end

  def start_link(opts) do
    %PostgresReplicationSlot{} = pg_replication = Keyword.fetch!(opts, :pg_replication)
    DynamicSupervisor.start_link(__MODULE__, opts, name: via_tuple(pg_replication.id))
  end

  @impl DynamicSupervisor
  def init(_opts) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  def start_children(%PostgresReplicationSlot{} = pg_replication, opts) do
    pg_replication =
      Repo.preload(pg_replication, [
        :postgres_database,
        sink_consumers: [:sequence],
        not_disabled_sink_consumers: [:sequence]
      ])

    # First start all message stores for consumers
    Enum.each(pg_replication.sink_consumers, &start_message_store_only!(&1, opts))

    # Then start the slot processor
    slot_processor_spec = slot_processor_child_spec(pg_replication, opts)

    case Sequin.DynamicSupervisor.start_child(via_tuple(pg_replication.id), slot_processor_spec) do
      {:ok, slot_processor_pid} ->
        # Register all active message stores after processor is started
        Enum.each(
          pg_replication.not_disabled_sink_consumers,
          &SlotProcessor.monitor_message_store(pg_replication.id, &1.id)
        )

        {:ok, slot_processor_pid}

      {:error, {:already_started, slot_processor_pid}} ->
        # Register all active message stores with existing processor
        Enum.each(
          pg_replication.not_disabled_sink_consumers,
          &SlotProcessor.monitor_message_store(pg_replication.id, &1.id)
        )

        {:ok, slot_processor_pid}

      {:error, error} ->
        Logger.error("Failed to start slot processor: #{inspect(error)}")
        raise error
    end
  end

  # New helper function to start message store without processor dependency
  defp start_message_store_only!(%SinkConsumer{} = sink_consumer, opts) do
    sink_consumer = Repo.preload(sink_consumer, :replication_slot)
    child_spec = slot_message_store_child_spec(sink_consumer, opts)

    sink_consumer.replication_slot_id
    |> via_tuple()
    |> Sequin.DynamicSupervisor.start_child(child_spec)
    |> case do
      {:ok, pid} ->
        pid

      {:error, {:already_started, pid}} ->
        pid

      {:error, error} ->
        Logger.error("Failed to start child #{inspect(child_spec)}: #{inspect(error)}")
        raise error
    end
  end

  def start_message_store!(%SinkConsumer{} = sink_consumer, opts \\ []) do
    sink_consumer = Repo.preload(sink_consumer, :replication_slot)

    # First start the message store
    start_message_store_only!(sink_consumer, opts)

    # Then ensure slot processor is started
    start_slot_processor!(sink_consumer.replication_slot)

    # Finally monitor the message store
    SlotProcessor.monitor_message_store(sink_consumer.replication_slot_id, sink_consumer.id)
    :ok
  end

  def stop_message_store(replication_slot_id, id) do
    Logger.info("[SlotSupervisor] Stopping message store #{id} in slot #{replication_slot_id}",
      consumer_id: id,
      replication_id: replication_slot_id
    )

    sup_via = via_tuple(replication_slot_id)
    child_via = SlotMessageStore.via_tuple(id)
    SlotProcessor.demonitor_message_store(replication_slot_id, id)

    Sequin.DynamicSupervisor.stop_child(sup_via, child_via)
  end

  defp start_slot_processor!(%PostgresReplicationSlot{} = pg_replication, opts \\ []) do
    pg_replication = Repo.preload(pg_replication, :postgres_database)
    slot_processor_spec = slot_processor_child_spec(pg_replication, opts)

    case Sequin.DynamicSupervisor.start_child(via_tuple(pg_replication.id), slot_processor_spec) do
      {:ok, pid} ->
        pid

      {:error, {:already_started, pid}} ->
        pid

      {:error, error} ->
        Logger.error("Failed to start slot processor: #{inspect(error)}")
        raise error
    end
  end

  defp slot_processor_child_spec(%PostgresReplicationSlot{} = pg_replication, opts) do
    default_opts = [
      id: pg_replication.id,
      slot_name: pg_replication.slot_name,
      publication: pg_replication.publication_name,
      postgres_database: pg_replication.postgres_database,
      replication_slot: pg_replication,
      message_handler_ctx: MessageHandler.context(pg_replication),
      message_handler_module: MessageHandler,
      connection: PostgresDatabase.to_postgrex_opts(pg_replication.postgres_database),
      ipv6: pg_replication.postgres_database.ipv6
    ]

    opts = Keyword.merge(default_opts, opts)
    {Sequin.DatabasesRuntime.SlotProcessor, opts}
  end

  defp slot_message_store_child_spec(%SinkConsumer{} = sink_consumer, opts) do
    opts = Keyword.put(opts, :consumer_id, sink_consumer.id)
    {SlotMessageStore, opts}
  end
end

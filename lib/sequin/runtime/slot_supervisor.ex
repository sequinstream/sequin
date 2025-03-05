defmodule Sequin.Runtime.SlotSupervisor do
  @moduledoc false
  use DynamicSupervisor

  alias Sequin.Consumers
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Replication.PostgresReplicationSlot
  alias Sequin.Repo
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Runtime.SlotMessageStoreSupervisor
  alias Sequin.Runtime.SlotProcessor
  alias Sequin.Runtime.SlotProcessor.MessageHandler

  require Logger

  def via_tuple(id) do
    {:via, :syn, {:replication, {__MODULE__, id}}}
  end

  def start_link(opts) do
    %PostgresReplicationSlot{} = pg_replication = Keyword.fetch!(opts, :pg_replication)

    case DynamicSupervisor.start_link(__MODULE__, opts, name: via_tuple(pg_replication.id)) do
      {:ok, pid} ->
        {:ok, _} = start_children(pg_replication, opts)
        {:ok, pid}

      {:error, {:already_started, pid}} ->
        {:ok, _} = start_children(pg_replication, opts)
        {:ok, pid}

      {:error, error} ->
        Logger.error("[SlotSupervisor] Failed to start: #{inspect(error)}")
    end
  end

  @impl DynamicSupervisor
  def init(_opts) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  def stop_slot_processor(id) do
    sup_via = via_tuple(id)
    Sequin.DynamicSupervisor.stop_child(sup_via, SlotProcessor.via_tuple(id))
  end

  def start_children(%PostgresReplicationSlot{} = pg_replication, opts) do
    pg_replication =
      Repo.preload(pg_replication, [
        :postgres_database,
        sink_consumers: [:sequence],
        not_disabled_sink_consumers: [:sequence]
      ])

    slot_processor_spec = slot_processor_child_spec(pg_replication, opts)
    sup = via_tuple(pg_replication.id)

    # First start all message stores for consumers
    opts = Keyword.put(opts, :skip_monitor, true)
    Enum.each(pg_replication.not_disabled_sink_consumers, &start_stores_and_pipeline!(&1, opts))

    # Then start the slot processor
    case Sequin.DynamicSupervisor.maybe_start_child(sup, slot_processor_spec) do
      {:ok, slot_processor_pid} ->
        # Register all active message stores after processor is started
        Enum.each(
          pg_replication.not_disabled_sink_consumers,
          &SlotProcessor.monitor_message_store(pg_replication.id, &1.id)
        )

        {:ok, slot_processor_pid}

      {:error, error} ->
        Logger.error("[SlotSupervisor] Failed to start SlotProcessor: #{inspect(error)}", error: error)
        {:error, error}
    end
  end

  def start_stores_and_pipeline!(%SinkConsumer{} = sink_consumer, opts \\ []) do
    sink_consumer = Repo.preload(sink_consumer, :replication_slot)
    store_sup_child_spec = slot_message_store_supervisor_child_spec(sink_consumer, opts)
    supervisor = via_tuple(sink_consumer.replication_slot_id)

    with {:ok, _store_sup_pid} <- Sequin.DynamicSupervisor.maybe_start_child(supervisor, store_sup_child_spec),
         :ok <- maybe_start_consumer_pipeline(sink_consumer, opts) do
      unless Keyword.get(opts, :skip_monitor, false) do
        :ok = SlotProcessor.monitor_message_store(sink_consumer.replication_slot_id, sink_consumer.id)
      end

      :ok
    else
      error ->
        Logger.error("[SlotSupervisor] Failed to start child",
          error: error,
          consumer_id: sink_consumer.id,
          replication_id: sink_consumer.replication_slot_id
        )

        raise error
    end
  end

  def stop_stores_and_pipeline(%SinkConsumer{} = sink_consumer) do
    stop_stores_and_pipeline(sink_consumer.replication_slot_id, sink_consumer.id)
  end

  def stop_stores_and_pipeline(replication_slot_id, id) do
    Logger.info("[SlotSupervisor] Stopping message store and pipeline #{id} in slot #{replication_slot_id}",
      consumer_id: id,
      replication_id: replication_slot_id
    )

    sup_via = via_tuple(replication_slot_id)
    store_sup_via = SlotMessageStoreSupervisor.via_tuple(id)

    SlotProcessor.demonitor_message_store(replication_slot_id, id)

    Sequin.DynamicSupervisor.stop_child(sup_via, store_sup_via)
    Sequin.DynamicSupervisor.stop_child(sup_via, SinkPipeline.via_tuple(id))
  end

  def restart_stores_and_pipeline(%SinkConsumer{} = sink_consumer) do
    stop_stores_and_pipeline(sink_consumer)
    start_stores_and_pipeline!(sink_consumer)
  end

  defp slot_message_store_supervisor_child_spec(%SinkConsumer{} = sink_consumer, opts) do
    opts = Keyword.put(opts, :consumer, sink_consumer)
    {SlotMessageStoreSupervisor, opts}
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
    {Sequin.Runtime.SlotProcessor, opts}
  end

  defp maybe_start_consumer_pipeline(%SinkConsumer{} = sink_consumer, opts) do
    consumer = Repo.preload(sink_consumer, [:sequence, :postgres_database])
    supervisor = via_tuple(sink_consumer.replication_slot_id)

    if consumer.type == :sequin_stream do
      :ok
    else
      default_opts = [consumer: consumer]
      consumer_features = Consumers.consumer_features(consumer)

      {features, opts} = Keyword.pop(opts, :features, [])
      features = Keyword.merge(consumer_features, features)

      opts =
        default_opts
        |> Keyword.merge(opts)
        |> Keyword.put(:features, features)

      child_spec = {SinkPipeline, opts}

      with {:ok, _} <- Sequin.DynamicSupervisor.maybe_start_child(supervisor, child_spec) do
        :ok
      end
    end
  end
end

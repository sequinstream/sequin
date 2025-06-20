defmodule Sequin.Runtime.SlotSupervisor do
  @moduledoc false
  use DynamicSupervisor

  alias Ecto.Adapters.SQL.Sandbox
  alias Sequin.Consumers
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Replication
  alias Sequin.Replication.PostgresReplicationSlot
  alias Sequin.Repo
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Runtime.SlotMessageStoreSupervisor
  alias Sequin.Runtime.SlotProcessorServer
  alias Sequin.Runtime.SlotProcessorSupervisor

  require Logger

  def via_tuple(id) do
    {:via, :syn, {:replication, {__MODULE__, id}}}
  end

  def start_link(opts) do
    pg_replication_id = Keyword.fetch!(opts, :pg_replication_id)
    %PostgresReplicationSlot{} = pg_replication = Replication.get_pg_replication!(pg_replication_id)

    case DynamicSupervisor.start_link(__MODULE__, opts, name: via_tuple(pg_replication_id)) do
      {:ok, pid} ->
        {:ok, _} = start_children(pg_replication, opts)
        {:ok, pid}

      {:error, {:already_started, pid}} ->
        {:ok, _} = start_children(pg_replication, opts)
        {:ok, pid}

      {:error, error} ->
        Logger.error("[SlotSupervisor] Failed to start: #{inspect(error)}")
        {:error, error}
    end
  end

  @impl DynamicSupervisor
  def init(_opts) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  def stop_slot_processor(id) do
    sup_via = via_tuple(id)
    Sequin.DynamicSupervisor.stop_child(sup_via, SlotProcessorSupervisor.via_tuple(id))
  end

  def start_children(%PostgresReplicationSlot{} = pg_replication, opts) do
    pg_replication = Repo.preload(pg_replication, [:sink_consumers, :not_disabled_sink_consumers])

    opts = Keyword.put(opts, :replication_slot_id, pg_replication.id)
    slot_processor_spec = SlotProcessorSupervisor.child_spec(opts)
    sup = via_tuple(pg_replication.id)

    # First start all message stores for consumers
    opts = Keyword.put(opts, :skip_link, true)

    pg_replication.not_disabled_sink_consumers
    |> Task.async_stream(
      fn consumer ->
        if test_pid = opts[:test_pid], do: Sandbox.allow(Sequin.Repo, test_pid, self())
        start_stores_and_pipeline!(consumer, opts)
      end,
      max_concurrency: 10,
      timeout: :timer.seconds(20)
    )
    |> Stream.map(fn
      {:ok, :ok} -> :ok
      {:error, error} -> raise error
    end)
    |> Stream.run()

    # Then start the slot processor
    case Sequin.DynamicSupervisor.maybe_start_child(sup, slot_processor_spec) do
      {:ok, slot_processor_sup_pid} ->
        # Register all active message stores after processor is started
        Enum.each(
          pg_replication.not_disabled_sink_consumers,
          &SlotProcessorServer.link_message_stores(pg_replication.id, &1)
        )

        {:ok, slot_processor_sup_pid}

      {:error, error} ->
        Logger.error("[SlotSupervisor] Failed to start SlotProcessorServer: #{inspect(error)}", error: error)
        {:error, error}
    end
  end

  def start_stores_and_pipeline!(%SinkConsumer{} = sink_consumer, opts \\ []) do
    sink_consumer = Repo.preload(sink_consumer, :replication_slot)
    store_sup_child_spec = slot_message_store_supervisor_child_spec(sink_consumer, opts)
    supervisor = via_tuple(sink_consumer.replication_slot_id)

    with {:ok, _store_sup_pid} <- Sequin.DynamicSupervisor.maybe_start_child(supervisor, store_sup_child_spec),
         :ok <- maybe_start_consumer_pipeline(sink_consumer, opts) do
      unless Keyword.get(opts, :skip_link, false) do
        :ok = SlotProcessorServer.link_message_stores(sink_consumer.replication_slot_id, sink_consumer)
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

    Sequin.DynamicSupervisor.stop_child(sup_via, store_sup_via)
    Sequin.DynamicSupervisor.stop_child(sup_via, SinkPipeline.via_tuple(id))
  end

  def restart_stores_and_pipeline(%SinkConsumer{} = sink_consumer) do
    stop_stores_and_pipeline(sink_consumer)
    start_stores_and_pipeline!(sink_consumer)
  end

  defp slot_message_store_supervisor_child_spec(%SinkConsumer{} = sink_consumer, opts) do
    opts = Keyword.put(opts, :consumer_id, sink_consumer.id)
    {SlotMessageStoreSupervisor, opts}
  end

  defp maybe_start_consumer_pipeline(%SinkConsumer{} = sink_consumer, opts) do
    supervisor = via_tuple(sink_consumer.replication_slot_id)

    if sink_consumer.type == :sequin_stream do
      :ok
    else
      default_opts = [consumer_id: sink_consumer.id]
      consumer_features = Consumers.consumer_features(sink_consumer)

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

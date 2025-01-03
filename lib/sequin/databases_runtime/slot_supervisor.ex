defmodule Sequin.DatabasesRuntime.SlotSupervisor do
  @moduledoc false
  use DynamicSupervisor

  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.DatabasesRuntime.SlotMessageStore
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
    pg_replication = Repo.preload(pg_replication, [:postgres_database, sink_consumers: [:sequence]])

    pg_replication
    |> children(opts)
    |> Enum.map(fn child ->
      case Sequin.DynamicSupervisor.start_child(via_tuple(pg_replication.id), child) do
        {:ok, pid} ->
          {:ok, pid}

        {:error, {:already_started, pid}} ->
          {:ok, pid}

        {:error, error} ->
          Logger.error("Failed to start child #{inspect(child)}: #{inspect(error)}")
          raise error
      end
    end)
  end

  def start_child(%SinkConsumer{} = sink_consumer, opts \\ []) do
    child_spec = slot_message_store_child_spec(sink_consumer, opts)

    sink_consumer.replication_slot_id
    |> via_tuple()
    |> Sequin.DynamicSupervisor.start_child(child_spec)
    |> case do
      {:ok, pid} ->
        {:ok, pid}

      {:error, {:already_started, pid}} ->
        {:ok, pid}

      {:error, error} ->
        Logger.error("Failed to start child #{inspect(child_spec)}: #{inspect(error)}")
        raise error
    end
  end

  def stop_child(replication_slot_id, id) do
    sup_via = via_tuple(replication_slot_id)
    child_via = SlotMessageStore.via_tuple(id)

    Sequin.DynamicSupervisor.stop_child(sup_via, child_via)
  end

  defp children(%PostgresReplicationSlot{} = pg_replication, opts) do
    slot_message_store_opts = Keyword.get(opts, :slot_message_store_opts, [])

    [
      slot_processor_child_spec(pg_replication, opts)
      | Enum.map(pg_replication.sink_consumers, &slot_message_store_child_spec(&1, slot_message_store_opts))
    ]
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

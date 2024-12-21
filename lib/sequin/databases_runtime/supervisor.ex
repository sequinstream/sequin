defmodule Sequin.DatabasesRuntime.Supervisor do
  @moduledoc """
  Supervisor for managing database-related runtime processes.
  """
  use Supervisor

  alias Sequin.Databases.PostgresDatabase
  alias Sequin.DatabasesRuntime.SlotProcessor
  alias Sequin.DatabasesRuntime.SlotProcessor.MessageHandler
  alias Sequin.DatabasesRuntime.SlotProcessorSupervisor
  alias Sequin.DatabasesRuntime.TableReaderServer
  alias Sequin.DatabasesRuntime.TableReaderServerSupervisor
  alias Sequin.DatabasesRuntime.WalEventSupervisor
  alias Sequin.DatabasesRuntime.WalPipelineServer
  alias Sequin.Replication.PostgresReplicationSlot
  alias Sequin.Repo

  require Logger

  defp table_reader_supervisor, do: {:via, :syn, {:replication, TableReaderServerSupervisor}}
  defp postgres_replication_supervisor, do: {:via, :syn, {:replication, SlotProcessorSupervisor}}
  defp wal_event_supervisor, do: {:via, :syn, {:replication, WalEventSupervisor}}

  def start_link(opts) do
    name = Keyword.get(opts, :name, {:via, :syn, {:replication, __MODULE__}})
    Supervisor.start_link(__MODULE__, nil, name: name)
  end

  @impl Supervisor
  def init(_opts) do
    Supervisor.init(children(), strategy: :one_for_one)
  end

  def start_table_reader(supervisor \\ table_reader_supervisor(), consumer, opts \\ []) do
    consumer = Repo.preload(consumer, [:active_backfill, :sequence])

    default_opts = [
      backfill_id: consumer.active_backfill.id,
      table_oid: consumer.sequence.table_oid
    ]

    opts = Keyword.merge(default_opts, opts)

    Sequin.DynamicSupervisor.start_child(supervisor, {TableReaderServer, opts})
  end

  def stop_table_reader(supervisor \\ table_reader_supervisor(), consumer_or_backfill_id) do
    Sequin.DynamicSupervisor.stop_child(supervisor, TableReaderServer.via_tuple(consumer_or_backfill_id))
    :ok
  end

  def restart_table_reader(supervisor \\ table_reader_supervisor(), consumer, opts \\ []) do
    stop_table_reader(supervisor, consumer)
    start_table_reader(supervisor, consumer, opts)
  end

  def start_replication(supervisor \\ postgres_replication_supervisor(), pg_replication_or_id, opts \\ [])

  def start_replication(supervisor, %PostgresReplicationSlot{} = pg_replication, opts) do
    pg_replication = Repo.preload(pg_replication, :postgres_database)

    if pg_replication.status == :disabled do
      Logger.info("PostgresReplicationSlot #{pg_replication.id} is disabled, skipping start")
      {:error, :disabled}
    else
      default_opts = [
        id: pg_replication.id,
        slot_name: pg_replication.slot_name,
        publication: pg_replication.publication_name,
        postgres_database: pg_replication.postgres_database,
        message_handler_ctx: MessageHandler.context(pg_replication),
        message_handler_module: MessageHandler,
        connection: PostgresDatabase.to_postgrex_opts(pg_replication.postgres_database),
        ipv6: pg_replication.postgres_database.ipv6
      ]

      opts = Keyword.merge(default_opts, opts)
      Sequin.DynamicSupervisor.start_child(supervisor, {SlotProcessor, opts})
    end
  end

  def refresh_message_handler_ctx(id) do
    case Sequin.Replication.get_pg_replication(id) do
      {:ok, pg_replication} ->
        # Remove races by locking - better way?
        :global.trans(
          {__MODULE__, id},
          fn ->
            new_ctx = MessageHandler.context(pg_replication)

            case SlotProcessor.update_message_handler_ctx(id, new_ctx) do
              :ok -> :ok
              {:error, :not_running} -> :ok
              error -> error
            end
          end,
          [node() | Node.list()],
          # This is retries, not timeout
          20
        )

      error ->
        error
    end
  end

  def stop_replication(supervisor \\ postgres_replication_supervisor(), pg_replication_or_id)

  def stop_replication(supervisor, %PostgresReplicationSlot{id: id}) do
    stop_replication(supervisor, id)
  end

  def stop_replication(supervisor, id) do
    Sequin.DynamicSupervisor.stop_child(supervisor, SlotProcessor.via_tuple(id))
    :ok
  end

  def restart_replication(supervisor \\ postgres_replication_supervisor(), pg_replication_or_id) do
    stop_replication(supervisor, pg_replication_or_id)
    start_replication(supervisor, pg_replication_or_id)
  end

  def start_wal_pipeline_servers(supervisor \\ wal_event_supervisor(), pg_replication) do
    pg_replication = Repo.preload(pg_replication, :wal_pipelines)

    pg_replication.wal_pipelines
    |> Enum.filter(fn wp -> wp.status == :active end)
    |> Enum.group_by(fn wp -> {wp.replication_slot_id, wp.destination_oid, wp.destination_database_id} end)
    |> Enum.each(fn {{replication_slot_id, destination_oid, destination_database_id}, pipelines} ->
      start_wal_pipeline_server(supervisor, replication_slot_id, destination_oid, destination_database_id, pipelines)
    end)
  end

  def start_wal_pipeline_server(
        supervisor \\ wal_event_supervisor(),
        replication_slot_id,
        destination_oid,
        destination_database_id,
        pipelines
      ) do
    opts = [
      replication_slot_id: replication_slot_id,
      destination_oid: destination_oid,
      destination_database_id: destination_database_id,
      wal_pipeline_ids: Enum.map(pipelines, & &1.id)
    ]

    Sequin.DynamicSupervisor.start_child(
      supervisor,
      {WalPipelineServer, opts}
    )
  end

  def stop_wal_pipeline_servers(supervisor \\ wal_event_supervisor(), pg_replication) do
    pg_replication = Repo.preload(pg_replication, :wal_pipelines)

    pg_replication.wal_pipelines
    |> Enum.group_by(fn wp -> {wp.replication_slot_id, wp.destination_oid, wp.destination_database_id} end)
    |> Enum.each(fn {{replication_slot_id, destination_oid, destination_database_id}, _} ->
      stop_wal_pipeline_server(supervisor, replication_slot_id, destination_oid, destination_database_id)
    end)
  end

  def stop_wal_pipeline_server(
        supervisor \\ wal_event_supervisor(),
        replication_slot_id,
        destination_oid,
        destination_database_id
      ) do
    Logger.info("Stopping WalPipelineServer",
      replication_slot_id: replication_slot_id,
      destination_oid: destination_oid,
      destination_database_id: destination_database_id
    )

    Sequin.DynamicSupervisor.stop_child(
      supervisor,
      WalPipelineServer.via_tuple({replication_slot_id, destination_oid, destination_database_id})
    )
  end

  def restart_wal_pipeline_servers(supervisor \\ wal_event_supervisor(), pg_replication) do
    stop_wal_pipeline_servers(supervisor, pg_replication)
    start_wal_pipeline_servers(supervisor, pg_replication)
  end

  defp children do
    [
      Sequin.DatabasesRuntime.Starter,
      Sequin.DynamicSupervisor.child_spec(name: table_reader_supervisor()),
      Sequin.DynamicSupervisor.child_spec(name: postgres_replication_supervisor()),
      Sequin.DynamicSupervisor.child_spec(name: wal_event_supervisor())
    ]
  end
end

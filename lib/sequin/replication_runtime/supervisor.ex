defmodule Sequin.ReplicationRuntime.Supervisor do
  @moduledoc """
  """
  use Supervisor

  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Extensions.Replication, as: ReplicationExt
  alias Sequin.Replication.MessageHandler
  alias Sequin.Replication.PostgresReplicationSlot
  alias Sequin.ReplicationRuntime.PostgresReplicationSupervisor
  alias Sequin.ReplicationRuntime.WalEventSupervisor
  alias Sequin.ReplicationRuntime.WalPipelineServer
  alias Sequin.Repo

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @impl Supervisor
  def init(_) do
    Supervisor.init(children(), strategy: :one_for_one)
  end

  def start_replication(supervisor \\ PostgresReplicationSupervisor, pg_replication_or_id, opts \\ [])

  def start_replication(supervisor, %PostgresReplicationSlot{} = pg_replication, opts) do
    pg_replication = Repo.preload(pg_replication, :postgres_database)

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

    Sequin.DynamicSupervisor.start_child(supervisor, {ReplicationExt, opts})
  end

  def start_replication(supervisor, id, opts) do
    case Sequin.Replication.get_pg_replication(id) do
      {:ok, pg_replication} -> start_replication(supervisor, pg_replication, opts)
      error -> error
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

            case ReplicationExt.update_message_handler_ctx(id, new_ctx) do
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

  def stop_replication(supervisor \\ PostgresReplicationSupervisor, pg_replication_or_id)

  def stop_replication(supervisor, %PostgresReplicationSlot{id: id}) do
    stop_replication(supervisor, id)
  end

  def stop_replication(supervisor, id) do
    Sequin.DynamicSupervisor.stop_child(supervisor, ReplicationExt.via_tuple(id))
    :ok
  end

  def restart_replication(supervisor \\ PostgresReplicationSupervisor, pg_replication_or_id) do
    stop_replication(supervisor, pg_replication_or_id)
    start_replication(supervisor, pg_replication_or_id)
  end

  def start_wal_pipeline_servers(pg_replication) do
    pg_replication = Repo.preload(pg_replication, :wal_pipelines)

    pg_replication.wal_pipelines
    |> Enum.filter(fn wp -> wp.status == :active end)
    |> Enum.group_by(fn wp -> {wp.replication_slot_id, wp.destination_oid, wp.destination_database_id} end)
    |> Enum.each(fn {{replication_slot_id, destination_oid, destination_database_id}, pipelines} ->
      start_wal_pipeline_server(replication_slot_id, destination_oid, destination_database_id, pipelines)
    end)
  end

  def start_wal_pipeline_server(replication_slot_id, destination_oid, destination_database_id, pipelines) do
    opts = [
      replication_slot_id: replication_slot_id,
      destination_oid: destination_oid,
      destination_database_id: destination_database_id,
      wal_pipeline_ids: Enum.map(pipelines, & &1.id)
    ]

    Sequin.DynamicSupervisor.start_child(
      WalEventSupervisor,
      {WalPipelineServer, opts}
    )
  end

  def stop_wal_pipeline_servers(pg_replication) do
    pg_replication = Repo.preload(pg_replication, :wal_pipelines)

    pg_replication.wal_pipelines
    |> Enum.group_by(fn wp -> {wp.replication_slot_id, wp.destination_oid, wp.destination_database_id} end)
    |> Enum.each(fn {{replication_slot_id, destination_oid, destination_database_id}, _} ->
      stop_wal_pipeline_server(replication_slot_id, destination_oid, destination_database_id)
    end)
  end

  def stop_wal_pipeline_server(replication_slot_id, destination_oid, destination_database_id) do
    Sequin.DynamicSupervisor.stop_child(
      WalEventSupervisor,
      WalPipelineServer.via_tuple({replication_slot_id, destination_oid, destination_database_id})
    )
  end

  def restart_wal_pipeline_servers(pg_replication) do
    stop_wal_pipeline_servers(pg_replication)
    start_wal_pipeline_servers(pg_replication)
  end

  defp children do
    [
      Sequin.ReplicationRuntime.Starter,
      Sequin.DynamicSupervisor.child_spec(name: Sequin.ReplicationRuntime.PostgresReplicationSupervisor),
      Sequin.DynamicSupervisor.child_spec(name: WalEventSupervisor)
    ]
  end
end

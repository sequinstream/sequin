defmodule Sequin.SourcesRuntime.Supervisor do
  @moduledoc """
  """
  use Supervisor

  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Extensions.Replication
  alias Sequin.Repo
  alias Sequin.Sources.PostgresReplication
  alias Sequin.Sources.PostgresReplicationMessageHandler
  alias Sequin.SourcesRuntime.PostgresReplicationSupervisor

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @impl Supervisor
  def init(_) do
    Supervisor.init(children(), strategy: :one_for_one)
  end

  def start_for_pg_replication(supervisor \\ PostgresReplicationSupervisor, pg_replication_or_id, opts \\ [])

  def start_for_pg_replication(supervisor, %PostgresReplication{} = pg_replication, opts) do
    pg_replication = Repo.preload(pg_replication, :postgres_database)

    if pg_replication.status == :active do
      default_opts = [
        id: pg_replication.id,
        slot_name: pg_replication.slot_name,
        publication: pg_replication.publication_name,
        message_handler_ctx: PostgresReplicationMessageHandler.context(pg_replication),
        message_handler_module: PostgresReplicationMessageHandler,
        connection: PostgresDatabase.to_postgrex_opts(pg_replication.postgres_database),
        key_format: pg_replication.key_format
      ]

      opts = Keyword.merge(default_opts, opts)

      Sequin.DynamicSupervisor.start_child(supervisor, {Replication, opts})
    else
      {:error, :backfill_not_completed}
    end
  end

  def start_for_pg_replication(supervisor, id, opts) do
    case Sequin.Sources.get_pg_replication(id) do
      {:ok, pg_replication} -> start_for_pg_replication(supervisor, pg_replication, opts)
      error -> error
    end
  end

  def stop_for_pg_replication(supervisor \\ PostgresReplicationSupervisor, pg_replication_or_id)

  def stop_for_pg_replication(supervisor, %PostgresReplication{id: id}) do
    stop_for_pg_replication(supervisor, id)
  end

  def stop_for_pg_replication(supervisor, id) do
    Sequin.DynamicSupervisor.stop_child(supervisor, Replication.via_tuple(id))
    :ok
  end

  def restart_for_pg_replication(supervisor \\ PostgresReplicationSupervisor, pg_replication_or_id) do
    stop_for_pg_replication(supervisor, pg_replication_or_id)
    start_for_pg_replication(supervisor, pg_replication_or_id)
  end

  defp children do
    [
      Sequin.SourcesRuntime.Starter,
      Sequin.DynamicSupervisor.child_spec(name: Sequin.SourcesRuntime.PostgresReplicationSupervisor)
    ]
  end
end

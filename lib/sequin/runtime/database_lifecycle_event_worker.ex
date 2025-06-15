defmodule Sequin.Runtime.DatabaseLifecycleEventWorker do
  @moduledoc """
  Worker that runs after a lifecycle event (create/update/delete) in the databases context.
  """

  use Oban.Worker,
    queue: :lifecycle,
    max_attempts: 3

  alias Sequin.Databases
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Replication
  alias Sequin.Repo
  alias Sequin.Runtime

  require Logger

  @events ~w(create update delete)a
  @entities ~w(postgres_database postgres_replication_slot)a

  @spec enqueue(event :: atom(), entity_type :: atom(), entity_id :: String.t(), data :: map() | nil) ::
          {:ok, Oban.Job.t()} | {:error, Oban.Job.changeset() | term()}
  def enqueue(event, entity_type, entity_id, data \\ nil) when event in @events and entity_type in @entities do
    Oban.insert(new(%{event: event, entity_type: entity_type, entity_id: entity_id, data: data}))
  end

  @impl Oban.Worker
  def perform(%Oban.Job{
        args: %{"event" => event, "entity_type" => entity_type, "entity_id" => entity_id, "data" => data}
      }) do
    case entity_type do
      "postgres_database" ->
        handle_database_event(event, entity_id, data)

      "postgres_replication_slot" ->
        handle_postgres_replication_slot_event(event, entity_id)
    end
  end

  defp handle_database_event(event, id, data) do
    Databases.invalidate_cached_db(id)
    Logger.info("[LifecycleEventWorker] Handling event `#{event}` for database", database_id: id)

    case event do
      "create" ->
        with {:ok, %PostgresDatabase{} = db} <- Databases.get_db(id) do
          Databases.update_pg_major_version(db)
        end

      "update" ->
        with {:ok, %PostgresDatabase{} = db} <- Databases.get_db(id) do
          Databases.ConnectionCache.invalidate_connection(db)
          db = Repo.preload(db, [:replication_slot])
          Runtime.Supervisor.restart_replication(db.replication_slot)
        end

      "delete" ->
        replication_slot_id = Map.fetch!(data, "replication_slot_id")
        Runtime.Supervisor.stop_replication(replication_slot_id)
    end
  end

  defp handle_postgres_replication_slot_event(event, id) do
    Logger.info("[LifecycleEventWorker] Handling event `#{event}` for postgres_replication_slot",
      replication_id: id
    )

    case event do
      "create" ->
        with {:ok, pg_replication} <- Replication.get_pg_replication(id) do
          Runtime.Supervisor.start_replication(pg_replication)
        end

      "update" ->
        with {:ok, pg_replication} <- Replication.get_pg_replication(id) do
          if pg_replication.status == :disabled do
            Runtime.Supervisor.stop_replication(pg_replication)
          else
            Runtime.Supervisor.restart_replication(pg_replication)
          end
        end

      "delete" ->
        Runtime.Supervisor.stop_replication(id)
    end
  end
end

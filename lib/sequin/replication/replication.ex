defmodule Sequin.Replication do
  @moduledoc false
  alias Sequin.Databases
  alias Sequin.Databases.ConnectionCache
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Error
  alias Sequin.Error.NotFoundError
  alias Sequin.Extensions.Replication, as: ReplicationExt
  alias Sequin.Replication.BackfillPostgresTableWorker
  alias Sequin.Replication.PostgresReplicationSlot
  alias Sequin.ReplicationRuntime
  alias Sequin.Repo

  # PostgresReplicationSlot

  def all_pg_replications do
    Repo.all(PostgresReplicationSlot)
  end

  def all_active_pg_replications do
    Repo.all(PostgresReplicationSlot)
  end

  def list_pg_replications_for_account(account_id) do
    Repo.all(PostgresReplicationSlot.where_account(account_id))
  end

  def get_pg_replication(id) do
    case Repo.get(PostgresReplicationSlot, id, preload: :postgres_database) do
      nil -> {:error, Error.not_found(entity: :pg_replication, params: %{id: id})}
      pg_replication -> {:ok, pg_replication}
    end
  end

  def get_pg_replication_for_account(account_id, id) do
    case Repo.get_by(PostgresReplicationSlot, id: id, account_id: account_id) do
      nil -> {:error, Error.not_found(entity: :pg_replication)}
      pg_replication -> {:ok, pg_replication}
    end
  end

  def create_pg_replication_for_account_with_lifecycle(account_id, attrs) do
    attrs = Sequin.Map.atomize_keys(attrs)
    backfill? = attrs[:backfill_existing_rows] != false
    status = if backfill?, do: :backfilling, else: :active
    attrs = Map.put(attrs, :status, status)

    with {:ok, postgres_database} <- get_or_build_postgres_database(account_id, attrs),
         :ok <- validate_replication_config(postgres_database, attrs) do
      pg_replication =
        %PostgresReplicationSlot{account_id: account_id}
        |> PostgresReplicationSlot.create_changeset(attrs)
        |> Repo.insert()

      case pg_replication do
        {:ok, pg_replication} ->
          if backfill? do
            enqueue_backfill_jobs(pg_replication)
          end

          unless Application.get_env(:sequin, :env) == :test do
            ReplicationRuntime.Supervisor.start_replication(pg_replication)
          end

          {:ok, pg_replication}

        error ->
          error
      end
    else
      {:error, %NotFoundError{}} ->
        {:error, Error.validation(summary: "Database with id #{attrs[:postgres_database_id]} not found")}

      error ->
        error
    end
  end

  def update_pg_replication(%PostgresReplicationSlot{} = pg_replication, attrs) do
    pg_replication
    |> PostgresReplicationSlot.update_changeset(attrs)
    |> Repo.update()
  end

  def delete_pg_replication(%PostgresReplicationSlot{} = pg_replication) do
    Repo.delete(pg_replication)
  end

  def delete_pg_replication_with_lifecycle(%PostgresReplicationSlot{} = pg_replication) do
    res = Repo.delete(pg_replication)
    ReplicationRuntime.Supervisor.stop_replication(pg_replication)
    res
  end

  def add_info(%PostgresReplicationSlot{} = pg_replication) do
    pg_replication = Repo.preload(pg_replication, [:postgres_database])

    last_committed_at = ReplicationExt.get_last_committed_at(pg_replication.id)
    # key_pattern = "#{pg_replication.postgres_database.name}.>"

    # total_ingested_messages =
    #   Streams.fast_count_messages_for_stream(pg_replication.stream_id, key_pattern: key_pattern)

    info = %PostgresReplicationSlot.Info{
      last_committed_at: last_committed_at,
      total_ingested_messages: nil
    }

    %{pg_replication | info: info}
  end

  defp get_or_build_postgres_database(account_id, attrs) do
    case attrs do
      %{postgres_database_id: id} ->
        Databases.get_db_for_account(account_id, id)

      %{postgres_database: db_attrs} ->
        db_attrs = Sequin.Map.atomize_keys(db_attrs)
        {:ok, struct(PostgresDatabase, Map.put(db_attrs, :account_id, account_id))}

      _ ->
        {:error, Error.validation(summary: "Missing postgres_database_id or postgres_database")}
    end
  end

  def validate_replication_config(%PostgresDatabase{} = db, attrs) do
    Databases.with_connection(db, fn conn ->
      with :ok <- validate_slot(conn, attrs.slot_name) do
        validate_publication(conn, attrs.publication_name)
      end
    end)
  end

  defp validate_slot(conn, slot_name) do
    query = "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1"

    case Postgrex.query(conn, query, [slot_name]) do
      {:ok, %{num_rows: 1}} ->
        :ok

      {:ok, %{num_rows: 0}} ->
        {:error, Error.validation(summary: "Replication slot `#{slot_name}` does not exist")}

      _ ->
        {:error,
         Error.validation(summary: "Error connecting to the database to verify the existence of the replication slot.")}
    end
  end

  defp validate_publication(conn, publication_name) do
    query = "SELECT 1 FROM pg_publication WHERE pubname = $1"

    case Postgrex.query(conn, query, [publication_name]) do
      {:ok, %{num_rows: 1}} ->
        :ok

      {:ok, %{num_rows: 0}} ->
        {:error, Error.validation(summary: "Publication `#{publication_name}` does not exist")}

      _ ->
        {:error,
         Error.validation(summary: "Error connecting to the database to verify the existence of the publication.")}
    end
  end

  defp enqueue_backfill_jobs(%PostgresReplicationSlot{} = pg_replication) do
    pg_replication = Repo.preload(pg_replication, :postgres_database)
    {:ok, conn} = ConnectionCache.connection(pg_replication.postgres_database)

    tables = get_publication_tables(conn, pg_replication.publication_name)

    Enum.each(tables, fn {schema, table} ->
      BackfillPostgresTableWorker.create(
        pg_replication.postgres_database_id,
        schema,
        table,
        pg_replication.id
      )
    end)
  end

  @doc """
  Creates backfill jobs for the specified tables in a PostgresReplicationSlot.
  """
  def create_backfill_jobs(postgres_replication, tables) do
    jobs =
      Enum.map(tables, fn %{"schema" => schema, "table" => table} ->
        BackfillPostgresTableWorker.create(
          postgres_replication.postgres_database_id,
          schema,
          table,
          postgres_replication.id
        )
      end)

    job_ids = Enum.map(jobs, fn {:ok, job} -> job.id end)
    {:ok, job_ids}
  end

  defp get_publication_tables(conn, publication_name) do
    query = """
    SELECT schemaname, tablename
    FROM pg_publication_tables
    WHERE pubname = $1
    """

    {:ok, %{rows: rows}} = Postgrex.query(conn, query, [publication_name])
    Enum.map(rows, fn [schemaname, tablename] -> {schemaname, tablename} end)
  end
end

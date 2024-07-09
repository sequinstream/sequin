defmodule Sequin.Sources do
  @moduledoc false
  alias Sequin.Databases
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Error
  alias Sequin.Error.NotFoundError
  alias Sequin.Repo
  alias Sequin.Sources.PostgresReplication
  alias Sequin.SourcesRuntime

  # PostgresReplication

  def all_pg_replications do
    Repo.all(PostgresReplication)
  end

  def all_active_pg_replications do
    Repo.all(PostgresReplication.where_active())
  end

  def list_pg_replications_for_account(account_id) do
    Repo.all(PostgresReplication.where_account(account_id))
  end

  def get_pg_replication(id) do
    case Repo.get(PostgresReplication, id, preload: :postgres_database) do
      nil -> {:error, Error.not_found(entity: :pg_replication)}
      pg_replication -> {:ok, pg_replication}
    end
  end

  def get_pg_replication_for_account(account_id, id) do
    case Repo.get_by(PostgresReplication, id: id, account_id: account_id) do
      nil -> {:error, Error.not_found(entity: :pg_replication)}
      pg_replication -> {:ok, pg_replication}
    end
  end

  def create_pg_replication_for_account(account_id, attrs) do
    attrs = Sequin.Map.atomize_keys(attrs)

    with {:ok, postgres_database} <- get_or_build_postgres_database(account_id, attrs),
         :ok <- validate_replication_config(postgres_database, attrs) do
      %PostgresReplication{account_id: account_id}
      |> PostgresReplication.create_changeset(attrs)
      |> Repo.insert()
    else
      {:error, %NotFoundError{}} ->
        {:error, Error.validation(summary: "Database with id #{attrs[:postgres_database_id]} not found")}

      error ->
        error
    end
  end

  def update_pg_replication(%PostgresReplication{} = pg_replication, attrs) do
    pg_replication
    |> PostgresReplication.update_changeset(attrs)
    |> Repo.update()
  end

  def delete_pg_replication(%PostgresReplication{} = pg_replication) do
    Repo.delete(pg_replication)
  end

  def delete_pg_replication_with_lifecycle(%PostgresReplication{} = pg_replication) do
    res = Repo.delete(pg_replication)
    SourcesRuntime.Supervisor.stop_for_pg_replication(pg_replication)
    res
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

  defp validate_replication_config(%PostgresDatabase{} = db, attrs) do
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
end

defmodule Sequin.Sources do
  @moduledoc false
  alias Sequin.Error
  alias Sequin.Repo
  alias Sequin.Sources.PostgresReplication

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
    res =
      %PostgresReplication{account_id: account_id}
      |> PostgresReplication.create_changeset(attrs)
      |> Repo.insert()

    case res do
      {:ok, pg_replication} -> {:ok, pg_replication}
      {:error, changeset} -> {:error, Error.validation(changeset: changeset)}
    end
  end

  def update_pg_replication(%PostgresReplication{} = pg_replication, attrs) do
    res = Repo.update(PostgresReplication.update_changeset(pg_replication, attrs))

    case res do
      {:ok, updated_pg_replication} -> {:ok, updated_pg_replication}
      {:error, changeset} -> {:error, Error.validation(changeset: changeset)}
    end
  end

  def delete_pg_replication(%PostgresReplication{} = pg_replication) do
    Repo.delete(pg_replication)
  end
end

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

  def get_pg_replication(id) do
    case Repo.get(PostgresReplication, id, preload: :postgres_database) do
      nil -> {:error, Error.not_found(entity: :pg_replication, id: id)}
      pg_replication -> {:ok, pg_replication}
    end
  end

  def create_pg_replication_for_account(account_id, attrs) do
    Repo.insert(PostgresReplication.changeset(%PostgresReplication{account_id: account_id}, attrs))
  end
end

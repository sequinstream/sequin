defmodule SequinWeb.PostgresReplicationController do
  use SequinWeb, :controller

  alias Sequin.Databases
  alias Sequin.Replication
  alias SequinWeb.ApiFallbackPlug

  action_fallback ApiFallbackPlug

  def index(conn, %{"db_id_or_name" => db_id_or_name}) do
    account_id = conn.assigns.account_id

    with {:ok, database} <- Databases.get_db_for_account(account_id, db_id_or_name) do
      render(conn, "index.json", postgres_replications: Replication.list_pg_replications_for_database(database.id))
    end
  end

  def show(conn, %{"db_id_or_name" => db_id_or_name, "id" => id_or_name}) do
    account_id = conn.assigns.account_id

    with {:ok, database} <- Databases.get_db_for_account(account_id, db_id_or_name),
         {:ok, postgres_replication} <- Replication.get_pg_replication_for_database_by_id_or_name(database.id, id_or_name) do
      postgres_replication_with_info = Replication.add_info(postgres_replication)
      render(conn, "show_with_info.json", postgres_replication: postgres_replication_with_info)
    end
  end

  def create(conn, %{"db_id_or_name" => db_id_or_name} = params) do
    account_id = conn.assigns.account_id

    with {:ok, database} <- Databases.get_db_for_account(account_id, db_id_or_name),
         {:ok, params} <- parse_create_params(params),
         # Set the database ID in params
         params = Map.put(params, "postgres_database_id", database.id),
         {:ok, postgres_replication} <- Replication.create_pg_replication(account_id, params) do
      render(conn, "show.json", postgres_replication: postgres_replication)
    end
  end

  def update(conn, %{"db_id_or_name" => db_id_or_name, "id" => id_or_name} = params) do
    account_id = conn.assigns.account_id

    with {:ok, database} <- Databases.get_db_for_account(account_id, db_id_or_name),
         {:ok, params} <- parse_update_params(params),
         {:ok, postgres_replication} <-
           Replication.get_pg_replication_for_database_by_id_or_name(database.id, id_or_name),
         {:ok, updated_postgres_replication} <- Replication.update_pg_replication(postgres_replication, params) do
      render(conn, "show.json", postgres_replication: updated_postgres_replication)
    end
  end

  def delete(conn, %{"db_id_or_name" => db_id_or_name, "id" => id_or_name}) do
    account_id = conn.assigns.account_id

    with {:ok, database} <- Databases.get_db_for_account(account_id, db_id_or_name),
         {:ok, postgres_replication} <-
           Replication.get_pg_replication_for_database_by_id_or_name(database.id, id_or_name),
         {:ok, _postgres_replication} <- Replication.delete_pg_replication(postgres_replication) do
      render(conn, "delete.json", postgres_replication: postgres_replication)
    end
  end

  defp parse_update_params(params) do
    params = Map.take(params, ["publication_name", "slot_name"])

    {:ok, params}
  end

  defp parse_create_params(params) do
    params = Map.take(params, ["publication_name", "slot_name"])

    {:ok, params}
  end
end

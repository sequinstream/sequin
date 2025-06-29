defmodule SequinWeb.PostgresReplicationController do
  use SequinWeb, :controller

  alias Sequin.Replication
  alias SequinWeb.ApiFallbackPlug

  action_fallback ApiFallbackPlug

  def index(conn, _params) do
    account_id = conn.assigns.account_id

    render(conn, "index.json", postgres_replications: Replication.list_pg_replications_for_account(account_id))
  end

  def show(conn, %{"id" => id}) do
    account_id = conn.assigns.account_id

    with {:ok, postgres_replication} <- Replication.get_pg_replication_for_account(account_id, id) do
      render(conn, "show_with_info.json", postgres_replication: postgres_replication)
    end
  end

  def create(conn, params) do
    account_id = conn.assigns.account_id

    with {:ok, postgres_replication} <- Replication.create_pg_replication(account_id, params) do
      render(conn, "show.json", postgres_replication: postgres_replication)
    end
  end

  def update(conn, %{"id" => id} = params) do
    account_id = conn.assigns.account_id

    with {:ok, params} <- parse_update_params(params),
         {:ok, postgres_replication} <- Replication.get_pg_replication_for_account(account_id, id),
         {:ok, updated_postgres_replication} <- Replication.update_pg_replication(postgres_replication, params) do
      render(conn, "show.json", postgres_replication: updated_postgres_replication)
    end
  end

  def delete(conn, %{"id" => id}) do
    account_id = conn.assigns.account_id

    with {:ok, postgres_replication} <- Replication.get_pg_replication_for_account(account_id, id),
         {:ok, _postgres_replication} <- Replication.delete_pg_replication(postgres_replication) do
      render(conn, "delete.json", postgres_replication: postgres_replication)
    end
  end

  defp parse_update_params(params) do
    forbidden_keys = ["postgres_database_id"]

    if Enum.any?(forbidden_keys, &Map.has_key?(params, &1)) do
      {:error,
       Sequin.Error.validation(
         summary: "Cannot update postgres_database_id",
         errors: %{
           base: ["Updating postgres_database_id is not allowed"]
         }
       )}
    else
      {:ok, params}
    end
  end
end

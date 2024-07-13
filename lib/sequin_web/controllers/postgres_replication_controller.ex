defmodule SequinWeb.PostgresReplicationController do
  use SequinWeb, :controller

  alias Sequin.Sources
  alias SequinWeb.ApiFallbackPlug

  action_fallback ApiFallbackPlug

  def index(conn, _params) do
    account_id = conn.assigns.account_id

    render(conn, "index.json", postgres_replications: Sources.list_pg_replications_for_account(account_id))
  end

  def show(conn, %{"id" => id}) do
    account_id = conn.assigns.account_id

    with {:ok, postgres_replication} <- Sources.get_pg_replication_for_account(account_id, id) do
      postgres_replication_with_info = Sources.add_info(postgres_replication)
      render(conn, "show_with_info.json", postgres_replication: postgres_replication_with_info)
    end
  end

  def create(conn, params) do
    account_id = conn.assigns.account_id

    with {:ok, postgres_replication} <- Sources.create_pg_replication_for_account_with_lifecycle(account_id, params) do
      render(conn, "show.json", postgres_replication: postgres_replication)
    end
  end

  def update(conn, %{"id" => id} = params) do
    account_id = conn.assigns.account_id

    with {:ok, params} <- parse_update_params(params),
         {:ok, postgres_replication} <- Sources.get_pg_replication_for_account(account_id, id),
         {:ok, updated_postgres_replication} <- Sources.update_pg_replication(postgres_replication, params) do
      render(conn, "show.json", postgres_replication: updated_postgres_replication)
    end
  end

  def delete(conn, %{"id" => id}) do
    account_id = conn.assigns.account_id

    with {:ok, postgres_replication} <- Sources.get_pg_replication_for_account(account_id, id),
         {:ok, _postgres_replication} <- Sources.delete_pg_replication(postgres_replication) do
      send_resp(conn, :no_content, "")
    end
  end

  def create_backfills(conn, %{"id" => id, "tables" => tables}) when is_list(tables) do
    account_id = conn.assigns.account_id

    with {:ok, valid_tables} <- validate_tables(tables),
         {:ok, postgres_replication} <- Sources.get_pg_replication_for_account(account_id, id),
         {:ok, job_ids} <- Sources.create_backfill_jobs(postgres_replication, valid_tables) do
      conn
      |> put_status(:created)
      |> render("backfill_jobs.json", job_ids: job_ids)
    else
      {:error, :invalid_table_format} ->
        conn
        |> put_status(:bad_request)
        |> json(%{error: "Invalid table format. Each table must be a map with 'schema' and 'table' keys."})

      error ->
        error
    end
  end

  def create_backfills(conn, %{"id" => _id, "tables" => _tables}) do
    conn
    |> put_status(:bad_request)
    |> json(%{error: "Invalid tables format. Expected a list of tables."})
  end

  def create_backfills(conn, %{"id" => _id}) do
    conn
    |> put_status(:bad_request)
    |> json(%{error: "Missing tables parameter."})
  end

  defp parse_update_params(params) do
    forbidden_keys = ["stream_id", "postgres_database_id"]

    if Enum.any?(forbidden_keys, &Map.has_key?(params, &1)) do
      {:error,
       Sequin.Error.validation(
         summary: "Cannot update stream_id or postgres_database_id",
         errors: %{
           base: ["Updating stream_id or postgres_database_id is not allowed"]
         }
       )}
    else
      {:ok, params}
    end
  end

  defp validate_tables(tables) do
    if Enum.all?(tables, &valid_table?/1) do
      {:ok, tables}
    else
      {:error, :invalid_table_format}
    end
  end

  defp valid_table?(%{"schema" => schema, "table" => table}) when is_binary(schema) and is_binary(table), do: true
  defp valid_table?(_), do: false
end

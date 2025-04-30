defmodule SequinWeb.PostgresDatabaseController do
  use SequinWeb, :controller

  alias Sequin.Databases
  alias Sequin.Error
  alias Sequin.Error.NotFoundError
  alias Sequin.Repo
  alias SequinWeb.ApiFallbackPlug

  action_fallback ApiFallbackPlug

  def index(conn, params) do
    account_id = conn.assigns.account_id
    show_sensitive = Map.get(params, "show_sensitive", "false") == "true"

    databases = Databases.list_dbs_for_account(account_id, [:replication_slot])
    render(conn, "index.json", databases: databases, show_sensitive: show_sensitive)
  end

  def show(conn, %{"id_or_name" => id_or_name} = params) do
    account_id = conn.assigns.account_id
    show_sensitive = Map.get(params, "show_sensitive", "false") == "true"

    with {:ok, database} <- Databases.get_db_for_account(account_id, id_or_name) do
      database = Repo.preload(database, :replication_slot)
      render(conn, "show.json", database: database, show_sensitive: show_sensitive)
    end
  end

  def test_connection(conn, %{"id_or_name" => id_or_name}) do
    account_id = conn.assigns.account_id

    with {:ok, database} <- Databases.get_db_for_account(account_id, id_or_name),
         database = Repo.preload(database, :replication_slot),
         :ok <- Databases.test_tcp_reachability(database),
         :ok <- Databases.test_connect(database, 10_000),
         :ok <- Databases.test_permissions(database),
         :ok <- Databases.verify_slot(database, database.replication_slot) do
      render(conn, "test_connection.json", success: true)
    else
      {:error, %NotFoundError{entity: :postgres_database}} = error ->
        error

      {:error, reason} ->
        conn
        |> put_status(:unprocessable_entity)
        |> render("test_connection.json", success: false, reason: format_error_reason(reason))
    end
  end

  # def list_schemas(conn, %{"id_or_name" => id_or_name}) do
  #   account_id = conn.assigns.account_id

  #   with {:ok, database} <- Databases.get_db_for_account(account_id, id_or_name),
  #        {:ok, schemas} <- Databases.list_schemas(database) do
  #     render(conn, "schemas.json", schemas: schemas)
  #   end
  # end

  # def list_tables(conn, %{"id_or_name" => id_or_name, "schema" => schema}) do
  #   account_id = conn.assigns.account_id

  #   with {:ok, database} <- Databases.get_db_for_account(account_id, id_or_name),
  #        {:ok, tables} <- Databases.list_tables(database, schema) do
  #     render(conn, "tables.json", tables: tables)
  #   end
  # end

  def refresh_tables(conn, %{"id_or_name" => id_or_name}) do
    account_id = conn.assigns.account_id

    with {:ok, database} <- Databases.get_db_for_account(account_id, id_or_name),
         {:ok, _updated_db} <- Databases.update_tables(database) do
      render(conn, "refresh_tables.json", success: true)
    else
      {:error, %NotFoundError{}} = error ->
        error

      {:error, _reason} ->
        conn
        |> put_status(:unprocessable_entity)
        |> render("error.json", success: false)
    end
  end

  defp format_error_reason(reason) do
    case reason do
      %Error.ValidationError{summary: summary} ->
        summary

      %Postgrex.Error{postgres: %{code: code, message: msg}} when not is_nil(code) and not is_nil(msg) ->
        "Error from Postgres: #{msg} (code=#{code})"

      %Postgrex.Error{message: msg} when not is_nil(msg) ->
        "Error from Postgres: #{msg}"

      :econnrefused ->
        "Connection refused. Please check if the database server is running and accessible."

      :timeout ->
        "Connection timed out. Please verify the hostname and port are correct."

      :nxdomain ->
        "Unable to resolve the hostname. Please check if the hostname is correct."

      %Error.NotFoundError{entity: entity, params: params} ->
        "Not found: #{entity} with params #{inspect(params)}"

      _ ->
        "An unexpected error occurred: #{inspect(reason)}"
    end
  end
end

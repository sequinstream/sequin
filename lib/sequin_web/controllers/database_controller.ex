defmodule SequinWeb.DatabaseController do
  use SequinWeb, :controller

  alias Sequin.Databases
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Error
  alias Sequin.Error.NotFoundError
  alias SequinWeb.ApiFallbackPlug

  action_fallback ApiFallbackPlug

  def index(conn, _params) do
    account_id = conn.assigns.account_id

    render(conn, "index.json", databases: Databases.list_dbs_for_account(account_id))
  end

  def show(conn, %{"id" => id}) do
    account_id = conn.assigns.account_id

    with {:ok, database} <- Databases.get_db_for_account(account_id, id) do
      render(conn, "show.json", database: database)
    end
  end

  def create(conn, params) do
    account_id = conn.assigns.account_id

    with :ok <- test_database_connection(params),
         {:ok, database} <- Databases.create_db_for_account(account_id, params) do
      render(conn, "show.json", database: database)
    end
  end

  def update(conn, %{"id" => id} = params) do
    account_id = conn.assigns.account_id

    with {:ok, existing_database} <- Databases.get_db_for_account(account_id, id),
         :ok <- test_database_connection(existing_database, params),
         {:ok, updated_database} <- Databases.update_db(existing_database, params) do
      render(conn, "show.json", database: updated_database)
    end
  end

  def delete(conn, %{"id" => id}) do
    account_id = conn.assigns.account_id

    with {:ok, database} <- Databases.get_db_for_account(account_id, id),
         {:ok, _database} <- Databases.delete_db(database) do
      render(conn, "delete.json", database: database)
    end
  end

  def test_connection(conn, %{"id" => id}) do
    account_id = conn.assigns.account_id

    with {:ok, database} <- Databases.get_db_for_account(account_id, id),
         :ok <- test_database_connection(database) do
      render(conn, "test_connection.json", success: true)
    else
      {:error, %NotFoundError{}} = error ->
        error

      {:error, reason} ->
        conn
        |> put_status(:unprocessable_entity)
        |> render("test_connection.json", success: false, reason: reason)
    end
  end

  def test_connection_params(conn, params) do
    case test_database_connection(params) do
      :ok ->
        render(conn, "test_connection.json", success: true)

      {:error, %NotFoundError{}} = error ->
        error

      {:error, reason} ->
        conn
        |> put_status(:unprocessable_entity)
        |> render("test_connection.json", success: false, reason: reason)
    end
  end

  def setup_replication(conn, %{"id" => id, "slot_name" => slot_name, "publication_name" => publication_name}) do
    account_id = conn.assigns.account_id

    with :ok <- validate_replication_params(slot_name, publication_name),
         {:ok, database} <- Databases.get_db_for_account(account_id, id),
         {:ok, _} <- Databases.setup_replication(database, slot_name, publication_name) do
      render(conn, "setup_replication.json", slot_name: slot_name, publication_name: publication_name)
    end
  end

  defp validate_replication_params(slot_name, publication_name) do
    with true <- String.length(slot_name) > 0,
         true <- String.length(publication_name) > 0 do
      :ok
    else
      false -> {:error, Error.validation(summary: "slot_name and publication_name are required")}
    end
  end

  defp test_database_connection(database, new_attrs \\ %{})

  defp test_database_connection(%PostgresDatabase{} = database, new_attrs) do
    database = Map.merge(database, Sequin.Map.atomize_keys(new_attrs))

    with :ok <- Databases.test_tcp_reachability(database, connection_test_timeouts()),
         :ok <- Databases.test_connect(database, connection_test_timeouts()) do
      Databases.test_permissions(database)
    end
  end

  defp test_database_connection(params, _new_attrs) do
    atomized_params = Sequin.Map.atomize_keys(params)
    database = struct(PostgresDatabase, atomized_params)
    test_database_connection(database)
  end

  defp connection_test_timeouts do
    if Application.fetch_env!(:sequin, :env) == :test do
      25
    else
      10_000
    end
  end
end

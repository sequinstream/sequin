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

  def show(conn, %{"id_or_name" => id_or_name}) do
    account_id = conn.assigns.account_id

    with {:ok, database} <- Databases.get_db_for_account(account_id, id_or_name) do
      render(conn, "show.json", database: database)
    end
  end

  def create(conn, params) do
    account_id = conn.assigns.account_id

    with {:ok, cleaned_params} <- parse_params(params),
         :ok <- test_database_connection(cleaned_params),
         {:ok, database} <- Databases.create_db_for_account(account_id, cleaned_params) do
      render(conn, "show.json", database: database)
    end
  end

  def update(conn, %{"id_or_name" => id_or_name} = params) do
    account_id = conn.assigns.account_id

    with {:ok, existing_database} <- Databases.get_db_for_account(account_id, id_or_name),
         {:ok, cleaned_params} <- parse_params(params),
         :ok <- test_database_connection(existing_database, cleaned_params),
         {:ok, updated_database} <- Databases.update_db(existing_database, cleaned_params) do
      render(conn, "show.json", database: updated_database)
    end
  end

  def delete(conn, %{"id_or_name" => id_or_name}) do
    account_id = conn.assigns.account_id

    with {:ok, database} <- Databases.get_db_for_account(account_id, id_or_name),
         {:ok, _database} <- Databases.delete_db(database) do
      render(conn, "delete.json", database: database)
    end
  end

  def test_connection(conn, %{"id_or_name" => id_or_name}) do
    account_id = conn.assigns.account_id

    with {:ok, database} <- Databases.get_db_for_account(account_id, id_or_name),
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
    with {:ok, cleaned_params} <- parse_params(params),
         :ok <- test_database_connection(cleaned_params) do
      render(conn, "test_connection.json", success: true)
    else
      {:error, reason} ->
        conn
        |> put_status(:unprocessable_entity)
        |> render("test_connection.json", success: false, reason: reason)
    end
  end

  def setup_replication(conn, %{
        "id_or_name" => id_or_name,
        "slot_name" => slot_name,
        "publication_name" => publication_name,
        "tables" => tables
      }) do
    account_id = conn.assigns.account_id

    with :ok <- validate_replication_params(slot_name, publication_name, tables),
         {:ok, database} <- Databases.get_db_for_account(account_id, id_or_name),
         {:ok, _} <- Databases.setup_replication(database, slot_name, publication_name, tables) do
      render(conn, "setup_replication.json",
        slot_name: slot_name,
        publication_name: publication_name,
        tables: tables
      )
    end
  end

  def list_schemas(conn, %{"id_or_name" => id_or_name}) do
    account_id = conn.assigns.account_id

    with {:ok, database} <- Databases.get_db_for_account(account_id, id_or_name),
         {:ok, schemas} <- Databases.list_schemas(database) do
      render(conn, "schemas.json", schemas: schemas)
    end
  end

  def list_tables(conn, %{"id_or_name" => id_or_name, "schema" => schema}) do
    account_id = conn.assigns.account_id

    with {:ok, database} <- Databases.get_db_for_account(account_id, id_or_name),
         {:ok, tables} <- Databases.list_tables(database, schema) do
      render(conn, "tables.json", tables: tables)
    end
  end

  defp validate_replication_params(slot_name, publication_name, tables) do
    with :ok <- validate_slot_name(slot_name),
         :ok <- validate_publication_name(publication_name),
         :ok <- validate_tables(tables) do
      :ok
    else
      {:error, reason} -> {:error, Error.validation(summary: reason)}
    end
  end

  defp validate_slot_name(slot_name) do
    if String.length(slot_name) > 0 do
      :ok
    else
      {:error, "slot_name must not be empty"}
    end
  end

  defp validate_publication_name(publication_name) do
    if String.length(publication_name) > 0 do
      :ok
    else
      {:error, "publication_name must not be empty"}
    end
  end

  defp validate_tables(tables) do
    if is_list(tables) && length(tables) > 0 do
      (Enum.all?(tables, &validate_table/1) && :ok) || {:error, "Invalid table format"}
    else
      {:error, "tables must be a non-empty list"}
    end
  end

  defp validate_table([schema, table_name]), do: validate_pg_identifier(schema) && validate_pg_identifier(table_name)

  defp validate_table(_), do: false

  defp validate_pg_identifier(str), do: is_binary(str) && String.length(str) > 0 && !String.contains?(str, "\"")

  defp test_database_connection(database, new_attrs \\ %{})

  defp test_database_connection(%PostgresDatabase{} = database, new_attrs) do
    new_attrs = Map.drop(new_attrs, ["id", "id_or_name"])
    database = Map.merge(database, Sequin.Map.atomize_keys(new_attrs))

    with :ok <- Databases.test_tcp_reachability(database, connection_test_timeout()),
         :ok <- Databases.test_connect(database, connection_test_timeout()) do
      Databases.test_permissions(database)
    else
      {:error, %Error.ValidationError{}} = error ->
        error

      {:error, %Postgrex.Error{} = error} ->
        # On connection issues, message is sometimes in first layer
        message =
          (error.postgres && error.postgres.message) || error.message || "Unknown Postgres error"

        code = error.postgres && error.postgres.code

        summary =
          if code do
            "Error from Postgres: #{message} (code=#{code})"
          else
            "Error from Postgres: #{message}"
          end

        {:error, Error.validation(summary: summary)}

      {:error, error} ->
        {:error, Error.validation(summary: "Unknown error connecting to database: #{inspect(error)}")}
    end
  end

  defp test_database_connection(params, _new_attrs) do
    atomized_params = Sequin.Map.atomize_keys(params)
    database = struct(PostgresDatabase, atomized_params)
    test_database_connection(database)
  end

  defp connection_test_timeout do
    if Application.fetch_env!(:sequin, :env) == :test do
      50
    else
      10_000
    end
  end

  defp parse_params(params) do
    cleaned_params =
      params
      |> Map.take(["hostname", "port", "database", "username", "name"])
      |> Map.update("hostname", nil, &maybe_trim/1)
      |> Map.update("port", nil, &maybe_trim/1)
      |> Map.update("database", nil, &maybe_trim/1)
      |> Map.update("username", nil, &maybe_trim/1)
      |> Map.update("name", nil, &maybe_trim/1)
      |> Sequin.Map.reject_nil_values()

    {:ok, Map.merge(params, cleaned_params)}
  end

  defp maybe_trim(str) when is_binary(str), do: String.trim(str)
  defp maybe_trim(other), do: other
end

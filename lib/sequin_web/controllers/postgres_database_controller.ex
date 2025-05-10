defmodule SequinWeb.PostgresDatabaseController do
  use SequinWeb, :controller

  import Sequin.Error.Guards, only: [is_error: 1]

  alias Sequin.Databases
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Error
  alias Sequin.Error.NotFoundError
  alias Sequin.Replication.PostgresReplicationSlot
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

  def create(conn, params) when is_map(params) do
    account_id = conn.assigns.account_id

    with {:ok, db_params} <- parse_db_params(params),
         {:ok, slot_params} <- parse_slot_params(params, :create),
         :ok <- test_db_conn(db_params, slot_params, account_id),
         {:ok, database} <- Databases.create_db_with_slot(account_id, db_params, slot_params) do
      conn
      |> put_status(:created)
      |> render("show.json", database: database, show_sensitive: false)
    else
      # Special handling/wrapping for replication slot changeset errors
      {:error, %Ecto.Changeset{data: %PostgresReplicationSlot{}} = error} ->
        render_slot_error(error)

      error ->
        error
    end
  end

  def update(conn, %{"id_or_name" => id_or_name} = params) do
    account_id = conn.assigns.account_id

    with {:ok, db_params} <- parse_db_params(params),
         {:ok, slot_params} <- parse_slot_params(params, :update),
         {:ok, database} <- Databases.get_db_for_account(account_id, id_or_name),
         database = Repo.preload(database, :replication_slot),
         {:ok, updated_database} <- update_database_with_slot(database, db_params, slot_params) do
      render(conn, "show.json", database: updated_database, show_sensitive: false)
    else
      # Special handling/wrapping for replication slot changeset errors
      {:error, %Ecto.Changeset{data: %PostgresReplicationSlot{}} = error} ->
        render_slot_error(error)

      error ->
        error
    end
  end

  def delete(conn, %{"id_or_name" => id_or_name}) do
    account_id = conn.assigns.account_id

    with {:ok, database} <- Databases.get_db_for_account(account_id, id_or_name),
         :ok <- Databases.delete_db_with_replication_slot(database) do
      render(conn, "delete.json", success: true, id: database.id)
    else
      {:error, %NotFoundError{}} = error ->
        error

      {:error, %Error.ValidationError{} = error} ->
        conn
        |> put_status(:unprocessable_entity)
        |> render("error.json", error: error.summary)
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

  # Test database connection with parameters
  defp test_db_conn(db_params, slot_params, account_id) do
    db = params_to_db(db_params, account_id)

    replication_slot =
      slot_params
      |> Sequin.Map.atomize_keys()
      |> then(&struct(PostgresReplicationSlot, &1))

    with :ok <- Databases.test_tcp_reachability(db),
         :ok <- Databases.test_connect(db, 10_000),
         :ok <- Databases.test_permissions(db),
         {:ok, primary} <- parse_primary_params(db_params, account_id),
         :ok <- Databases.test_maybe_replica(db, primary) do
      Databases.verify_slot(db, replication_slot)
    else
      {:error, error} when is_error(error) ->
        {:error, error}

      {:error, %DBConnection.ConnectionError{message: message}} ->
        {:error,
         Error.validation(summary: "Failed to connect to database. Please check connection details. (error=#{message})")}

      {:error, %Postgrex.Error{postgres: %{code: code, message: msg}}} ->
        {:error,
         Error.validation(
           summary: "Failed to connect to database. Please check connection details. (error=#{code} #{msg})"
         )}
    end
  end

  defp parse_primary_params(nil, _), do: {:ok, nil}

  defp parse_primary_params(ps, account_id) do
    with {:ok, pp} <- parse_db_params(ps) do
      params_to_db(pp, account_id)
    end
  end

  @spec params_to_db(map() | nil, String.t()) :: PostgresDatabase.t() | nil
  # Ignore nil params
  defp params_to_db(nil, _), do: nil

  # Convert params to a PostgresDatabase struct
  defp params_to_db(params, account_id) do
    params
    |> Sequin.Map.atomize_keys()
    |> Map.put(:account_id, account_id)
    |> then(&struct(PostgresDatabase, &1))
  end

  @overlapping_url_params ["database", "hostname", "port", "username", "password"]

  @example_url "postgresql://user:password@localhost:5432/mydb"

  defp parse_db_params(%{"url" => url} = params) do
    uri = URI.parse(url)

    uri_params = %{
      "hostname" => uri.host,
      "port" => uri.port,
      "database" =>
        case uri.path do
          "/" <> dbname -> dbname
          dbname -> dbname
        end
    }

    uri_params = maybe_add_userinfo(uri_params, uri.userinfo)

    missing_params = for {k, nil} <- uri_params, do: k

    cond do
      Enum.any?(@overlapping_url_params, fn p -> Map.get(params, p) end) ->
        {:error,
         Error.validation(
           summary: "Bad connection details. If `url` is specified, no other connection params are allowed"
         )}

      not Enum.empty?(missing_params) ->
        {:error,
         Error.validation(
           summary:
             "Parameters missing from `url`: #{Enum.join(missing_params, ", ")}. It should look like: #{@example_url}"
         )}

      not is_nil(uri.query) ->
        {:error, Error.validation(summary: "Query parameters not allowed in `url` - specify e.g. ssl with `ssl` key")}

      true ->
        params
        |> Map.delete("url")
        |> Map.merge(uri_params)
        |> parse_db_params()
    end
  end

  # Extract and validate database parameters
  defp parse_db_params(db_params) do
    # Only allow specific fields to be set by the API
    allowed_params =
      db_params
      |> Map.take([
        "database",
        "hostname",
        "name",
        "port",
        "username",
        "password",
        "ssl",
        "use_local_tunnel",
        "ipv6",
        "annotations",
        "primary"
      ])
      |> Map.put_new("port", 5432)

    {:ok, allowed_params}
  end

  defp maybe_add_userinfo(uri_params, nil), do: uri_params

  defp maybe_add_userinfo(uri_params, userinfo) do
    case String.split(userinfo, ":", parts: 2) do
      [username, password] ->
        Map.merge(uri_params, %{"username" => username, "password" => password})

      _ ->
        uri_params
    end
  end

  # Extract and validate slot parameters
  defp parse_slot_params(%{"replication_slots" => [slot]}, :create) when is_map(slot) do
    # For creation, we only allow these specific fields
    slot_params = Map.take(slot, ["publication_name", "slot_name", "status"])
    {:ok, slot_params}
  end

  defp parse_slot_params(_, :create) do
    {:error, Error.validation(summary: "A `replication_slots` field with exactly one slot is required")}
  end

  defp parse_slot_params(%{"replication_slots" => [slot]}, :update) when is_map(slot) do
    if Map.has_key?(slot, "id") do
      # For updates, we only allow these specific fields
      slot_params = Map.take(slot, ["id", "publication_name", "slot_name", "status"])
      {:ok, slot_params}
    else
      {
        :error,
        # Forwards compatible with multiple slots
        Error.validation(
          summary:
            "Slot in `replication_slots` must have an `id` field for updates. Alternatively, if you don't want to change the slot, you can omit the `replication_slots` key on the request body."
        )
      }
    end
  end

  defp parse_slot_params(%{"replication_slots" => []}, :update) do
    # Empty array is allowed for updates
    {:ok, %{}}
  end

  defp parse_slot_params(%{"replication_slots" => slots}, :update) when is_list(slots) do
    {:error,
     Error.validation(
       summary: "For updates, you can omit the `replication_slots` field or provide a list with exactly one slot"
     )}
  end

  defp parse_slot_params(_, :update) do
    # Not required for updates
    {:ok, %{}}
  end

  # Update database with slot if slot params are provided, otherwise just update the database
  defp update_database_with_slot(database, db_params, slot_params) when slot_params == %{} do
    Databases.update_db(database, db_params)
  end

  defp update_database_with_slot(database, db_params, slot_params) do
    Databases.update_db_with_slot(database, db_params, slot_params)
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

  defp render_slot_error(changeset) do
    # Wrap the keys for the replication slot, as it's nested
    errors = Sequin.Error.errors_on(changeset)

    {:error, Error.validation(errors: %{replication_slots: errors}, summary: "Validation failed")}
  end
end

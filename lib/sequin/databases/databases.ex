defmodule Sequin.Databases do
  @moduledoc false
  alias Sequin.Databases.ConnectionCache
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Error
  alias Sequin.Error.NotFoundError
  alias Sequin.Postgres
  alias Sequin.Repo
  alias Sequin.TcpUtils

  require Logger

  # PostgresDatabase

  def all_dbs do
    Repo.all(PostgresDatabase)
  end

  def list_dbs_for_account(account_id) do
    account_id
    |> PostgresDatabase.where_account()
    |> Repo.all()
  end

  def get_db(id) do
    case Repo.get(PostgresDatabase, id) do
      nil -> {:error, Error.not_found(entity: :postgres_database)}
      db -> {:ok, db}
    end
  end

  def get_db_for_account(account_id, id_or_name) do
    query =
      account_id
      |> PostgresDatabase.where_account()
      |> PostgresDatabase.where_id_or_name(id_or_name)

    case Repo.one(query) do
      nil -> {:error, Error.not_found(entity: :postgres_database)}
      db -> {:ok, db}
    end
  end

  def create_db_for_account(account_id, attrs) do
    res =
      %PostgresDatabase{account_id: account_id}
      |> PostgresDatabase.changeset(attrs)
      |> Repo.insert()

    case res do
      {:ok, db} -> {:ok, db}
      {:error, changeset} -> {:error, Error.validation(changeset: changeset)}
    end
  end

  def update_db(%PostgresDatabase{} = db, attrs) do
    res =
      db
      |> PostgresDatabase.changeset(attrs)
      |> Repo.update()

    case res do
      {:ok, updated_db} -> {:ok, updated_db}
      {:error, changeset} -> {:error, Error.validation(changeset: changeset)}
    end
  end

  def delete_db(%PostgresDatabase{} = db) do
    Repo.delete(db)
  end

  # PostgresDatabase runtime

  @spec start_link(%PostgresDatabase{}) :: {:ok, pid()} | {:error, Postgrex.Error.t()}
  def start_link(db, overrides \\ %{})

  def start_link(%PostgresDatabase{} = db, overrides) do
    db
    |> Map.merge(overrides)
    |> PostgresDatabase.to_postgrex_opts()
    |> Postgrex.start_link()
  end

  @spec with_connection(%PostgresDatabase{}, (pid() -> any())) :: any()
  def with_connection(%PostgresDatabase{} = db, fun) do
    Logger.metadata(database_id: db.id)

    case ConnectionCache.existing_connection(db) do
      {:ok, conn} ->
        fun.(conn)

      # Not already started, create a temporary connection
      {:error, %NotFoundError{}} ->
        with {:ok, conn} <- start_link(db) do
          try do
            fun.(conn)
          after
            GenServer.stop(conn)
          end
        end
    end
  end

  @spec test_tcp_reachability(%PostgresDatabase{}, integer()) :: :ok | {:error, Error.t()}
  def test_tcp_reachability(%PostgresDatabase{} = db, timeout \\ 10_000) do
    TcpUtils.test_reachability(db.hostname, db.port, timeout)
  end

  @spec test_connect(%PostgresDatabase{}, integer()) :: :ok | {:error, term()}
  def test_connect(%PostgresDatabase{} = db, timeout \\ 30_000) do
    db
    |> PostgresDatabase.to_postgrex_opts()
    |> Postgrex.Utils.default_opts()
    # Willing to wait this long to get a connection
    |> Keyword.put(:timeout, timeout)
    |> Postgrex.Protocol.connect()
    |> case do
      {:ok, state} ->
        # First argument is supposed to be an Exception, but
        # disconnect doesn't use it.
        # Use a dummy exception for disconnect
        # so there's no dialyzer complaints
        :ok =
          "disconnect"
          |> RuntimeError.exception()
          |> Postgrex.Protocol.disconnect(state)

        :ok

      {:error, error} when is_struct(error, Postgrex.Error) ->
        {:error, error}

      {:error, error} when is_exception(error) ->
        sanitized = db |> Map.from_struct() |> Map.delete(:password)

        Logger.error("Unable to connect to database", error: error, metadata: %{connection_opts: sanitized})

        {:error, error}
    end
  end

  # This query checks on db $1, if user has grant $2
  @db_privilege_query "select has_database_privilege($1, $2);"
  @db_read_only_query "show transaction_read_only;"

  @spec test_permissions(%PostgresDatabase{}) ::
          :ok
          | {:error,
             :database_connect_forbidden
             | :database_create_forbidden
             | :transaction_read_only
             | :unknown_privileges}
  def test_permissions(%PostgresDatabase{} = db) do
    with {:ok, conn} <- start_link(db),
         {:ok, has_connect} <-
           run_test_query(conn, @db_privilege_query, [db.database, "connect"]),
         {:ok, has_create} <-
           run_test_query(conn, @db_privilege_query, [db.database, "create"]),
         {:ok, {:ok, is_read_only}} <-
           Postgrex.transaction(conn, fn conn ->
             run_test_query(conn, @db_read_only_query, [])
           end) do
      case {has_connect, has_create, is_read_only} do
        {true, true, "off"} ->
          :ok

        {false, _, _} ->
          {:error, :database_connect_forbidden}

        {_, false, _} ->
          {:error, :database_create_forbidden}

        {_, _, "on"} ->
          {:error, :transaction_read_only}

        _ ->
          {:error, :unknown_privileges}
      end
    end
  end

  @namespace_exists_query "select exists(select 1 from pg_namespace WHERE nspname = $1);"
  @namespace_privilege_query "select has_schema_privilege($1, $2);"

  def maybe_test_namespace_permissions(%PostgresDatabase{} = db, namespace) do
    with {:ok, conn} <- start_link(db),
         {:ok, namespace_exists} <- run_test_query(conn, @namespace_exists_query, [namespace]) do
      if namespace_exists do
        test_namespace_permissions(conn, namespace)
      else
        :ok
      end
    end
  end

  defp test_namespace_permissions(conn, namespace) do
    with {:ok, has_usage} <-
           run_test_query(conn, @namespace_privilege_query, [namespace, "usage"]),
         {:ok, has_create} <-
           run_test_query(conn, @namespace_privilege_query, [namespace, "create"]) do
      case {has_usage, has_create} do
        {true, true} -> :ok
        {false, _} -> {:error, :namespace_usage_forbidden}
        {_, false} -> {:error, :namespace_create_forbidden}
        _ -> {:error, :unknown_privileges}
      end
    end
  end

  defp run_test_query(conn, query, params) do
    with {:ok, %{rows: [[result]]}} <- Postgrex.query(conn, query, params) do
      {:ok, result}
    end
  end

  def setup_replication(%PostgresDatabase{} = database, slot_name, publication_name, tables) do
    with_connection(database, fn conn ->
      Postgrex.transaction(conn, fn t_conn ->
        with :ok <- create_replication_slot(t_conn, slot_name),
             :ok <- create_publication(t_conn, publication_name, tables) do
          %{slot_name: slot_name, publication_name: publication_name, tables: tables}
        else
          {:error, %Postgrex.Error{} = error} ->
            message = (error.postgres && error.postgres.message) || "Unknown Postgres error"
            code = (error.postgres && error.postgres.code) || "unknown"
            Postgrex.rollback(t_conn, Error.service(service: :external_postgres, message: message, code: code))

          {:error, error} ->
            Logger.error("Failed to setup replication: #{inspect(error)}", error: error)
            Postgrex.rollback(t_conn, error)
        end
      end)
    end)
  end

  defp create_replication_slot(conn, slot_name) do
    # First, check if the slot already exists
    check_query = "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1"

    case Postgrex.query(conn, check_query, [slot_name]) do
      {:ok, %{num_rows: 0}} ->
        # Slot doesn't exist, create it
        # ::text is important, as Postgrex can't handle return type pg_lsn
        create_query = "SELECT pg_create_logical_replication_slot($1, 'pgoutput')::text"

        case Postgrex.query(conn, create_query, [slot_name]) do
          {:ok, _} -> :ok
          {:error, error} -> {:error, error}
        end

      {:ok, _} ->
        # Slot already exists
        :ok

      {:error, error} ->
        {:error, "Failed to check for existing replication slot: #{inspect(error)}"}
    end
  end

  defp create_publication(conn, publication_name, tables) do
    # Check if publication exists
    check_query = "SELECT 1 FROM pg_publication WHERE pubname = $1"

    case Postgrex.query(conn, check_query, [publication_name]) do
      {:ok, %{num_rows: 0}} ->
        # Publication doesn't exist, create it
        table_list = Enum.map_join(tables, ", ", fn [schema, table] -> ~s{"#{schema}"."#{table}"} end)
        create_query = "CREATE PUBLICATION #{publication_name} FOR TABLE #{table_list}"

        case Postgrex.query(conn, create_query, []) do
          {:ok, _} -> :ok
          {:error, error} -> {:error, "Failed to create publication: #{inspect(error)}"}
        end

      {:ok, _} ->
        # Publication already exists
        :ok

      {:error, error} ->
        {:error, "Failed to check for existing publication: #{inspect(error)}"}
    end
  end

  @spec tables(%PostgresDatabase{}) :: {:ok, [PostgresDatabase.Table.t()]} | {:error, term()}
  def tables(%PostgresDatabase{tables: []} = db) do
    with {:ok, updated_db} <- update_tables(db) do
      {:ok, updated_db.tables}
    end
  end

  def tables(%PostgresDatabase{tables: tables}) do
    {:ok, tables}
  end

  @spec update_tables(%PostgresDatabase{}) :: {:ok, %PostgresDatabase{}} | {:error, term()}
  def update_tables(%PostgresDatabase{} = db) do
    with_connection(db, fn conn ->
      update_tables(conn, db)
    end)
  end

  defp update_tables(conn, %PostgresDatabase{} = db) do
    with {:ok, schemas} <- list_schemas(conn),
         {:ok, tables} <- list_tables_for_schemas(conn, schemas) do
      tables = PostgresDatabase.tables_to_map(tables)

      db
      |> PostgresDatabase.changeset(%{tables: tables, tables_updated_at: DateTime.utc_now()})
      |> Repo.update()
    end
  end

  defp list_tables_for_schemas(conn, schemas) do
    Enum.reduce_while(schemas, {:ok, []}, fn schema, {:ok, acc} ->
      case list_tables(conn, schema) do
        {:ok, tables} ->
          schema_tables =
            Enum.map(tables, fn table ->
              %PostgresDatabase.Table{
                oid: Postgres.fetch_table_oid(conn, schema, table),
                schema: schema,
                name: table,
                columns: list_columns(conn, schema, table)
              }
            end)

          {:cont, {:ok, acc ++ schema_tables}}

        error ->
          {:halt, error}
      end
    end)
  end

  defp list_columns(conn, schema, table) do
    with {:ok, rows} <- Postgres.list_columns(conn, schema, table) do
      Enum.map(rows, fn [attnum, name, type, is_pk] ->
        %PostgresDatabase.Table.Column{attnum: attnum, name: name, type: type, is_pk?: is_pk}
      end)
    end
  end

  def list_schemas(%PostgresDatabase{} = database) do
    with_connection(database, &list_schemas/1)
  end

  def list_schemas(conn), do: Postgres.list_schemas(conn)

  def list_tables(%PostgresDatabase{} = database, schema) do
    with_connection(database, &Postgres.list_tables(&1, schema))
  end

  def list_tables(conn, schema), do: Postgres.list_tables(conn, schema)
end

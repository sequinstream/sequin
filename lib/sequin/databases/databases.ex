defmodule Sequin.Databases do
  @moduledoc false
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Error
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

  def get_db_for_account(account_id, id) do
    query =
      account_id
      |> PostgresDatabase.where_account()
      |> PostgresDatabase.where_id(id)

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
    with {:ok, conn} <- start_link(db) do
      try do
        fun.(conn)
      after
        GenServer.stop(conn)
      end
    end
  end

  @spec test_tcp_reachability(%PostgresDatabase{}, integer()) :: :ok | {:error, term()}
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

  def setup_replication(%PostgresDatabase{} = database, slot_name, publication_name) do
    with {:ok, conn} <- start_link(database),
         :ok <- create_replication_slot(conn, slot_name),
         :ok <- create_publication(conn, publication_name) do
      {:ok, %{slot_name: slot_name, publication_name: publication_name}}
    end
  end

  defp create_replication_slot(conn, slot_name) do
    # ::text is important, as Postgrex can't handle return type pg_lsn
    case Postgrex.query(conn, "SELECT pg_create_logical_replication_slot($1, 'pgoutput')::text", [slot_name]) do
      {:ok, _} -> :ok
      {:error, %Postgrex.Error{postgres: %{code: :duplicate_object}}} -> :ok
      {:error, error} -> {:error, "Failed to create replication slot: #{inspect(error)}"}
    end
  end

  defp create_publication(conn, publication_name) do
    case Postgrex.query(conn, "CREATE PUBLICATION #{publication_name} FOR ALL TABLES", []) do
      {:ok, _} -> :ok
      {:error, %Postgrex.Error{postgres: %{code: :duplicate_object}}} -> :ok
      {:error, error} -> {:error, "Failed to create publication: #{inspect(error)}"}
    end
  end
end

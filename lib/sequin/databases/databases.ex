defmodule Sequin.Databases do
  @moduledoc false
  alias Sequin.Databases.ConnectionOpts
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.TcpUtils

  require Logger

  # PostgresDatabase runtime

  @spec start_link(%PostgresDatabase{} | ConnectionOpts.t()) :: {:ok, pid()} | {:error, Postgrex.Error.t()}
  def start_link(opts, overrides \\ %{})

  def start_link(%PostgresDatabase{} = db, overrides) do
    db
    |> connection_opts()
    |> Map.merge(overrides)
    |> start_link()
  end

  def start_link(%ConnectionOpts{} = opts, overrides) do
    opts
    |> Map.merge(overrides)
    |> ConnectionOpts.to_postgrex_opts()
    |> Postgrex.start_link()
  end

  @spec test_tcp_reachability(%PostgresDatabase{} | ConnectionOpts.t()) :: :ok | {:error, term()}
  def test_tcp_reachability(%PostgresDatabase{} = db) do
    db
    |> connection_opts()
    |> test_tcp_reachability()
  end

  # Handles testing when we *do not* have a tunnel
  def test_tcp_reachability(%ConnectionOpts{} = conn_opts) do
    TcpUtils.test_reachability(conn_opts.hostname, conn_opts.port)
  end

  @spec test_connect(%PostgresDatabase{} | ConnectionOpts.t(), integer()) :: :ok | {:error, term()}
  def test_connect(db_or_conn_opts, timeout \\ 30_000)

  def test_connect(%PostgresDatabase{} = db, timeout) do
    db
    |> connection_opts()
    |> test_connect(timeout)
  end

  def test_connect(%ConnectionOpts{} = opts, timeout) do
    opts
    |> ConnectionOpts.to_postgrex_opts()
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
        sanitized = opts |> Map.from_struct() |> Map.delete(:password)

        Logger.error("Unable to connect to database", error: error, metadata: %{connection_opts: sanitized})

        {:error, error}
    end
  end

  # This query checks on db $1, if user has grant $2
  @db_privilege_query "select has_database_privilege($1, $2);"
  @db_read_only_query "show transaction_read_only;"

  @spec test_permissions(%PostgresDatabase{} | ConnectionOpts.t()) ::
          :ok
          | {:error,
             :database_connect_forbidden
             | :database_create_forbidden
             | :transaction_read_only
             | :unknown_privileges}
  def test_permissions(%PostgresDatabase{} = db) do
    db
    |> connection_opts()
    |> test_permissions()
  end

  def test_permissions(%ConnectionOpts{} = opts) do
    with {:ok, conn} <- start_link(opts),
         {:ok, has_connect} <-
           run_test_query(conn, @db_privilege_query, [opts.database, "connect"]),
         {:ok, has_create} <-
           run_test_query(conn, @db_privilege_query, [opts.database, "create"]),
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
    db
    |> connection_opts()
    |> maybe_test_namespace_permissions(namespace)
  end

  def maybe_test_namespace_permissions(%ConnectionOpts{} = opts, namespace) do
    with {:ok, conn} <- start_link(opts),
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

  def connection_opts(%PostgresDatabase{} = db) do
    %ConnectionOpts{
      database: db.dbname,
      hostname: db.host,
      password: db.password,
      port: db.port,
      ssl: Map.get(db, :ssl, false),
      username: db.user,
      # These are optionally set/configured
      pool_size: db.pool_size || 10,
      # qi and qt default to 50ms, 100ms
      queue_interval: db.queue_interval,
      queue_target: db.queue_target
    }
  end
end

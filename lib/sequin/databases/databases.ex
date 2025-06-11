defmodule Sequin.Databases do
  @moduledoc false
  import Ecto.Query, only: [preload: 2]

  alias Sequin.Cache
  alias Sequin.Consumers
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Databases.ConnectionCache
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Databases.PostgresDatabasePrimary
  alias Sequin.Databases.PostgresDatabaseTable
  alias Sequin.Error
  alias Sequin.Error.NotFoundError
  alias Sequin.Health.CheckPostgresReplicationSlotWorker
  alias Sequin.NetworkUtils
  alias Sequin.ObanQuery
  alias Sequin.Postgres
  alias Sequin.Replication
  alias Sequin.Replication.PostgresReplicationSlot
  alias Sequin.Repo
  alias Sequin.Runtime.DatabaseLifecycleEventWorker

  require Logger

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])
  @stream_schema Application.compile_env(:sequin, [Sequin.Repo, :stream_schema_prefix])

  # PostgresDatabase

  def list_dbs do
    Repo.all(PostgresDatabase)
  end

  def list_active_dbs do
    PostgresDatabase.join_replication_slot()
    |> PostgresReplicationSlot.where_status(:active)
    |> Repo.all()
  end

  def list_dbs_for_account(account_id, preload \\ []) do
    account_id
    |> PostgresDatabase.where_account()
    |> preload(^preload)
    |> Repo.all()
  end

  def list_local_tunnel_dbs_for_account(account_id, preload \\ []) do
    account_id
    |> PostgresDatabase.where_account()
    |> PostgresDatabase.where_use_local_tunnel()
    |> preload(^preload)
    |> Repo.all()
  end

  def get_db(id) do
    case Repo.get(PostgresDatabase, id) do
      nil -> {:error, Error.not_found(entity: :postgres_database)}
      db -> {:ok, db}
    end
  end

  def db_names_for_consumer_ids(consumer_ids) do
    consumer_ids
    |> SinkConsumer.where_id_in()
    |> SinkConsumer.join_postgres_database()
    |> Ecto.Query.select([consumer: sc, database: db], {sc.id, db.name})
    |> Repo.all()
    |> Map.new()
  end

  def get_db!(id) do
    Repo.get!(PostgresDatabase, id)
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

  @spec get_cached_db(database_id :: PostgresDatabase.id()) ::
          {:ok, database :: PostgresDatabase.t()} | {:error, Error.t()}
  def get_cached_db(database_id) do
    ttl = Sequin.Time.with_jitter(:timer.seconds(30))

    Cache.get_or_store(
      database_id,
      fn ->
        case get_db(database_id) do
          {:ok, database} ->
            {:ok, database}

          {:error, _} = error ->
            error
        end
      end,
      ttl
    )
  end

  def invalidate_cached_db(database_id) do
    Cache.delete(database_id)
  end

  def create_db(account_id, attrs) do
    Repo.transact(fn ->
      res =
        account_id
        |> create_db_changeset(attrs)
        |> Repo.insert()

      with {:ok, db} <- res,
           {:ok, db} <- update_tables(db) do
        DatabaseLifecycleEventWorker.enqueue(:create, :postgres_database, db.id)
        CheckPostgresReplicationSlotWorker.enqueue(db.id)

        {:ok, db}
      else
        {:error, %Ecto.Changeset{} = changeset} ->
          {:error, Error.validation(changeset: changeset)}

        {:error, %DBConnection.ConnectionError{} = error} ->
          message = Exception.message(error)

          {:error,
           Error.validation(summary: "Failed to connect to database. Please check connection details. (error=#{message})")}

        error ->
          error
      end
    end)
  end

  def create_db_changeset(account_id, attrs) do
    PostgresDatabase.create_changeset(%PostgresDatabase{account_id: account_id}, attrs)
  end

  def update_db(%PostgresDatabase{} = db, attrs) do
    Repo.transact(fn ->
      res =
        db
        |> PostgresDatabase.update_changeset(attrs)
        |> Repo.update()

      case res do
        {:ok, updated_db} ->
          CheckPostgresReplicationSlotWorker.enqueue(updated_db.id)
          DatabaseLifecycleEventWorker.enqueue(:update, :postgres_database, updated_db.id)
          {:ok, updated_db}

        {:error, changeset} ->
          {:error, Error.validation(changeset: changeset)}
      end
    end)
  end

  @doc """
  Creates a PostgresDatabase and its associated PostgresReplicationSlot transactionally.
  If primary database parameters are provided, also creates a PostgresDatabasePrimary record.
  """
  def create_db_with_slot(account_id, db_params, replication_params) do
    Repo.transact(fn ->
      with {:ok, db} <- create_db(account_id, db_params),
           replication_params = Map.put(replication_params, "postgres_database_id", db.id),
           {:ok, replication} <-
             Replication.create_pg_replication(
               account_id,
               replication_params
             ) do
        db_with_associations = Repo.preload(db, [:replication_slot])
        {:ok, %PostgresDatabase{db_with_associations | replication_slot: replication}}
      end
    end)
  end

  @doc """
  Updates a PostgresDatabase and its associated PostgresReplicationSlot transactionally.
  """
  def update_db_with_slot(%PostgresDatabase{} = database, db_params, replication_params) do
    Repo.transact(fn ->
      with {:ok, updated_db} <- update_db(database, db_params),
           # Preload the slot association after successful update for the next step
           replication_slot = Repo.preload(updated_db, :replication_slot).replication_slot,
           {:ok, replication} <- Replication.update_pg_replication(replication_slot, replication_params) do
        {:ok, %PostgresDatabase{updated_db | replication_slot: replication}}
      end
    end)
  end

  def delete_db(%PostgresDatabase{} = db) do
    Repo.delete(db)
  end

  def delete_db_with_replication_slot(%PostgresDatabase{} = db) do
    res =
      Repo.transact(fn ->
        db = Repo.preload(db, [:replication_slot])

        health_checker_query =
          Oban.Job
          |> ObanQuery.where_args(%{postgres_database_id: db.id})
          |> ObanQuery.where_worker(Sequin.Health.CheckPostgresReplicationSlotWorker)

        # Check for related entities that need to be removed first
        with :ok <- check_related_entities(db),
             {:ok, _} <- Replication.delete_pg_replication(db.replication_slot),
             {:ok, _} <- Repo.delete(db),
             {:ok, _} <- Oban.cancel_all_jobs(health_checker_query) do
          DatabaseLifecycleEventWorker.enqueue(:delete, :postgres_database, db.id, %{
            replication_slot_id: db.replication_slot.id
          })

          :ok
        end
      end)

    with {:ok, _res} <- res do
      :ok
    end
  end

  defp check_related_entities(db) do
    sink_consumers = Consumers.list_consumers_for_replication_slot(db.replication_slot.id)
    wal_pipelines = Replication.list_wal_pipelines_for_replication_slot(db.replication_slot.id)

    cond do
      sink_consumers != [] ->
        {:error,
         Error.validation(
           summary: "Cannot delete database that's used by sink consumers. Please delete associated sink consumers first."
         )}

      wal_pipelines != [] ->
        {:error,
         Error.validation(
           summary: "Cannot delete database that's used by WAL pipelines. Please delete associated WAL pipelines first."
         )}

      true ->
        :ok
    end
  end

  def reject_sequin_internal_tables(tables) do
    Enum.reject(tables, fn %PostgresDatabaseTable{} = table ->
      in_sequin_schema? = table.schema != "public" and table.schema in [@config_schema, @stream_schema]
      is_logical_messages_table? = table.name == Sequin.Constants.logical_messages_table_name()

      in_sequin_schema? or is_logical_messages_table?
    end)
  end

  def print_connection_url(id) do
    with {:ok, db} <- get_db(id) do
      url = PostgresDatabase.connection_url(db)
      IO.puts("'#{url}'")
    end
  end

  @spec start_link(%PostgresDatabase{}) :: {:ok, pid()} | {:error, Postgrex.Error.t()}
  def start_link(db, overrides \\ %{})

  def start_link(%PostgresDatabase{} = db, overrides) do
    db
    |> Map.merge(overrides)
    |> PostgresDatabase.to_postgrex_opts()
    |> Postgrex.start_link()
  end

  def with_connection(%PostgresDatabase{} = db, fun) do
    Logger.metadata(database_id: db.id)

    case ConnectionCache.existing_connection(db) do
      {:ok, conn} ->
        fun.(conn)

      # Not already started, create a temporary connection
      {:error, %NotFoundError{}} ->
        with_uncached_connection(db, fun)
    end
  end

  def with_uncached_connection(%PostgresDatabase{} = db, fun) do
    with {:ok, conn} <- start_link(db, %{pool_size: 1}) do
      try do
        fun.(conn)
      after
        GenServer.stop(conn)
      end
    end
  end

  @spec test_tcp_reachability(%PostgresDatabase{}, integer()) :: :ok | {:error, Error.t()}
  def test_tcp_reachability(%PostgresDatabase{} = db, timeout \\ 10_000) do
    NetworkUtils.test_tcp_reachability(db.hostname, db.port, db.ipv6, timeout)
  end

  @spec test_connect(%PostgresDatabase{} | %PostgresDatabasePrimary{}, integer()) :: :ok | {:error, term()}
  def test_connect(db, timeout \\ 30_000)

  def test_connect(%PostgresDatabasePrimary{} = db, timeout) do
    test_connect(PostgresDatabase.from_primary(db), timeout)
  end

  def test_connect(%PostgresDatabase{} = db, timeout) do
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
        {:error, error}
    end
  end

  # This query checks on db $1, if user has grant $2
  @db_privilege_query "select has_database_privilege($1, $2);"

  @spec test_permissions(%PostgresDatabase{}) :: :ok | {:error, Error.ValidationError.t()} | {:error, Postgrex.Error.t()}
  def test_permissions(%PostgresDatabase{} = db) do
    with_uncached_connection(db, fn conn ->
      with {:ok, %{rows: [[result]]}} <- Postgres.query(conn, @db_privilege_query, [db.database, "connect"]) do
        if result do
          :ok
        else
          {:error, Error.validation(summary: "User does not have connect permission on database")}
        end
      end
    end)
  end

  def test_maybe_replica(%PostgresDatabase{} = db, %PostgresDatabasePrimary{} = db_primary) do
    test_maybe_replica(db, PostgresDatabase.from_primary(db_primary))
  end

  def test_maybe_replica(%PostgresDatabase{} = db, db_primary) do
    cond do
      not physical_replica?(db) ->
        :ok

      is_nil(db_primary) ->
        summary = """
        Primary connection parameters are required for replica databases. Edit the database to add primary connection details.
        """

        {:error, Error.validation(summary: summary)}

      not match?({:ok, version} when version >= 16, get_pg_major_version(db)) ->
        {:error, Error.validation(summary: "PostgreSQL version 16 or higher is required for replica databases")}

      true ->
        case test_connect(db_primary) do
          :ok ->
            :ok

          {:error, ex} when is_exception(ex) ->
            {:error, Error.validation(summary: "Failed to connect to primary database: #{Exception.message(ex)}")}

          {:error, error} ->
            {:error, Error.validation(summary: "Failed to connect to primary database: #{inspect(error)}")}
        end
    end
  end

  def verify_slot(%PostgresDatabase{} = database, %PostgresReplicationSlot{} = slot) do
    with_uncached_connection(database, fn conn ->
      with {:ok, _} <- Postgres.get_publication(conn, slot.publication_name),
           {:ok, slot_info} <- Postgres.fetch_replication_slot(conn, slot.slot_name),
           :ok <- validate_slot(database, slot_info) do
        Postgres.check_replication_permissions(conn)
      end
    end)
  end

  defp validate_slot(%PostgresDatabase{} = db, slot_info) do
    if Map.get(slot_info, "database") == db.database do
      :ok
    else
      {:error, Error.validation(summary: "Replication slot was created in a different logical database")}
    end
  end

  def get_pg_major_version(%PostgresDatabase{} = database) do
    with_uncached_connection(database, fn conn ->
      Postgres.get_pg_major_version(conn)
    end)
  end

  def verify_table_in_publication(%PostgresDatabase{} = database, table_oid) do
    database = Repo.preload(database, :replication_slot)
    pub_name = database.replication_slot.publication_name

    with_connection(database, fn conn ->
      Postgres.verify_table_in_publication(conn, pub_name, table_oid)
    end)
  end

  def check_replica_identity(%PostgresDatabase{} = db, oid) do
    with_connection(db, fn conn ->
      Postgres.check_replica_identity(conn, oid)
    end)
  end

  @spec tables(%PostgresDatabase{}) :: {:ok, [PostgresDatabaseTable.t()]} | {:error, term()}
  def tables(%PostgresDatabase{tables: []} = db) do
    with {:ok, updated_db} <- update_tables(db) do
      {:ok, updated_db.tables}
    end
  end

  def tables(%PostgresDatabase{tables: tables}) do
    {:ok, tables}
  end

  def fetch_table(%PostgresDatabase{tables: tables}, oid) do
    case Enum.find(tables, fn t -> t.oid == oid end) do
      nil -> {:error, Error.not_found(entity: :table, params: [oid: oid])}
      table -> {:ok, table}
    end
  end

  @spec update_pg_major_version(%PostgresDatabase{}) :: {:ok, %PostgresDatabase{}} | {:error, term()}
  def update_pg_major_version(%PostgresDatabase{} = db) do
    current_pg_major_version = db.pg_major_version

    with {:ok, pg_major_version} <- get_pg_major_version(db) do
      if current_pg_major_version == pg_major_version do
        {:ok, db}
      else
        # If the pg_major_version is different, update the database and emit lifecycle events
        update_db(db, %{pg_major_version: pg_major_version})
      end
    end
  end

  @spec update_tables(%PostgresDatabase{}) :: {:ok, %PostgresDatabase{}} | {:error, term()}
  def update_tables(%PostgresDatabase{} = db) do
    with_connection(db, fn conn ->
      update_tables(conn, db)
    end)
  end

  defp update_tables(conn, %PostgresDatabase{} = db) do
    with {:ok, tables} <- list_tables(conn) do
      db
      |> PostgresDatabase.changeset(%{tables: tables, tables_refreshed_at: DateTime.utc_now()})
      |> Repo.update()
    end
  end

  @spec physical_replica?(%PostgresDatabase{}) :: boolean()
  def physical_replica?(%PostgresDatabase{} = db) do
    with_uncached_connection(db, fn conn ->
      query = """
        SELECT pg_is_in_recovery() AS is_physical_replica
      """

      case Postgres.query(conn, query, []) do
        {:ok, %{rows: [[is_physical]]}} ->
          is_physical

        _ ->
          false
      end
    end)
  end

  def update_tables!(%PostgresDatabase{} = db) do
    case update_tables(db) do
      {:ok, db} -> db
      {:error, error} -> raise error
    end
  end

  def list_tables(conn) do
    with {:ok, schemas} when schemas != [] <- list_schemas(conn),
         {:ok, tables} <- Postgres.fetch_tables_with_columns(conn, schemas) do
      {:ok, Postgres.tables_to_map(tables)}
    else
      {:ok, []} ->
        {:error,
         Error.service(
           service: :postgres,
           message:
             "List tables: Unable to list schemas in database. Does Sequin have permissions (`usage`) for any schemas?"
         )}

      error ->
        error
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

defmodule Sequin.Databases do
  @moduledoc false
  import Ecto.Query, only: [preload: 2]

  alias Sequin.Consumers
  alias Sequin.Databases.ConnectionCache
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Databases.PostgresDatabaseTable
  alias Sequin.Databases.Sequence
  alias Sequin.DatabasesRuntime.LifecycleEventWorker
  alias Sequin.Error
  alias Sequin.Error.NotFoundError
  alias Sequin.Health.CheckPostgresReplicationSlotWorker
  alias Sequin.NetworkUtils
  alias Sequin.ObanQuery
  alias Sequin.Postgres
  alias Sequin.Replication
  alias Sequin.Replication.PostgresReplicationSlot
  alias Sequin.Repo

  require Logger

  # PostgresDatabase

  def list_dbs do
    Repo.all(PostgresDatabase)
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

  def create_db(account_id, attrs) do
    Repo.transact(fn ->
      res =
        %PostgresDatabase{account_id: account_id}
        |> PostgresDatabase.changeset(attrs)
        |> Repo.insert()

      with {:ok, db} <- res,
           {:ok, db} <- update_tables(db) do
        CheckPostgresReplicationSlotWorker.enqueue(db.id)

        {:ok, db}
      else
        {:error, %Ecto.Changeset{} = changeset} ->
          {:error, Error.validation(changeset: changeset)}

        error ->
          error
      end
    end)
  end

  def update_db(%PostgresDatabase{} = db, attrs) do
    Repo.transact(fn ->
      res =
        db
        |> PostgresDatabase.changeset(attrs)
        |> Repo.update()

      case res do
        {:ok, updated_db} ->
          CheckPostgresReplicationSlotWorker.enqueue(updated_db.id, unique: false)
          LifecycleEventWorker.enqueue(:update, :postgres_database, updated_db.id)
          {:ok, updated_db}

        {:error, changeset} ->
          {:error, Error.validation(changeset: changeset)}
      end
    end)
  end

  def delete_db(%PostgresDatabase{} = db) do
    Repo.delete(db)
  end

  def delete_db_with_replication_slot(%PostgresDatabase{} = db) do
    Repo.transact(fn ->
      db = Repo.preload(db, [:replication_slot, :sequences])

      health_checker_query =
        Oban.Job
        |> ObanQuery.where_args(%{postgres_database_id: db.id})
        |> ObanQuery.where_worker(Sequin.Health.CheckPostgresReplicationSlotWorker)

      # Check for related entities that need to be removed first
      with :ok <- check_related_entities(db),
           {:ok, _} <- Replication.delete_pg_replication(db.replication_slot),
           {:ok, _} <- delete_sequences(db),
           {:ok, _} <- Repo.delete(db),
           {:ok, _} <- Oban.cancel_all_jobs(health_checker_query) do
        LifecycleEventWorker.enqueue(:delete, :postgres_database, db.id, %{replication_slot_id: db.replication_slot.id})
        :ok
      end
    end)
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

  def print_connection_url(id) do
    with {:ok, db} <- get_db(id) do
      url = PostgresDatabase.connection_url(db)
      IO.puts("'#{url}'")
    end
  end

  # Sequences

  def find_sequence_for_account(account_id, params \\ []) do
    params
    |> Enum.reduce(Sequence.where_account(account_id), fn
      {:id, id}, query ->
        Sequence.where_id(query, id)

      {:table_schema, table_schema}, query ->
        Sequence.where_table_schema(query, table_schema)

      {:table_name, table_name}, query ->
        Sequence.where_table_name(query, table_name)

      {:name, name}, query ->
        Sequence.where_name(query, name)

      {:table_oid, table_oid}, query ->
        Sequence.where_table_oid(query, table_oid)

      {:postgres_database_id, postgres_database_id}, query ->
        Sequence.where_postgres_database_id(query, postgres_database_id)
    end)
    |> Repo.one()
    |> case do
      nil -> {:error, Error.not_found(entity: :sequence)}
      sequence -> {:ok, sequence}
    end
  end

  def list_sequences_for_account(account_id, preload \\ []) do
    account_id
    |> Sequence.where_account()
    |> preload(^preload)
    |> Repo.all()
  end

  def create_sequence(account_id, attrs) do
    %Sequence{account_id: account_id}
    |> Sequence.changeset(attrs)
    |> Repo.insert()
  end

  def delete_sequence(%Sequence{} = sequence) do
    case Consumers.list_consumers_for_sequence(sequence.id) do
      [] ->
        Repo.delete(sequence)

      _ ->
        {:error, Error.invariant(message: "Cannot delete stream that's used by consumers")}
    end
  end

  def delete_sequences(%PostgresDatabase{} = db) do
    Repo.transact(fn ->
      db.id
      |> Sequence.where_postgres_database_id()
      |> Repo.all()
      |> Enum.reduce_while({:ok, 0}, fn sequence, {:ok, count} ->
        case delete_sequence(sequence) do
          {:ok, _} -> {:cont, {:ok, count + 1}}
          error -> {:halt, error}
        end
      end)
    end)
  end

  def update_sequences_from_db(%PostgresDatabase{} = db) do
    Enum.reduce(Repo.preload(db, :sequences).sequences, {0, 0}, fn %Sequence{} = sequence, {ok_count, error_count} ->
      case update_sequence_from_db(sequence, db) do
        {:ok, _} ->
          {ok_count + 1, error_count}

        {:error, error} ->
          Logger.error("Failed to update stream #{sequence.id} from database: #{inspect(error)}", error: error)
          {ok_count, error_count + 1}
      end
    end)
  end

  @doc """
  Updates a sequence given a database's table schema. Adds names for table_schema, table_name, and sort_column_name.
  These can drift if the customer migrates the database.
  """
  def update_sequence_from_db(%Sequence{} = sequence, %PostgresDatabase{} = db) do
    with {:ok, tables} <- tables(db) do
      table = Enum.find(tables, fn t -> t.oid == sequence.table_oid end)
      column = table && Enum.find(table.columns, fn c -> c.attnum == sequence.sort_column_attnum end)

      case {table, column} do
        {nil, _} ->
          {:error, Error.not_found(entity: :table, params: %{oid: sequence.table_oid})}

        {_, nil} ->
          {:error, Error.not_found(entity: :column, params: %{attnum: sequence.sort_column_attnum})}

        {table, column} ->
          sequence
          |> Sequence.changeset(%{
            table_schema: table.schema,
            table_name: table.name,
            sort_column_name: column.name
          })
          |> Repo.update()
      end
    end
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
    with {:ok, conn} <- start_link(db) do
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

  def get_major_pg_version(%PostgresDatabase{} = database) do
    with_uncached_connection(database, fn conn ->
      Postgres.get_major_pg_version(conn)
    end)
  end

  def verify_table_in_publication(%PostgresDatabase{} = database, table_oid) do
    database = Repo.preload(database, :replication_slot)
    pub_name = database.replication_slot.publication_name

    with_connection(database, fn conn ->
      Postgres.verify_table_in_publication(conn, pub_name, table_oid)
    end)
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

    case Postgres.query(conn, check_query, [slot_name]) do
      {:ok, %{num_rows: 0}} ->
        # Slot doesn't exist, create it
        # ::text is important, as Postgrex can't handle return type pg_lsn
        create_query = "SELECT pg_create_logical_replication_slot($1, 'pgoutput')::text"

        case Postgres.query(conn, create_query, [slot_name]) do
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

    case Postgres.query(conn, check_query, [publication_name]) do
      {:ok, %{num_rows: 0}} ->
        # Publication doesn't exist, create it
        table_list = Enum.map_join(tables, ", ", fn [schema, table] -> ~s{"#{schema}"."#{table}"} end)

        create_query =
          "CREATE PUBLICATION #{publication_name} FOR TABLE #{table_list} WITH (publish_via_partition_root = true)"

        case Postgres.query(conn, create_query, []) do
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

  @spec update_tables(%PostgresDatabase{}) :: {:ok, %PostgresDatabase{}} | {:error, term()}
  def update_tables(%PostgresDatabase{} = db) do
    with_connection(db, fn conn ->
      update_tables(conn, db)
    end)
  end

  defp update_tables(conn, %PostgresDatabase{} = db) do
    with {:ok, tables} <- list_tables(conn) do
      res =
        db
        |> PostgresDatabase.changeset(%{tables: tables, tables_refreshed_at: DateTime.utc_now()})
        |> Repo.update()

      case res do
        {:ok, db} ->
          update_sequences_from_db(db)

          {:ok, db}

        error ->
          error
      end
    end
  end

  def update_tables!(%PostgresDatabase{} = db) do
    case update_tables(db) do
      {:ok, db} -> db
      {:error, error} -> raise error
    end
  end

  def list_tables(conn) do
    with {:ok, schemas} <- list_schemas(conn),
         {:ok, tables} <- Postgres.fetch_tables_with_columns(conn, schemas) do
      {:ok, Postgres.tables_to_map(tables)}
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

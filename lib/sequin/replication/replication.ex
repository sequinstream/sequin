defmodule Sequin.Replication do
  @moduledoc false
  import Ecto.Query

  alias Sequin.Databases
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Error
  alias Sequin.Error.NotFoundError
  alias Sequin.Extensions.Replication, as: ReplicationExt
  alias Sequin.Postgres
  alias Sequin.Replication.PostgresReplicationSlot
  alias Sequin.Replication.WalEvent
  alias Sequin.Replication.WalProjection
  alias Sequin.ReplicationRuntime
  alias Sequin.Repo

  require Logger

  # PostgresReplicationSlot

  def all_pg_replications do
    Repo.all(PostgresReplicationSlot)
  end

  def all_active_pg_replications do
    Repo.all(PostgresReplicationSlot)
  end

  def list_pg_replications_for_account(account_id) do
    Repo.all(PostgresReplicationSlot.where_account(account_id))
  end

  def get_pg_replication(id) do
    case Repo.get(PostgresReplicationSlot, id, preload: :postgres_database) do
      nil -> {:error, Error.not_found(entity: :pg_replication, params: %{id: id})}
      pg_replication -> {:ok, pg_replication}
    end
  end

  def get_pg_replication_for_account(account_id, id) do
    case Repo.get_by(PostgresReplicationSlot, id: id, account_id: account_id) do
      nil -> {:error, Error.not_found(entity: :pg_replication)}
      pg_replication -> {:ok, pg_replication}
    end
  end

  def create_pg_replication_for_account_with_lifecycle(account_id, attrs) do
    attrs = Sequin.Map.atomize_keys(attrs)
    attrs = Map.put(attrs, :status, :active)

    with {:ok, postgres_database} <- get_or_build_postgres_database(account_id, attrs),
         :ok <- validate_replication_config(postgres_database, attrs) do
      pg_replication =
        %PostgresReplicationSlot{account_id: account_id}
        |> PostgresReplicationSlot.create_changeset(attrs)
        |> Repo.insert()

      case pg_replication do
        {:ok, pg_replication} ->
          unless Application.get_env(:sequin, :env) == :test do
            ReplicationRuntime.Supervisor.start_replication(pg_replication)
          end

          {:ok, pg_replication}

        error ->
          error
      end
    else
      {:error, %NotFoundError{}} ->
        {:error, Error.validation(summary: "Database with id #{attrs[:postgres_database_id]} not found")}

      error ->
        error
    end
  end

  def update_pg_replication(%PostgresReplicationSlot{} = pg_replication, attrs) do
    pg_replication
    |> PostgresReplicationSlot.update_changeset(attrs)
    |> Repo.update()
  end

  def delete_pg_replication(%PostgresReplicationSlot{} = pg_replication) do
    Repo.delete(pg_replication)
  end

  def delete_pg_replication_with_lifecycle(%PostgresReplicationSlot{} = pg_replication) do
    res = Repo.delete(pg_replication)
    ReplicationRuntime.Supervisor.stop_replication(pg_replication)
    res
  end

  def add_info(%PostgresReplicationSlot{} = pg_replication) do
    pg_replication = Repo.preload(pg_replication, [:postgres_database])

    last_committed_at = ReplicationExt.get_last_committed_at(pg_replication.id)
    # key_pattern = "#{pg_replication.postgres_database.name}.>"

    # total_ingested_messages =
    #   Streams.fast_count_messages_for_stream(pg_replication.stream_id, key_pattern: key_pattern)

    info = %PostgresReplicationSlot.Info{
      last_committed_at: last_committed_at,
      total_ingested_messages: nil
    }

    %{pg_replication | info: info}
  end

  # Helper Functions

  defp get_or_build_postgres_database(account_id, attrs) do
    case attrs do
      %{postgres_database_id: id} ->
        Databases.get_db_for_account(account_id, id)

      %{postgres_database: db_attrs} ->
        db_attrs = Sequin.Map.atomize_keys(db_attrs)
        {:ok, struct(PostgresDatabase, Map.put(db_attrs, :account_id, account_id))}

      _ ->
        {:error, Error.validation(summary: "Missing postgres_database_id or postgres_database")}
    end
  end

  def validate_replication_config(%PostgresDatabase{} = db, attrs) do
    Databases.with_connection(db, fn conn ->
      with :ok <- validate_slot(conn, attrs.slot_name) do
        validate_publication(conn, attrs.publication_name)
      end
    end)
  end

  defp validate_slot(conn, slot_name) do
    query = "select 1 from pg_replication_slots where slot_name = $1"

    case Postgres.query(conn, query, [slot_name]) do
      {:ok, %{num_rows: 1}} ->
        :ok

      {:ok, %{num_rows: 0}} ->
        {:error, Error.validation(summary: "Replication slot `#{slot_name}` does not exist")}

      _ ->
        {:error,
         Error.validation(summary: "Error connecting to the database to verify the existence of the replication slot.")}
    end
  end

  defp validate_publication(conn, publication_name) do
    query = "select 1 from pg_publication where pubname = $1"

    case Postgres.query(conn, query, [publication_name]) do
      {:ok, %{num_rows: 1}} ->
        :ok

      {:ok, %{num_rows: 0}} ->
        {:error, Error.validation(summary: "Publication `#{publication_name}` does not exist")}

      _ ->
        {:error,
         Error.validation(summary: "Error connecting to the database to verify the existence of the publication.")}
    end
  end

  # WAL Projection

  def list_wal_projections do
    Repo.all(WalProjection)
  end

  def list_wal_projections_for_replication_slot(replication_slot_id) do
    replication_slot_id
    |> WalProjection.where_replication_slot_id()
    |> Repo.all()
  end

  def get_wal_projection(id) do
    case Repo.get(WalProjection, id) do
      nil -> {:error, Error.not_found(entity: :wal_projection)}
      wal_projection -> {:ok, wal_projection}
    end
  end

  def get_wal_projection!(id) do
    case get_wal_projection(id) do
      {:ok, wal_projection} -> wal_projection
      {:error, _} -> raise Error.not_found(entity: :wal_projection)
    end
  end

  def create_wal_projection(attrs) do
    %WalProjection{}
    |> WalProjection.create_changeset(attrs)
    |> Repo.insert()
  end

  def create_wal_projection_with_lifecycle(account_id, attrs) do
    with {:ok, wal_projection} <-
           %WalProjection{account_id: account_id}
           |> WalProjection.create_changeset(attrs)
           |> Repo.insert(),
         :ok <- notify_wal_projection_create(wal_projection) do
      {:ok, wal_projection}
    end
  end

  def update_wal_projection(%WalProjection{} = wal_projection, attrs) do
    wal_projection
    |> WalProjection.update_changeset(attrs)
    |> Repo.update()
  end

  def update_wal_projection_with_lifecycle(%WalProjection{} = wal_projection, attrs) do
    with {:ok, updated_wal_projection} <- update_wal_projection(wal_projection, attrs),
         :ok <- notify_wal_projection_update(updated_wal_projection) do
      {:ok, updated_wal_projection}
    end
  end

  def delete_wal_projection(%WalProjection{} = wal_projection) do
    Repo.delete(wal_projection)
  end

  def delete_wal_projection_with_lifecycle(%WalProjection{} = wal_projection) do
    with {:ok, deleted_wal_projection} <- delete_wal_projection(wal_projection),
         :ok <- notify_wal_projection_delete(deleted_wal_projection) do
      {:ok, deleted_wal_projection}
    end
  end

  # Lifecycle Notifications

  defp notify_wal_projection_create(_wal_projection), do: :ok
  defp notify_wal_projection_update(_wal_projection), do: :ok
  defp notify_wal_projection_delete(_wal_projection), do: :ok

  # WAL Event

  def get_wal_event(wal_projection_id, commit_lsn) do
    wal_event =
      wal_projection_id
      |> WalEvent.where_wal_projection_id()
      |> WalEvent.where_commit_lsn(commit_lsn)
      |> Repo.one()

    case wal_event do
      nil -> {:error, Error.not_found(entity: :wal_event)}
      wal_event -> {:ok, wal_event}
    end
  end

  def get_wal_event!(wal_projection_id, commit_lsn) do
    case get_wal_event(wal_projection_id, commit_lsn) do
      {:ok, wal_event} -> wal_event
      {:error, _} -> raise Error.not_found(entity: :wal_event)
    end
  end

  def list_wal_events(wal_projection_id, params \\ []) do
    base_query = WalEvent.where_wal_projection_id(wal_projection_id)

    query =
      Enum.reduce(params, base_query, fn
        {:limit, limit}, query ->
          limit(query, ^limit)

        {:offset, offset}, query ->
          offset(query, ^offset)

        {:order_by, order_by}, query ->
          order_by(query, ^order_by)
      end)

    Repo.all(query)
  end

  def insert_wal_events([]), do: {:ok, 0}

  def insert_wal_events(wal_events) do
    now = DateTime.utc_now()

    events =
      Enum.map(wal_events, fn event ->
        event
        |> Map.merge(%{
          updated_at: now,
          inserted_at: now
        })
        |> Sequin.Map.from_ecto()
      end)

    {count, _} = Repo.insert_all(WalEvent, events)

    {:ok, count}
  end

  def delete_wal_events(wal_event_ids) when is_list(wal_event_ids) do
    query = from(we in WalEvent, where: we.id in ^wal_event_ids)
    {count, _} = Repo.delete_all(query)
    {:ok, count}
  end
end

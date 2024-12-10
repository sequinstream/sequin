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
  alias Sequin.Replication.WalPipeline
  alias Sequin.ReplicationRuntime
  alias Sequin.ReplicationRuntime.Supervisor, as: ReplicationSupervisor
  alias Sequin.Repo

  require Logger

  # PostgresReplicationSlot

  def all_pg_replications do
    Repo.all(PostgresReplicationSlot)
  end

  def all_active_pg_replications do
    Repo.all(PostgresReplicationSlot.where_status(:active))
  end

  def list_pg_replications_for_account(account_id) do
    account_id
    |> PostgresReplicationSlot.where_account()
    |> Repo.all()
  end

  def get_pg_replication(id) do
    case Repo.get(PostgresReplicationSlot, id, preload: :postgres_database) do
      nil -> {:error, Error.not_found(entity: :pg_replication, params: %{id: id})}
      pg_replication -> {:ok, pg_replication}
    end
  end

  def get_pg_replication!(id) do
    case get_pg_replication(id) do
      {:ok, pg_replication} -> pg_replication
      {:error, _} -> raise Error.not_found(entity: :pg_replication)
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
          # We skip the start when creating the replication slot inside a transaction, because
          # the transaction might be rolled back, leaving a zombie process.
          unless Application.get_env(:sequin, :env) == :test or Repo.in_transaction?() do
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

  # Replication runtime lifecycle
  def put_last_processed_seq!(replication_slot_id, seq) do
    Redix.command!(:redix, ["SET", last_processed_seq_key(replication_slot_id), seq])
  end

  def last_processed_seq(replication_slot_id) do
    case Redix.command(:redix, ["GET", last_processed_seq_key(replication_slot_id)]) do
      {:ok, nil} -> {:ok, -1}
      {:ok, seq} -> {:ok, String.to_integer(seq)}
      error -> error
    end
  end

  defp last_processed_seq_key(replication_slot_id), do: "sequin:replication:last_processed_seq:#{replication_slot_id}"

  # WAL Pipeline

  def list_wal_pipelines do
    Repo.all(WalPipeline)
  end

  def list_wal_pipelines_for_account(account_id, preloads \\ []) do
    account_id
    |> WalPipeline.where_account_id()
    |> preload(^preloads)
    |> Repo.all()
  end

  def list_wal_pipelines_for_replication_slot(replication_slot_id) do
    replication_slot_id
    |> WalPipeline.where_replication_slot_id()
    |> Repo.all()
  end

  def get_wal_pipeline_for_account(account_id, id_or_name, preloads \\ []) do
    account_id
    |> WalPipeline.where_account_id()
    |> WalPipeline.where_id_or_name(id_or_name)
    |> preload(^preloads)
    |> Repo.one()
  end

  def find_wal_pipeline_for_account(account_id, params \\ []) do
    params
    |> Enum.reduce(WalPipeline.where_account_id(account_id), fn
      {:name, name}, query ->
        WalPipeline.where_name(query, name)

      {:id, id}, query ->
        WalPipeline.where_id(query, id)
    end)
    |> Repo.one()
    |> case do
      nil -> {:error, Error.not_found(entity: :wal_pipeline, params: params)}
      wal_pipeline -> {:ok, wal_pipeline}
    end
  end

  def get_wal_pipeline(id) do
    case Repo.get(WalPipeline, id) do
      nil -> {:error, Error.not_found(entity: :wal_pipeline)}
      wal_pipeline -> {:ok, wal_pipeline}
    end
  end

  def get_wal_pipeline!(id) do
    case get_wal_pipeline(id) do
      {:ok, wal_pipeline} -> wal_pipeline
      {:error, _} -> raise Error.not_found(entity: :wal_pipeline)
    end
  end

  def create_wal_pipeline(account_id, attrs) do
    %WalPipeline{account_id: account_id}
    |> WalPipeline.create_changeset(attrs)
    |> Repo.insert()
  end

  def create_wal_pipeline_with_lifecycle(account_id, attrs) do
    with {:ok, wal_pipeline} <-
           %WalPipeline{account_id: account_id}
           |> WalPipeline.create_changeset(attrs)
           |> Repo.insert(),
         :ok <- notify_wal_pipeline_changed(wal_pipeline) do
      {:ok, wal_pipeline}
    end
  end

  def update_wal_pipeline(%WalPipeline{} = wal_pipeline, attrs) do
    wal_pipeline
    |> WalPipeline.update_changeset(attrs)
    |> Repo.update()
  end

  def update_wal_pipeline_with_lifecycle(%WalPipeline{} = wal_pipeline, attrs) do
    with {:ok, updated_wal_pipeline} <- update_wal_pipeline(wal_pipeline, attrs),
         :ok <- notify_wal_pipeline_changed(updated_wal_pipeline) do
      {:ok, updated_wal_pipeline}
    end
  end

  def delete_wal_pipeline(%WalPipeline{} = wal_pipeline) do
    Repo.delete(wal_pipeline)
  end

  def delete_wal_pipeline_with_lifecycle(%WalPipeline{} = wal_pipeline) do
    with {:ok, deleted_wal_pipeline} <- delete_wal_pipeline(wal_pipeline),
         :ok <- notify_wal_pipeline_changed(deleted_wal_pipeline) do
      {:ok, deleted_wal_pipeline}
    end
  end

  # Lifecycle Notifications

  defp notify_wal_pipeline_changed(%WalPipeline{} = pipeline) do
    pipeline = Repo.preload(pipeline, :replication_slot)

    unless env() == :test do
      ReplicationSupervisor.restart_wal_pipeline_servers(pipeline.replication_slot)
    end

    ReplicationSupervisor.refresh_message_handler_ctx(pipeline.replication_slot_id)
  end

  # WAL Event

  def get_wal_event(wal_pipeline_id, commit_lsn) do
    wal_event =
      wal_pipeline_id
      |> WalEvent.where_wal_pipeline_id()
      |> WalEvent.where_commit_lsn(commit_lsn)
      |> Repo.one()

    case wal_event do
      nil -> {:error, Error.not_found(entity: :wal_event)}
      wal_event -> {:ok, wal_event}
    end
  end

  def get_wal_event!(wal_pipeline_id, commit_lsn) do
    case get_wal_event(wal_pipeline_id, commit_lsn) do
      {:ok, wal_event} -> wal_event
      {:error, _} -> raise Error.not_found(entity: :wal_event)
    end
  end

  def list_wal_events(wal_pipeline_id_or_ids, params \\ []) do
    wal_pipeline_id_or_ids
    |> list_wal_events_query(params)
    |> Repo.all()
  end

  def list_wal_events_query(wal_pipeline_id_or_ids, params \\ []) do
    base_query =
      if is_list(wal_pipeline_id_or_ids) do
        WalEvent.where_wal_pipeline_id_in(wal_pipeline_id_or_ids)
      else
        WalEvent.where_wal_pipeline_id(wal_pipeline_id_or_ids)
      end

    Enum.reduce(params, base_query, fn
      {:limit, limit}, query ->
        limit(query, ^limit)

      {:offset, offset}, query ->
        offset(query, ^offset)

      {:order_by, order_by}, query ->
        order_by(query, ^order_by)
    end)
  end

  def insert_wal_events([]), do: {:ok, 0}

  def insert_wal_events(wal_events) do
    now = DateTime.truncate(DateTime.utc_now(), :second)
    wal_pipeline_id = hd(wal_events).wal_pipeline_id

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

    unless Repo.in_transaction?() do
      :syn.publish(:replication, {:wal_event_inserted, wal_pipeline_id}, :wal_event_inserted)
    end

    {:ok, count}
  end

  def delete_wal_events(wal_event_ids) when is_list(wal_event_ids) do
    query = from(we in WalEvent, where: we.id in ^wal_event_ids)
    {count, _} = Repo.delete_all(query)
    {:ok, count}
  end

  def wal_events_metrics(wal_pipeline_id) do
    tasks = [
      Task.async(fn ->
        query =
          from we in WalEvent,
            where: we.wal_pipeline_id == ^wal_pipeline_id,
            select: %{
              min: min(we.committed_at),
              max: max(we.committed_at)
            }

        Repo.one(query)
      end),
      Task.async(fn ->
        fast_count_events_for_wal_pipeline(wal_pipeline_id)
      end)
    ]

    [%{min: min, max: max}, count] = Task.await_many(tasks)

    %{
      min: min,
      max: max,
      count: count
    }
  end

  @fast_count_threshold 50_000
  def fast_count_threshold, do: @fast_count_threshold

  def fast_count_events_for_wal_pipeline(wal_pipeline_id, params \\ []) do
    query = list_wal_events_query(wal_pipeline_id, params)

    # This number can be pretty inaccurate
    result = Ecto.Adapters.SQL.explain(Repo, :all, query)
    [_, rows] = Regex.run(~r/rows=(\d+)/, result)

    case String.to_integer(rows) do
      count when count > @fast_count_threshold ->
        count

      _ ->
        count_wal_events_for_wal_pipeline(wal_pipeline_id)
    end
  end

  def count_wal_events_for_wal_pipeline(wal_pipeline_id) do
    wal_pipeline_id
    |> list_wal_events_query()
    |> Repo.aggregate(:count, :id)
  end

  defp env do
    Application.get_env(:sequin, :env)
  end
end

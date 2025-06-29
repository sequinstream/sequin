defmodule Sequin.Replication do
  @moduledoc false
  import Ecto.Query

  alias Sequin.Databases
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Error
  alias Sequin.Error.NotFoundError
  alias Sequin.Health
  alias Sequin.Metrics
  alias Sequin.Postgres
  alias Sequin.Redis
  alias Sequin.Replication.PostgresReplicationSlot
  alias Sequin.Replication.WalEvent
  alias Sequin.Replication.WalPipeline
  alias Sequin.Repo
  alias Sequin.Runtime.DatabaseLifecycleEventWorker
  alias Sequin.Runtime.Supervisor, as: RuntimeSupervisor

  require Logger

  @type wal_cursor :: %{commit_lsn: integer(), commit_idx: integer()}

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

  def create_pg_replication(account_id, attrs, opts \\ []) do
    attrs = Sequin.Map.atomize_keys(attrs)
    attrs = Map.put(attrs, :status, :active)

    changeset = PostgresReplicationSlot.create_changeset(%PostgresReplicationSlot{account_id: account_id}, attrs)

    Repo.transact(fn ->
      with %{valid?: true} = changeset <- changeset,
           {:ok, postgres_database} <- get_or_build_postgres_database(account_id, attrs),
           :ok <- validate_replication_config(postgres_database, attrs, opts),
           {:ok, pg_replication} <- Repo.insert(changeset) do
        DatabaseLifecycleEventWorker.enqueue(:create, :postgres_replication_slot, pg_replication.id)
        {:ok, pg_replication}
      else
        {:error, %NotFoundError{}} ->
          {:error, Error.validation(summary: "Database with id #{attrs[:postgres_database_id]} not found")}

        %{valid?: false} = changeset ->
          {:error, changeset}

        error ->
          error
      end
    end)
  end

  def update_pg_replication(%PostgresReplicationSlot{} = pg_replication, attrs) do
    Repo.transact(fn ->
      res =
        pg_replication
        |> PostgresReplicationSlot.update_changeset(attrs)
        |> Repo.update()

      with {:ok, pg_replication} <- res do
        DatabaseLifecycleEventWorker.enqueue(:update, :postgres_replication_slot, pg_replication.id)
        {:ok, pg_replication}
      end
    end)
  end

  def delete_pg_replication(%PostgresReplicationSlot{} = pg_replication) do
    Repo.transact(fn ->
      with {:ok, deleted_pg_replication} <- Repo.delete(pg_replication) do
        DatabaseLifecycleEventWorker.enqueue(:delete, :postgres_replication_slot, pg_replication.id)
        {:ok, deleted_pg_replication}
      end
    end)
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

  def validate_replication_config(%PostgresDatabase{} = db, attrs, opts \\ []) do
    validate_slot? = Keyword.get(opts, :validate_slot?, true)
    validate_pub? = Keyword.get(opts, :validate_pub?, true)

    Databases.with_connection(db, fn conn ->
      with :ok <- if(validate_slot?, do: validate_slot(conn, attrs.slot_name), else: :ok) do
        if validate_pub?, do: validate_publication(conn, attrs.publication_name), else: :ok
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
  @spec put_restart_wal_cursor!(replication_slot_id :: String.t(), wal_cursor :: wal_cursor()) ::
          Redis.redis_value()
  def put_restart_wal_cursor!(replication_slot_id, %{commit_lsn: lsn, commit_idx: idx}) do
    Redis.command!(["SET", restart_wal_cursor_key(replication_slot_id), "#{lsn}:#{idx}"])
  end

  @spec restart_wal_cursor(replication_slot_id :: String.t()) :: {:ok, wal_cursor()} | {:error, Error.t()}
  def restart_wal_cursor(replication_slot_id) do
    case Redis.command(["GET", restart_wal_cursor_key(replication_slot_id)]) do
      {:ok, nil} ->
        {:ok, %{commit_lsn: 0, commit_idx: 0}}

      {:ok, commit_tuple} ->
        [lsn, idx] = String.split(commit_tuple, ":")
        {:ok, %{commit_lsn: String.to_integer(lsn), commit_idx: String.to_integer(idx)}}

      error ->
        error
    end
  end

  defp restart_wal_cursor_key(replication_slot_id) do
    "sequin:replication:last_processed_commit_tuple:#{replication_slot_id}"
  end

  # WAL Pipeline

  def list_wal_pipelines do
    Repo.all(WalPipeline)
  end

  def list_active_wal_pipelines do
    Repo.all(WalPipeline.where_active())
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
      RuntimeSupervisor.restart_wal_pipeline_servers(pipeline.replication_slot)
    end

    RuntimeSupervisor.refresh_message_handler_ctx(pipeline.replication_slot_id)
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

    {count, _} =
      Repo.insert_all(WalEvent, events,
        conflict_target: [:wal_pipeline_id, :commit_lsn, :commit_idx],
        on_conflict: :nothing
      )

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

  def measure_replication_lag(%PostgresReplicationSlot{} = slot, measure_fn \\ &replication_lag_bytes/1) do
    with {:ok, lag_bytes} <- measure_fn.(slot) do
      if lag_bytes > lag_bytes_alert_threshold(slot) do
        Health.put_event(slot, %Health.Event{
          slug: :replication_lag_checked,
          status: :warning,
          data: %{lag_bytes: lag_bytes}
        })
      else
        Health.put_event(slot, %Health.Event{
          slug: :replication_lag_checked,
          status: :success,
          data: %{lag_bytes: lag_bytes}
        })
      end

      Metrics.measure_postgres_replication_slot_lag(slot, lag_bytes)

      {:ok, lag_bytes}
    end
  end

  def replication_lag_bytes(%PostgresReplicationSlot{} = slot) do
    slot = Repo.preload(slot, :postgres_database)
    Postgres.replication_lag_bytes(slot.postgres_database, slot.slot_name)
  end

  def lag_bytes_alert_threshold(%PostgresReplicationSlot{account_id: "52b3c9ff-4bd3-44f8-988e-9cc4b1162dc8"}) do
    # Higher for specific account
    10 * 1024 * 1024 * 1024
  end

  # 2.5GB
  def lag_bytes_alert_threshold(_slot) do
    2.5 * 1024 * 1024 * 1024
  end

  @doc """
  Gets both restart_lsn and confirmed_flush_lsn for a replication slot.
  Returns the LSNs in their original string format.
  """
  @spec get_replication_lsns(PostgresReplicationSlot.t()) ::
          {:ok, %{restart_lsn: String.t(), confirmed_flush_lsn: String.t()}} | {:error, Error.t()}
  def get_replication_lsns(%PostgresReplicationSlot{} = slot) do
    slot = Repo.preload(slot, :postgres_database)
    Postgres.get_replication_lsns(slot.postgres_database, slot.slot_name)
  end

  defp env do
    Application.get_env(:sequin, :env)
  end
end

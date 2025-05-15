defmodule Sequin.Health.CheckPostgresReplicationSlotWorker do
  @moduledoc false

  use Oban.Worker,
    queue: :default,
    max_attempts: 1,
    unique: [period: 10, timestamp: :scheduled_at]

  import Sequin.Error.Guards, only: [is_error: 1]

  alias Sequin.Databases
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Error
  alias Sequin.Health
  alias Sequin.Health.Event
  alias Sequin.Metrics
  alias Sequin.NetworkUtils
  alias Sequin.Postgres
  alias Sequin.Prometheus
  alias Sequin.Replication
  alias Sequin.Replication.PostgresReplicationSlot
  alias Sequin.Repo
  alias Sequin.Statsd

  require Logger

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"postgres_database_id" => postgres_database_id}}) do
    with {:ok, database} <- Databases.get_db(postgres_database_id),
         database = Repo.preload(database, [:replication_slot]),
         Logger.metadata(database_id: database.id, replication_id: database.replication_slot.id),
         :ok <- check_database(database) do
      check_replication_slot(database)
      measure_replication_lag(database)
      log_replication_lsns(database)

      :syn.publish(:replication, {:postgres_replication_slot_checked, database.id}, :postgres_replication_slot_checked)

      :ok
    else
      {:error, %Error.NotFoundError{}} ->
        Logger.warning("Database not found: #{postgres_database_id}")
        :ok

      :error ->
        :ok
    end
  end

  def enqueue(postgres_database_id) do
    %{postgres_database_id: postgres_database_id}
    |> new()
    |> Oban.insert()
  end

  def enqueue(postgres_database_id, unique: false) do
    %{postgres_database_id: postgres_database_id}
    # Effectively disable unique constraint for this job
    |> new(unique: [states: [:available], period: 1])
    |> Oban.insert()
  end

  def enqueue_in(postgres_database_id, delay_seconds) do
    %{postgres_database_id: postgres_database_id}
    |> new(schedule_in: delay_seconds)
    |> Oban.insert()
  end

  @legacy_pg13_database_ids ["dbbdcca6-b62c-458b-bc12-ebfe056374b9", "e1b5e9be-9886-41a3-98e9-bca3fd5f971b"]
  defp check_database(%PostgresDatabase{} = database) do
    with :ok <- Databases.test_tcp_reachability(database),
         :ok <- Databases.test_connect(database),
         :ok <- Databases.test_permissions(database),
         :ok <- Databases.test_maybe_replica(database, database.primary),
         {:ok, latency} <- NetworkUtils.measure_latency(database.hostname, database.port) do
      cond do
        is_nil(database.pg_major_version) ->
          Logger.warning("Database #{database.id} has no pg_major_version")
          :ok

        database.pg_major_version >= 14 or database.id in @legacy_pg13_database_ids ->
          # We mark success in case a database version is upgraded from below 14 to 14 or above
          Health.put_event(database.replication_slot, %Event{slug: :db_logical_messages_table_existence, status: :success})

          Health.put_event(database.replication_slot, %Event{
            slug: :db_logical_messages_table_in_publication,
            status: :success
          })

        database.pg_major_version < 14 ->
          if Postgres.has_sequin_logical_messages_table?(database) do
            Health.put_event(database.replication_slot, %Event{
              slug: :db_logical_messages_table_existence,
              status: :success
            })

            if Postgres.sequin_logical_messages_table_in_publication?(database) do
              Health.put_event(database.replication_slot, %Event{
                slug: :db_logical_messages_table_in_publication,
                status: :success
              })
            else
              Health.put_event(database.replication_slot, %Event{
                slug: :db_logical_messages_table_in_publication,
                status: :fail
              })
            end
          else
            Health.put_event(database.replication_slot, %Event{slug: :db_logical_messages_table_existence, status: :fail})
          end
      end

      Health.put_event(database.replication_slot, %Event{slug: :db_connectivity_checked, status: :success})
      Metrics.measure_database_avg_latency(database, latency)
      :ok
    else
      {:error, error} when is_error(error) ->
        Health.put_event(database.replication_slot, %Event{slug: :db_connectivity_checked, status: :fail, error: error})
        :error

      {:error, error} when is_exception(error) ->
        error = Error.service(service: :postgres_database, message: Exception.message(error))
        Health.put_event(database.replication_slot, %Event{slug: :db_connectivity_checked, status: :fail, error: error})
        :error
    end
  rescue
    error ->
      error = Error.service(service: :postgres_database, message: Exception.message(error))
      Health.put_event(database.replication_slot, %Event{slug: :db_connectivity_checked, status: :fail, error: error})
      :error
  end

  defp check_replication_slot(database) do
    case Databases.verify_slot(database, database.replication_slot) do
      :ok ->
        Health.put_event(database.replication_slot, %Event{slug: :replication_slot_checked, status: :success})
        :ok

      {:error, error} when is_error(error) ->
        Health.put_event(database.replication_slot, %Event{slug: :replication_slot_checked, status: :fail, error: error})
        :error

      {:error, error} when is_exception(error) ->
        error = Error.service(service: :postgres_database, message: Exception.message(error))
        Health.put_event(database.replication_slot, %Event{slug: :replication_slot_checked, status: :fail, error: error})
        :error
    end
  end

  defp measure_replication_lag(%PostgresDatabase{} = db) do
    %PostgresReplicationSlot{} = slot = db.replication_slot

    case Replication.measure_replication_lag(slot) do
      {:ok, lag_bytes} ->
        lag_mb = Float.round(lag_bytes / 1024 / 1024, 0)

        Prometheus.set_replication_slot_size(slot.id, slot.slot_name, db.name, lag_bytes)

        Statsd.gauge("sequin.db.replication_lag_mb", lag_mb,
          tags: %{replication_slot_id: slot.id, account_id: slot.account_id}
        )

        if lag_bytes > Replication.lag_bytes_alert_threshold(slot) do
          Logger.warning("Replication lag is #{lag_mb}MB", lag_bytes: lag_bytes)
        else
          Logger.info("Replication lag is #{lag_mb}MB", lag_bytes: lag_bytes)
        end

      {:error, error} ->
        Logger.error("Failed to measure replication lag: #{inspect(error)}")
    end
  end

  defp log_replication_lsns(%PostgresDatabase{} = db) do
    %PostgresReplicationSlot{} = slot = db.replication_slot

    case Replication.get_replication_lsns(slot) do
      {:ok, %{restart_lsn: restart_lsn, confirmed_flush_lsn: confirmed_flush_lsn}}
      when not is_nil(restart_lsn) and not is_nil(confirmed_flush_lsn) ->
        # Calculate lag in bytes
        lag_bytes = confirmed_flush_lsn - restart_lsn
        pretty_lag = Sequin.String.format_bytes(lag_bytes)

        Logger.info("Replication slot LSNs",
          slot_name: slot.slot_name,
          restart_lsn: restart_lsn,
          confirmed_flush_lsn: confirmed_flush_lsn,
          lag_bytes: lag_bytes,
          lag_pretty: pretty_lag
        )

      {:ok, %{restart_lsn: nil}} ->
        Logger.warning("Replication slot #{slot.slot_name} has nil restart_lsn")

      {:ok, %{confirmed_flush_lsn: nil}} ->
        Logger.warning("Replication slot #{slot.slot_name} has nil confirmed_flush_lsn")

      {:error, error} ->
        Logger.error("Failed to fetch replication LSNs: #{inspect(error)}")
    end
  end
end

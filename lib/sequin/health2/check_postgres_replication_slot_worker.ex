defmodule Sequin.Health2.CheckPostgresReplicationSlotWorker do
  @moduledoc false

  use Oban.Worker,
    queue: :default,
    max_attempts: 1

  import Sequin.Error.Guards, only: [is_error: 1]

  alias Sequin.Databases
  alias Sequin.Error
  alias Sequin.Health2
  alias Sequin.Health2.Event
  alias Sequin.Metrics
  alias Sequin.Repo

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"postgres_database_id" => postgres_database_id}}) do
    with {:ok, database} <- Databases.get_db(postgres_database_id),
         database = Repo.preload(database, :replication_slot),
         :ok <- check_database(database) do
      check_replication_slot(database)
    else
      {:error, %Error.NotFoundError{}} ->
        :ok

      error ->
        error
    end
  end

  def enqueue(postgres_database_id) do
    %{postgres_database_id: postgres_database_id}
    |> new()
    |> Oban.insert()
  end

  def enqueue_in(postgres_database_id, delay_seconds) do
    %{postgres_database_id: postgres_database_id}
    |> new(schedule_in: delay_seconds)
    |> Oban.insert()
  end

  defp check_database(database) do
    with :ok <- Databases.test_tcp_reachability(database),
         before_connect = System.monotonic_time(:millisecond),
         :ok <- Databases.test_connect(database),
         :ok <- Databases.test_permissions(database) do
      after_connect = System.monotonic_time(:millisecond)
      Health2.put_event(database.replication_slot, %Event{slug: :db_connectivity_checked, status: :success})
      Metrics.incr_database_avg_latency(database, after_connect - before_connect)
      :ok
    else
      {:error, error} when is_error(error) ->
        Health2.put_event(database, %Event{slug: :db_connectivity_checked, status: :fail, error: error})
        :error

      {:error, error} when is_exception(error) ->
        error = Error.service(service: :postgres_database, message: Exception.message(error))
        Health2.put_event(database, %Event{slug: :db_connectivity_checked, status: :fail, error: error})
        :error
    end
  rescue
    error ->
      error = Error.service(service: :postgres_database, message: Exception.message(error))
      Health2.put_event(database, %Event{slug: :db_connectivity_checked, status: :fail, error: error})
      :error
  end

  defp check_replication_slot(database) do
    case Databases.test_slot_permissions(database, database.replication_slot) do
      :ok ->
        Health2.put_event(database.replication_slot, %Event{slug: :replication_slot_checked, status: :success})
        :ok

      {:error, error} when is_error(error) ->
        Health2.put_event(database.replication_slot, %Event{slug: :replication_slot_checked, status: :fail, error: error})
        :error

      {:error, error} when is_exception(error) ->
        error = Error.service(service: :postgres_database, message: Exception.message(error))
        Health2.put_event(database.replication_slot, %Event{slug: :replication_slot_checked, status: :fail, error: error})
        :error
    end
  end
end

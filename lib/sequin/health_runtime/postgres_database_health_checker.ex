defmodule Sequin.HealthRuntime.PostgresDatabaseHealthChecker do
  @moduledoc false

  use Oban.Worker,
    queue: :health_checks,
    max_attempts: 1,
    unique: [states: [:available, :scheduled, :retryable]]

  alias Sequin.Databases
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Error
  alias Sequin.Health
  alias Sequin.Metrics
  alias Sequin.Repo

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"postgres_database_id" => postgres_database_id}}) do
    database = Repo.get!(PostgresDatabase, postgres_database_id)
    check_database(database)
    enqueue_next_run(postgres_database_id)
  end

  defp enqueue_next_run(postgres_database_id) do
    %{postgres_database_id: postgres_database_id}
    |> new(schedule_in: 5)
    |> Oban.insert()
  end

  defp check_database(database) do
    with :ok <- Databases.test_tcp_reachability(database),
         before_connect = System.monotonic_time(:millisecond),
         :ok <- Databases.test_connect(database) do
      after_connect = System.monotonic_time(:millisecond)
      Health.update(database, :reachable, :healthy)
      Metrics.incr_database_avg_latency(database, after_connect - before_connect)
    else
      {:error, error} when is_exception(error) ->
        error = Error.service(service: :postgres_database, message: Exception.message(error))
        Health.update(database, :reachable, :error, error)
    end
  rescue
    error ->
      error = Error.service(service: :postgres_database, message: Exception.message(error))
      Health.update(database, :reachable, :error, error)
  end
end

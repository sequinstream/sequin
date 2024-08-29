defmodule Sequin.Health.PostgresDatabaseHealthChecker do
  @moduledoc false
  use GenServer

  alias Sequin.Databases
  alias Sequin.Error
  alias Sequin.Health
  alias Sequin.Metrics

  require Logger

  @check_interval :timer.seconds(5)

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    schedule_check()
    {:ok, %{}}
  end

  @impl true
  def handle_info(:check_reachability, state) do
    perform_check()
    schedule_check()
    {:noreply, state}
  end

  defp perform_check do
    Enum.each(Databases.list_dbs(), &check_database/1)
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

  defp schedule_check do
    Process.send_after(self(), :check_reachability, @check_interval)
  end
end

defmodule Sequin.Health.CheckHttpEndpointHealthWorker do
  @moduledoc false

  use Oban.Worker,
    queue: :health_checks,
    max_attempts: 1,
    unique: [states: [:available, :scheduled, :retryable]]

  alias Sequin.Consumers
  alias Sequin.Error
  alias Sequin.Health
  alias Sequin.Health.Event
  alias Sequin.Metrics

  require Logger

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"http_endpoint_id" => http_endpoint_id}}) do
    case Consumers.get_http_endpoint(http_endpoint_id) do
      {:ok, endpoint} ->
        check_endpoint(endpoint)

      {:error, %Error.NotFoundError{}} ->
        :ok

      error ->
        error
    end
  end

  def enqueue(http_endpoint_id) do
    %{http_endpoint_id: http_endpoint_id}
    |> new()
    |> Oban.insert()
  end

  def enqueue(http_endpoint_id, unique: false) do
    %{http_endpoint_id: http_endpoint_id}
    |> new(unique: [states: [:available], period: 1])
    |> Oban.insert()
  end

  def enqueue_in(http_endpoint_id, delay_seconds) do
    %{http_endpoint_id: http_endpoint_id}
    |> new(schedule_in: delay_seconds)
    |> Oban.insert()
  end

  defp check_endpoint(endpoint) do
    with {:ok, :reachable} <- Consumers.test_reachability(endpoint),
         before_connect_time = System.monotonic_time(:millisecond),
         {:ok, :connected} <- Consumers.test_connect(endpoint) do
      after_connect_time = System.monotonic_time(:millisecond)
      Health.put_event(endpoint, %Event{slug: :endpoint_reachable, status: :success})
      Metrics.measure_http_endpoint_avg_latency(endpoint, after_connect_time - before_connect_time)
    else
      {:error, :unreachable} ->
        error = Error.service(service: :http_endpoint, message: "Endpoint unreachable")
        Health.put_event(endpoint, %Event{slug: :endpoint_reachable, status: :fail, error: error})

      {:error, :invalid_url} ->
        error = Error.service(service: :http_endpoint, message: "Invalid URL")
        Health.put_event(endpoint, %Event{slug: :endpoint_reachable, status: :fail, error: error})

      {:error, reason} when is_atom(reason) ->
        error = Error.service(service: :http_endpoint, message: Atom.to_string(reason))
        Health.put_event(endpoint, %Event{slug: :endpoint_reachable, status: :fail, error: error})
    end
  rescue
    error ->
      error = Error.service(service: :http_endpoint, message: Exception.message(error))
      Health.put_event(endpoint, %Event{slug: :endpoint_reachable, status: :fail, error: error})
  end
end

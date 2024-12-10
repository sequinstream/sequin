defmodule Sequin.HealthRuntime.HttpEndpointHealthWorker do
  @moduledoc false

  use Oban.Worker,
    queue: :health_checks,
    max_attempts: 1,
    unique: [states: [:available, :scheduled, :retryable]]

  alias Sequin.Consumers
  alias Sequin.Error
  alias Sequin.Health
  alias Sequin.Metrics

  require Logger

  @impl Oban.Worker
  def perform(_job) do
    Enum.each(Consumers.list_http_endpoints(), &check_endpoint/1)
  end

  defp check_endpoint(endpoint) do
    with {:ok, :reachable} <- Consumers.test_reachability(endpoint),
         before_connect_time = System.monotonic_time(:millisecond),
         {:ok, :connected} <- Consumers.test_connect(endpoint) do
      after_connect_time = System.monotonic_time(:millisecond)
      Health.update(endpoint, :reachable, :healthy)
      Metrics.incr_http_endpoint_avg_latency(endpoint, after_connect_time - before_connect_time)
    else
      {:error, :unreachable} ->
        error = Error.service(service: :http_endpoint, message: "Endpoint unreachable")
        Health.update(endpoint, :reachable, :error, error)

      {:error, :invalid_url} ->
        error = Error.service(service: :http_endpoint, message: "Invalid URL")
        Health.update(endpoint, :reachable, :error, error)

      {:error, reason} when is_atom(reason) ->
        error = Error.service(service: :http_endpoint, message: Atom.to_string(reason))
        Health.update(endpoint, :reachable, :error, error)
    end
  rescue
    error ->
      error = Error.service(service: :http_endpoint, message: Exception.message(error))
      Health.update(endpoint, :reachable, :error, error)
  end
end

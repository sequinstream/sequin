defmodule Sequin.Health.CheckHttpEndpointHealthWorker do
  @moduledoc false

  use Oban.Worker,
    queue: :health_checks,
    max_attempts: 1,
    unique: [
      states: ~w(available scheduled retryable)a,
      period: :infinity,
      timestamp: :scheduled_at
    ]

  alias Sequin.Consumers
  alias Sequin.Error
  alias Sequin.Health
  alias Sequin.Health.Event

  require Logger

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"http_endpoint_id" => http_endpoint_id}}) do
    case Consumers.get_http_endpoint(http_endpoint_id) do
      {:ok, endpoint} -> check_endpoint(endpoint)
      {:error, %Error.NotFoundError{}} -> :ok
    end
  end

  def enqueue(http_endpoint_id) do
    %{http_endpoint_id: http_endpoint_id}
    |> new()
    |> Oban.insert()
  end

  def enqueue_for_user(http_endpoint_id) do
    %{http_endpoint_id: http_endpoint_id}
    |> new(queue: :user_submitted)
    |> Oban.insert()
  end

  defp check_endpoint(endpoint) do
    case Consumers.test_reachability(endpoint) do
      {:ok, :reachable} ->
        Health.put_event(endpoint, %Event{slug: :endpoint_reachable, status: :success})

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

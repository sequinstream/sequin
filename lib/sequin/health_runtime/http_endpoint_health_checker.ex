defmodule Sequin.Health.HttpEndpointHealthChecker do
  @moduledoc false
  use GenServer

  alias Sequin.Consumers
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

  defp schedule_check do
    Process.send_after(self(), :check_reachability, @check_interval)
  end
end

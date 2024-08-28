defmodule Sequin.Health.HttpEndpointHealthChecker do
  @moduledoc false
  use GenServer

  alias Sequin.Consumers
  alias Sequin.Error
  alias Sequin.Health

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
    case Consumers.test_reachability(endpoint) do
      {:ok, :reachable} ->
        Health.update(endpoint, :reachable, :healthy)

      {:error, reason} ->
        error = Error.service(service: :http_endpoint, message: "Endpoint unreachable", details: %{reason: reason})
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

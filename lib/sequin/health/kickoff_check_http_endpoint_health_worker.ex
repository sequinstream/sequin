defmodule Sequin.Health.KickoffCheckHttpEndpointHealthWorker do
  @moduledoc false

  use Oban.Worker,
    queue: :kickoff,
    max_attempts: 1,
    unique: [
      period: :infinity,
      states: ~w(scheduled available)a
    ]

  alias Sequin.Consumers
  alias Sequin.Health.CheckHttpEndpointHealthWorker

  @impl Oban.Worker
  def perform(_) do
    Consumers.list_http_endpoints()
    |> Enum.with_index()
    |> Enum.each(fn {endpoint, index} ->
      # Integer division to get the delay in seconds (10 per second)
      delay_seconds = div(index, 10)
      CheckHttpEndpointHealthWorker.enqueue_in(endpoint.id, delay_seconds)
    end)

    :ok
  end

  def enqueue do
    %{}
    |> new()
    |> Oban.insert()
  end
end

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
    Enum.each(Consumers.list_http_endpoints(), fn endpoint ->
      CheckHttpEndpointHealthWorker.enqueue(endpoint.id)
    end)

    :ok
  end

  def enqueue do
    %{}
    |> new()
    |> Oban.insert()
  end
end

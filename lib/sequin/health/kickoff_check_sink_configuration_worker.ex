defmodule Sequin.Health.KickoffCheckSinkConfigurationWorker do
  @moduledoc false

  use Oban.Worker,
    queue: :kickoff,
    max_attempts: 1,
    unique: [
      period: :infinity,
      states: ~w(scheduled available)a
    ]

  alias Sequin.Consumers
  alias Sequin.Health.CheckSinkConfigurationWorker

  @impl Oban.Worker
  def perform(_) do
    Consumers.list_active_sink_consumers()
    |> Enum.with_index()
    |> Enum.map(fn {consumer, index} ->
      # Integer division to get the delay in seconds (10 per second)
      delay_seconds = div(index, 10)

      CheckSinkConfigurationWorker.enqueue_in(consumer.id, delay_seconds)
    end)

    :ok
  end

  def enqueue do
    %{}
    |> new()
    |> Oban.insert()
  end
end

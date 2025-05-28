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
    Enum.each(Consumers.list_active_sink_consumers(), fn consumer ->
      CheckSinkConfigurationWorker.enqueue(consumer.id)
    end)

    :ok
  end

  def enqueue do
    %{}
    |> new()
    |> Oban.insert()
  end
end

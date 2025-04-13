defmodule Sequin.Metrics.PrometheusSupervisor do
  @moduledoc false
  use Supervisor

  def start_link(arg) do
    Supervisor.start_link(__MODULE__, arg, name: __MODULE__)
  end

  @impl true
  def init(_arg) do
    children = [
      {Peep, [name: :sequin, metrics: Sequin.Metrics.Prometheus.metrics()]},
      SequinWeb.MetricsEndpoint
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule Sequin.Streams.Supervisors.Supervisor do
  @moduledoc """
  """
  use Supervisor

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @impl Supervisor
  def init(_) do
    Supervisor.init(children(), strategy: :one_for_one)
  end

  defp children do
    [
      Sequin.Streams.AssignMessageSeqServer,
      Sequin.Streams.Supervisors.ConsumerSupervisor,
      Sequin.Streams.Supervisors.ConsumerSupervisorStarter
    ]
  end
end

defmodule Sequin.Health.KickoffCheckPostgresReplicationSlotWorker do
  @moduledoc false

  use Oban.Worker,
    queue: :kickoff,
    max_attempts: 1,
    unique: [
      period: :infinity,
      states: ~w(scheduled available)a
    ]

  alias Sequin.Databases
  alias Sequin.Health.CheckPostgresReplicationSlotWorker

  @impl Oban.Worker
  def perform(_) do
    Enum.each(Databases.list_dbs(), fn db ->
      CheckPostgresReplicationSlotWorker.enqueue(db.id)
    end)

    :ok
  end

  def enqueue do
    %{}
    |> new()
    |> Oban.insert()
  end
end

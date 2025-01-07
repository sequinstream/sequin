defmodule Sequin.Health.KickoffCheckPostgresReplicationSlotWorker do
  @moduledoc false

  use Oban.Worker,
    queue: :default,
    max_attempts: 1

  alias Sequin.Databases
  alias Sequin.Health.CheckPostgresReplicationSlotWorker

  @impl Oban.Worker
  def perform(_) do
    Databases.list_dbs()
    |> Enum.with_index()
    |> Enum.map(fn {db, index} ->
      # Integer division to get the delay in seconds (10 per second)
      delay_seconds = div(index, 10)
      CheckPostgresReplicationSlotWorker.enqueue_in(db.id, delay_seconds)
    end)

    :ok
  end

  def enqueue do
    %{}
    |> new()
    |> Oban.insert()
  end
end

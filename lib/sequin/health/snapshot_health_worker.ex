defmodule Sequin.Health.SnapshotHealthWorker do
  @moduledoc """
  Worker that periodically snapshots health status to Postgres and creates alerts on status changes.
  """

  use Oban.Worker,
    queue: :health_checks,
    max_attempts: 1

  alias Sequin.Health

  require Logger

  def enqueue do
    Oban.insert(new(%{}))
  end

  @impl Oban.Worker
  def perform(_job) do
    Logger.info("[Sequin.Health.SnapshotHealthWorker] Updating health snapshots")

    Sequin.Statsd.measure("sequin.health.update_snapshots", fn ->
      Health.update_snapshots()
    end)

    Logger.info("[Sequin.Health.SnapshotHealthWorker] Health snapshots updated")
  end
end

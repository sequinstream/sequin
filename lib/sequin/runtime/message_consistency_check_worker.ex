defmodule Sequin.Runtime.MessageConsistencyCheckWorker do
  @moduledoc false
  use Oban.Worker,
    queue: :default,
    max_attempts: 1

  alias Sequin.Consumers
  alias Sequin.Replication
  alias Sequin.Runtime.MessageLedgers

  require Logger

  @impl Oban.Worker
  def perform(_job) do
    Logger.info("[MessageConsistencyCheckWorker] Starting consistency check")

    two_minutes_ago = DateTime.add(DateTime.utc_now(), -2 * 60, :second)

    active_replications = Replication.all_active_pg_replications()
    active_replication_ids = Enum.map(active_replications, & &1.id)

    Consumers.list_active_sink_consumers()
    |> Enum.filter(&(&1.replication_slot_id in active_replication_ids))
    |> Enum.each(&MessageLedgers.audit_and_trim_undelivered_cursors(&1, two_minutes_ago))

    Logger.info("[MessageConsistencyCheckWorker] Completed consistency check")
  end

  def enqueue do
    Oban.insert(new(%{}))
  end
end

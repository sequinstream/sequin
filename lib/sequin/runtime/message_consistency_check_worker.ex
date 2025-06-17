defmodule Sequin.Runtime.MessageConsistencyCheckWorker do
  @moduledoc false
  use Oban.Worker,
    queue: :default,
    max_attempts: 1

  alias Sequin.Consumers
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Runtime.MessageLedgers

  require Logger

  @impl Oban.Worker
  def perform(_job) do
    Logger.info("[MessageConsistencyCheckWorker] Starting consistency check")

    Enum.each(Consumers.list_active_sink_consumers(), fn %SinkConsumer{id: consumer_id} ->
      two_minutes_ago = DateTime.add(DateTime.utc_now(), -2 * 60, :second)
      audit_and_trim_undelivered_cursors(consumer_id, two_minutes_ago)
    end)

    Logger.info("[MessageConsistencyCheckWorker] Completed consistency check")
  end

  def audit_and_trim_undelivered_cursors(consumer_id, older_than_timestamp) do
    case MessageLedgers.count_undelivered_wal_cursors(consumer_id, older_than_timestamp) do
      {:ok, 0} ->
        :ok

      {:ok, undelivered_cursor_count} ->
        Logger.warning("[MessageConsistencyCheckWorker] Found undelivered cursors (count=#{undelivered_cursor_count})",
          consumer_id: consumer_id,
          undelivered_cursor_count: undelivered_cursor_count
        )

        MessageLedgers.trim_stale_undelivered_wal_cursors(consumer_id, older_than_timestamp)
    end
  end

  def enqueue do
    Oban.insert(new(%{}))
  end
end

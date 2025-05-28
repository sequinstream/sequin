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
    case MessageLedgers.list_undelivered_wal_cursors(consumer_id, older_than_timestamp) do
      {:ok, []} ->
        :ok

      {:ok, undelivered_cursors} ->
        Logger.info("[MessageConsistencyCheckWorker] Found undelivered cursors (count=#{length(undelivered_cursors)})",
          consumer_id: consumer_id,
          undelivered_cursors: inspect(undelivered_cursors)
        )

        consumer = Consumers.get_sink_consumer!(consumer_id)

        persisted_messages =
          undelivered_cursors
          # In case there are enough undelivered cursors to violate the 65535 limit on postgres params
          |> Enum.chunk_every(10_000)
          |> Enum.flat_map(fn chunk ->
            Consumers.list_consumer_messages_for_consumer(consumer, wal_cursor_in: chunk)
          end)

        persisted_wal_cursor_set = MapSet.new(persisted_messages, &{&1.commit_lsn, &1.commit_idx})

        undelivered_and_unpersisted_cursors =
          Enum.reject(undelivered_cursors, fn %{commit_lsn: commit_lsn, commit_idx: commit_idx} ->
            MapSet.member?(persisted_wal_cursor_set, {commit_lsn, commit_idx})
          end)

        unless undelivered_and_unpersisted_cursors == [] do
          Logger.warning(
            "[MessageConsistencyCheckWorker] Found undelivered/unpersisted cursors (count=#{length(undelivered_and_unpersisted_cursors)})",
            consumer_id: consumer_id,
            count: length(undelivered_and_unpersisted_cursors),
            undelivered_cursors: inspect(undelivered_and_unpersisted_cursors)
          )
        end

        MessageLedgers.trim_stale_undelivered_wal_cursors(consumer_id, older_than_timestamp)
    end
  end

  def enqueue do
    Oban.insert(new(%{}))
  end
end

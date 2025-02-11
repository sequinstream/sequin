defmodule Sequin.Replication.ReplicationSlotAdvanceWorker do
  @moduledoc """
  GenServer that periodically advances replication slots' WAL positions based on the minimum cursor
  position across all active sinks associated with each slot.
  """
  use GenServer

  alias Sequin.Replication
  alias Sequin.Repo

  require Logger

  @interval :timer.seconds(10)

  # Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  # Server Callbacks

  @impl GenServer
  def init(_opts) do
    schedule_next_run()
    {:ok, %{}}
  end

  @impl GenServer
  def handle_info(:advance_slots, state) do
    Logger.info("Starting replication slot advance cycle")

    # Get all active replication slots
    active_slots = Replication.all_active_pg_replications()

    # Process each slot
    Enum.each(active_slots, &advance_low_watermark/1)

    schedule_next_run()
    {:noreply, state}
  end

  # Private functions

  defp schedule_next_run do
    Process.send_after(self(), :advance_slots, @interval)
  end

  @doc """
  Advances the low watermark for the replicaiton slot to postgres so the SlotProcessor
  can advance the confirmed_flush_lsn to Postgres.

  We check the high_watermarks for every active sink and the high watermark for the replication slot.

  We set the low watermark to the lowest of all high watermarks.
  """
  def advance_low_watermark(replication_slot) do
    replication_slot =
      Repo.preload(
        replication_slot,
        [
          :high_watermark_wal_cursor,
          :low_watermark_wal_cursor,
          not_disabled_sink_consumers: [:high_watermark_wal_cursor]
        ],
        force: true
      )

    slot_high_watermark = replication_slot.high_watermark_wal_cursor
    sink_high_watermarks = Enum.map(replication_slot.not_disabled_sink_consumers, & &1.high_watermark_wal_cursor)

    lowest_high_watermark = Enum.min_by([slot_high_watermark | sink_high_watermarks], &{&1.commit_lsn, &1.commit_idx})

    Logger.info("Advancing low watermark for replication slot #{replication_slot.id} to #{lowest_high_watermark}")

    # Update the low watermark for the replication slot
    case Replication.update_watermark(replication_slot.low_watermark_wal_cursor, %{
           commit_lsn: lowest_high_watermark.commit_lsn,
           commit_idx: lowest_high_watermark.commit_idx
         }) do
      {:ok, _} ->
        :ok

      {:error, error} ->
        Logger.error("Error advancing low watermark for replication slot #{replication_slot.id}", error: error)
        :error
    end
  rescue
    error ->
      Logger.error("Error advancing low watermark for replication slot #{replication_slot.id}", error: error)
      :error
  end
end

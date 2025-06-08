defmodule Sequin.Runtime.InitBackfillStatsWorker do
  @moduledoc """
  Worker that calculates and sets the initial row count for a backfill.
  """
  use Oban.Worker,
    queue: :lifecycle,
    max_attempts: 3,
    unique: [period: 30]

  alias Sequin.Consumers
  alias Sequin.Repo
  alias Sequin.Runtime.TableReader

  require Logger

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"backfill_id" => backfill_id}}) do
    with {:ok, backfill} <- Consumers.get_backfill(backfill_id),
         true <- is_nil(backfill.rows_initial_count) do
      backfill = Repo.preload(backfill, sink_consumer: [replication_slot: :postgres_database])
      database = backfill.sink_consumer.replication_slot.postgres_database

      # Find table and set its sort_column_attnum from the sequence
      table =
        database.tables
        |> Sequin.Enum.find!(&(&1.oid == backfill.table_oid))
        |> Map.put(:sort_column_attnum, backfill.sort_column_attnum)

      case TableReader.fast_count_estimate(database, table, backfill.initial_min_cursor, timeout: :infinity) do
        {:ok, count} ->
          Consumers.update_backfill(backfill, %{rows_initial_count: count}, skip_lifecycle: true)

        {:error, error} ->
          Logger.error("[InitBackfillStatsWorker] Failed to get initial count: #{inspect(error)}")
          {:error, error}
      end
    else
      {:error, error} -> {:error, error}
      # rows_initial_count already set
      false -> :ok
    end
  end

  def enqueue(backfill_id) do
    %{backfill_id: backfill_id}
    |> new()
    |> Oban.insert()
  end
end

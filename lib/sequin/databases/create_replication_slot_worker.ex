defmodule Sequin.Workers.CreateReplicationSlotWorker do
  @moduledoc false
  use Oban.Worker, queue: :default, max_attempts: 1

  alias Sequin.Databases
  alias Sequin.DatabasesRuntime.Supervisor, as: DatabasesRuntimeSupervisor
  alias Sequin.Postgres
  alias Sequin.Replication.PostgresReplicationSlot
  alias Sequin.Repo

  # 15 minutes in seconds
  @unique_period 15 * 60

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"replication_slot_id" => replication_slot_id}}) do
    with {:ok, replication_slot} <- get_replication_slot(replication_slot_id),
         {:ok, database} <- Databases.get_db(replication_slot.postgres_database_id),
         :ok <- create_replication_slot(database, replication_slot) do
      DatabasesRuntimeSupervisor.restart_replication(replication_slot)
    end
  end

  def enqueue(replication_slot_id) do
    %{replication_slot_id: replication_slot_id}
    |> new(unique: [period: @unique_period, keys: [:replication_slot_id]])
    |> Oban.insert()
  end

  defp get_replication_slot(id) do
    case Repo.get(PostgresReplicationSlot, id) do
      nil -> {:error, :replication_slot_not_found}
      slot -> {:ok, slot}
    end
  end

  defp create_replication_slot(database, replication_slot) do
    Databases.with_connection(database, fn conn ->
      Postgres.create_replication_slot(conn, replication_slot.slot_name)
    end)
  end
end

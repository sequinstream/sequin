defmodule Sequin.Databases.DatabaseUpdateWorker do
  @moduledoc false
  use Oban.Worker, queue: :default, max_attempts: 1

  alias Sequin.Databases

  require Logger

  # 5 minutes in seconds
  @default_unique_period 5 * 60

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"postgres_database_id" => postgres_database_id}}) do
    Logger.metadata(database_id: postgres_database_id)

    with {:ok, database} <- Databases.get_db(postgres_database_id),
         {:ok, updated_database} <- Databases.update_tables(database) do
      :syn.publish(
        :account,
        {:database_tables_updated, database.account_id},
        {:database_tables_updated, updated_database}
      )

      {:ok, updated_database}
    else
      {:error, %DBConnection.ConnectionError{}} ->
        Logger.error(
          "[DatabaseUpdateWorker] Database update worker failed to connect to database #{postgres_database_id}"
        )

        :ok

      error ->
        error
    end
  end

  def enqueue(postgres_database_id, opts \\ []) do
    new_opts =
      case Keyword.get(opts, :unique_period, @default_unique_period) do
        0 ->
          []

        unique_period ->
          [unique: [period: unique_period, keys: [:postgres_database_id]]]
      end

    %{postgres_database_id: postgres_database_id}
    |> new(new_opts)
    |> Oban.insert()
  end
end

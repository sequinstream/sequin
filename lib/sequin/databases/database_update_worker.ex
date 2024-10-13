defmodule Sequin.Databases.DatabaseUpdateWorker do
  @moduledoc false
  use Oban.Worker, queue: :default, max_attempts: 1

  alias Sequin.Databases

  # 5 minutes in seconds
  @default_unique_period 5 * 60

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"postgres_database_id" => postgres_database_id}}) do
    with {:ok, database} <- Databases.get_db(postgres_database_id) do
      Databases.update_tables(database)
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

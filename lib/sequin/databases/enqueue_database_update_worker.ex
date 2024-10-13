defmodule Sequin.Databases.EnqueueDatabaseUpdateWorker do
  @moduledoc false
  use Oban.Worker

  alias Sequin.Databases
  alias Sequin.Databases.DatabaseUpdateWorker

  require Logger

  @impl Oban.Worker
  def perform(_job) do
    Enum.each(Databases.list_dbs(), fn db -> DatabaseUpdateWorker.enqueue(db.id) end)
    Logger.info("[EnqueueDatabaseUpdateWorker] Enqueue complete")
  end
end

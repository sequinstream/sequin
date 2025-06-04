defmodule SequinWeb.BackfillController do
  use SequinWeb, :controller

  alias Sequin.Consumers
  alias Sequin.Repo
  alias Sequin.Runtime.KeysetCursor
  alias Sequin.Transforms
  alias SequinWeb.ApiFallbackPlug

  action_fallback ApiFallbackPlug

  def index(conn, %{"sink_id_or_name" => sink_id_or_name}) do
    account_id = conn.assigns.account_id

    with {:ok, sink_consumer} <- Consumers.find_sink_consumer(account_id, id_or_name: sink_id_or_name) do
      backfills = Consumers.list_backfills_for_sink_consumer(sink_consumer.id)
      render(conn, "index.json", backfills: backfills)
    end
  end

  def show(conn, %{"sink_id_or_name" => sink_id_or_name, "id" => id}) do
    account_id = conn.assigns.account_id

    with {:ok, sink_consumer} <- Consumers.find_sink_consumer(account_id, id_or_name: sink_id_or_name),
         {:ok, backfill} <- Consumers.get_backfill_for_sink_consumer(sink_consumer.id, id) do
      render(conn, "show.json", backfill: backfill)
    end
  end

  def create(conn, %{"sink_id_or_name" => sink_id_or_name} = params) do
    params = Map.delete(params, "sink_id_or_name")
    account_id = conn.assigns.account_id

    with {:ok, sink_consumer} <- Consumers.find_sink_consumer(account_id, id_or_name: sink_id_or_name),
         sink_consumer = Repo.preload(sink_consumer, [:postgres_database, :sequence]),
         {:ok, backfill_params} <- Transforms.from_external_backfill(params),
         table = Sequin.Enum.find!(sink_consumer.postgres_database.tables, &(&1.oid == sink_consumer.sequence.table_oid)),
         backfill_params =
           Map.merge(backfill_params, %{
             account_id: account_id,
             sink_consumer_id: sink_consumer.id,
             initial_min_cursor: KeysetCursor.min_cursor(table),
             table_oid: table.oid
           }),
         {:ok, backfill} <- Consumers.create_backfill(backfill_params) do
      render(conn, "show.json", backfill: backfill)
    end
  end

  def update(conn, %{"sink_id_or_name" => sink_id_or_name, "id" => id} = params) do
    params = Map.drop(params, ["sink_id_or_name", "id"])
    account_id = conn.assigns.account_id

    with {:ok, sink_consumer} <- Consumers.find_sink_consumer(account_id, id_or_name: sink_id_or_name),
         {:ok, backfill} <- Consumers.get_backfill(id),
         true <- backfill.sink_consumer_id == sink_consumer.id,
         {:ok, backfill_params} <- Transforms.from_external_backfill(params),
         {:ok, updated_backfill} <- Consumers.update_backfill(backfill, backfill_params) do
      render(conn, "show.json", backfill: updated_backfill)
    end
  end
end

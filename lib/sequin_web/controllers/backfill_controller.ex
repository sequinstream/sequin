defmodule SequinWeb.BackfillController do
  use SequinWeb, :controller

  alias Sequin.Consumers
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Error
  alias SequinWeb.ApiFallbackPlug

  action_fallback ApiFallbackPlug

  def index(conn, %{"consumer_id_or_name" => consumer_id_or_name}) do
    account_id = conn.assigns.account_id

    with {:ok, consumer} <- Consumers.find_sink_consumer(account_id, id_or_name: consumer_id_or_name) do
      backfills = consumer.id |> Consumers.find_backfill(order_by: [desc: :inserted_at]) |> List.wrap()
      render(conn, :index, backfills: backfills)
    end
  end

  def show(conn, %{"consumer_id_or_name" => consumer_id_or_name, "id" => id}) do
    account_id = conn.assigns.account_id

    with {:ok, consumer} <- Consumers.find_sink_consumer(account_id, id_or_name: consumer_id_or_name),
         {:ok, backfill} <- get_backfill_for_consumer(consumer.id, id) do
      render(conn, :show, backfill: backfill)
    end
  end

  def create(conn, %{"consumer_id_or_name" => consumer_id_or_name} = params) do
    account_id = conn.assigns.account_id

    with {:ok, consumer} <- Consumers.find_sink_consumer(account_id, id_or_name: consumer_id_or_name),
         {:ok, params} <- parse_create_params(params),
         # Set the consumer ID and account ID in params
         params = Map.merge(params, %{sink_consumer_id: consumer.id, account_id: account_id}),
         {:ok, backfill} <- Consumers.create_backfill(params) do
      render(conn, :show, backfill: backfill)
    end
  end

  def update(conn, %{"consumer_id_or_name" => consumer_id_or_name, "id" => id} = params) do
    account_id = conn.assigns.account_id

    with {:ok, consumer} <- Consumers.find_sink_consumer(account_id, id_or_name: consumer_id_or_name),
         {:ok, backfill} <- get_backfill_for_consumer(consumer.id, id),
         {:ok, params} <- parse_update_params(params),
         {:ok, updated_backfill} <- Consumers.update_backfill(backfill, params) do
      render(conn, :show, backfill: updated_backfill)
    end
  end

  def delete(conn, %{"consumer_id_or_name" => consumer_id_or_name, "id" => id}) do
    account_id = conn.assigns.account_id

    with {:ok, consumer} <- Consumers.find_sink_consumer(account_id, id_or_name: consumer_id_or_name),
         {:ok, backfill} <- get_backfill_for_consumer(consumer.id, id),
         {:ok, updated_backfill} <- Consumers.update_backfill(backfill, %{state: :cancelled}) do
      render(conn, :delete, backfill: updated_backfill)
    end
  end

  defp get_backfill_for_consumer(consumer_id, id) do
    case Consumers.get_backfill(id) do
      {:ok, backfill} ->
        if backfill.sink_consumer_id == consumer_id do
          {:ok, backfill}
        else
          {:error, Error.not_found(entity: :backfill)}
        end

      error ->
        error
    end
  end

  defp parse_create_params(params) do
    params = Map.take(params, ["initial_min_cursor", "sort_column_attnum"])
    {:ok, params}
  end

  defp parse_update_params(params) do
    params =
      Map.take(params, [
        "state",
        "rows_initial_count",
        "rows_processed_count",
        "rows_ingested_count"
      ])

    {:ok, params}
  end
end

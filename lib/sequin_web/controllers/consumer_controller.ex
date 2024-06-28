defmodule SequinWeb.ConsumerController do
  use SequinWeb, :controller

  alias Sequin.Streams
  alias SequinWeb.ApiFallbackPlug

  action_fallback ApiFallbackPlug

  def index(conn, _params) do
    account_id = conn.assigns.account_id

    render(conn, "index.json", consumers: Streams.list_consumers_for_account(account_id))
  end

  def show(conn, %{"id" => id}) do
    account_id = conn.assigns.account_id

    with {:ok, consumer} <- Streams.get_consumer_for_account(account_id, id) do
      render(conn, "show.json", consumer: consumer)
    end
  end

  def create(conn, params) do
    account_id = conn.assigns.account_id

    with {:ok, consumer} <- Streams.create_consumer_for_account_with_lifecycle(account_id, params) do
      render(conn, "show.json", consumer: consumer)
    end
  end

  def update(conn, params) do
    account_id = conn.assigns.account_id

    with {:ok, consumer} <- Streams.get_consumer_for_account(account_id, params["id"]),
         {:ok, consumer} <- Streams.update_consumer_with_lifecycle(consumer, params) do
      render(conn, "show.json", consumer: consumer)
    end
  end

  def delete(conn, %{"id" => id}) do
    account_id = conn.assigns.account_id

    with {:ok, consumer} <- Streams.get_consumer_for_account(account_id, id),
         {:ok, _consumer} <- Streams.delete_consumer_with_lifecycle(consumer) do
      render(conn, "delete.json", consumer: consumer)
    end
  end
end

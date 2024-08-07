defmodule SequinWeb.WebhookController do
  use SequinWeb, :controller

  alias Sequin.Replication
  alias SequinWeb.ApiFallbackPlug

  action_fallback ApiFallbackPlug

  def index(conn, _params) do
    account_id = conn.assigns.account_id
    webhooks = Replication.list_webhooks_for_account(account_id)
    render(conn, "index.json", webhooks: webhooks)
  end

  def show(conn, %{"id_or_name" => id_or_name}) do
    account_id = conn.assigns.account_id

    with {:ok, webhook} <- Replication.get_webhook_for_account(account_id, id_or_name) do
      render(conn, "show.json", webhook: webhook)
    end
  end

  def create(conn, params) do
    account_id = conn.assigns.account_id

    with {:ok, webhook} <- Replication.create_webhook_for_account(account_id, params) do
      conn
      |> put_status(:created)
      |> render("show.json", webhook: webhook)
    end
  end

  def update(conn, %{"id_or_name" => id_or_name} = params) do
    account_id = conn.assigns.account_id

    with {:ok, _} <- Replication.get_webhook_for_account(account_id, id_or_name),
         {:ok, updated_webhook} <- Replication.update_webhook_for_account(account_id, id_or_name, params) do
      render(conn, "show.json", webhook: updated_webhook)
    end
  end

  def delete(conn, %{"id_or_name" => id_or_name}) do
    account_id = conn.assigns.account_id

    with {:ok, webhook} <- Replication.get_webhook_for_account(account_id, id_or_name),
         {:ok, _webhook} <- Replication.delete_webhook_for_account(account_id, id_or_name) do
      render(conn, "delete.json", webhook: webhook)
    end
  end
end

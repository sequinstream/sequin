defmodule SequinWeb.WebhookIngestionController do
  use SequinWeb, :controller

  alias Sequin.Sources
  alias Sequin.Streams

  def ingest(conn, %{"webhook_name" => webhook_name} = params) do
    account_id = conn.assigns.account.id
    payload = Map.delete(params, "webhook_name")

    case Sources.get_webhook_for_account(account_id, webhook_name) do
      {:ok, webhook} ->
        id = :sha256 |> :crypto.hash(Jason.encode!(payload)) |> Base.encode16()

        message = %{
          subject: "#{webhook.name}.#{id}",
          data: Jason.encode!(payload)
        }

        case Streams.upsert_messages(webhook.stream_id, [message]) do
          {:ok, _count} ->
            send_resp(conn, :no_content, "")

          {:error, _error} ->
            conn
            |> put_status(:unprocessable_entity)
            |> json(%{error: "Failed to ingest message"})
        end

      {:error, %Sequin.Error.NotFoundError{entity: :webhook}} ->
        conn
        |> put_status(:not_found)
        |> json(%{error: "Webhook not found"})

      {:error, _} ->
        conn
        |> put_status(:internal_server_error)
        |> json(%{error: "An unexpected error occurred"})
    end
  end
end

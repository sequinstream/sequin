defmodule SequinWeb.WebhookIngestionController do
  use SequinWeb, :controller

  alias Sequin.Repo
  alias Sequin.Sources
  alias Sequin.Streams

  def ingest(conn, %{"webhook_name" => webhook_name} = params) do
    account_id = conn.assigns.account.id
    payload = Map.delete(params, "webhook_name")

    case Sources.get_webhook_for_account(account_id, webhook_name) do
      {:ok, webhook} ->
        case validate_auth(conn, webhook, payload) do
          :ok ->
            webhook = Repo.preload(webhook, :stream)
            id = :sha256 |> :crypto.hash(Jason.encode!(payload)) |> Base.encode16()

            message = %{
              key: "#{webhook.name}.#{id}",
              data: Jason.encode!(payload)
            }

            case Streams.upsert_messages(webhook.stream_id, [message]) do
              {:ok, _count} ->
                SequinWeb.WebhookChannel.broadcast("webhook:ingested", %{webhook: webhook, message: message})
                json(conn, %{success: true})

              {:error, _error} ->
                conn
                |> put_status(:unprocessable_entity)
                |> json(%{error: "Failed to ingest message"})
            end

          {:error, :unauthorized} ->
            conn
            |> put_status(:unauthorized)
            |> json(%{error: "Unauthorized"})
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

  defp validate_auth(conn, webhook, payload) do
    case webhook.auth_strategy do
      %{"type" => "hmac", "header_name" => header_name, "secret" => secret} ->
        # Decode the base64 secret
        decoded_secret = Base.decode64!(secret)

        # Create HMAC using the decoded secret
        expected_hmac =
          "hmac-sha256=" <>
            (:hmac
             |> :crypto.mac(:sha256, decoded_secret, Jason.encode!(payload))
             |> Base.encode16(case: :lower))

        case get_req_header(conn, String.downcase(header_name)) do
          [^expected_hmac] -> :ok
          _ -> {:error, :unauthorized}
        end

      _ ->
        :ok
    end
  end
end

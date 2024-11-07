defmodule Sequin.Consumers.WebhookSiteGenerator do
  @moduledoc """
  Generates webhook.site URLs for testing purposes.
  """

  @doc """
  Generates a new webhook.site URL.
  Returns `{:ok, uuid}` on success or `{:error, reason}` on failure.
  """
  def generate do
    url = "https://webhook.site/token"
    headers = [{"Content-Type", "application/json"}]

    body =
      Jason.encode!(%{
        default_status: 200,
        default_content: Jason.encode!(%{ok: true}),
        default_content_type: "application/json",
        cors: true
      })

    case Req.post(url, headers: headers, body: body) do
      {:ok, %Req.Response{status: 201, body: %{"uuid" => uuid}}} ->
        {:ok, uuid}

      {:error, reason} ->
        {:error, "Failed to generate Webhook.site URL: #{inspect(reason)}"}

      _ ->
        {:error, "Unexpected response from Webhook.site"}
    end
  end
end

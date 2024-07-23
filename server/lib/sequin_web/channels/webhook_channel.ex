defmodule SequinWeb.WebhookChannel do
  @moduledoc false
  use SequinWeb, :channel

  require Logger

  @impl true
  def join("webhook", _payload, socket) do
    {:ok, socket}
  end

  def broadcast(event, message) do
    SequinWeb.Endpoint.broadcast("webhook", event, message)
  end
end

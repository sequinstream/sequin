defmodule SequinWeb.ObserveChannel do
  @moduledoc false
  use SequinWeb, :channel

  alias Sequin.Streams
  alias SequinWeb.Presence

  require Logger

  @impl true
  def join("observe", _payload, socket) do
    send(self(), :after_join)
    {:ok, socket}
  end

  @impl true
  def handle_info(:after_join, socket) do
    {:ok, _} =
      Presence.track(socket, socket.id, %{
        online_at: inspect(System.system_time(:second))
      })

    push(socket, "presence_state", Presence.list(socket))
    {:noreply, socket}
  end

  def broadcast("messages:upserted", {stream_id, messages}) do
    if any_users_present?() do
      Logger.warning("Broadcasting messages:upserted for observation: #{stream_id}")
      subjects = Enum.map(messages, & &1.subject)
      messages = Streams.list_messages_for_stream(stream_id, %{subjects: subjects})
      SequinWeb.Endpoint.broadcast("observe", "messages:upserted", %{messages: messages})
    end
  end

  def broadcast(event, message) do
    SequinWeb.Endpoint.broadcast("observe", event, message)
  end

  def any_users_present? do
    "observe"
    |> Presence.list()
    |> Enum.empty?()
    |> Kernel.not()
  end
end

defmodule SequinWeb.ObserveChannel do
  @moduledoc false
  use SequinWeb, :channel

  alias Sequin.Streams
  alias SequinWeb.Presence

  require Logger

  @impl true
  def join("observe", _payload, socket) do
    send(self(), :after_join)
    {:ok, assign(socket, listening_consumer_id: nil, message_limit: nil, last_messages_hash: nil)}
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

  @impl true
  def handle_info(:fetch_messages, socket) do
    consumer_id = socket.assigns.listening_consumer_id
    limit = socket.assigns.message_limit

    if consumer_id do
      {:ok, consumer} = Streams.get_consumer(consumer_id)

      pending_messages =
        Streams.list_consumer_messages_for_consumer(consumer.stream_id, consumer_id, is_deliverable: false, limit: limit)

      upcoming_messages =
        Streams.list_consumer_messages_for_consumer(consumer.stream_id, consumer_id, is_deliverable: true, limit: limit)

      new_messages = %{
        pending_messages: pending_messages,
        upcoming_messages: upcoming_messages
      }

      new_hash = :erlang.phash2(new_messages)

      if new_hash == socket.assigns.last_messages_hash do
        timer_ref = schedule_messages_fetch()
        {:noreply, assign(socket, timer_ref: timer_ref)}
      else
        Logger.info("Sending updated messages for consumer: #{consumer_id}")
        push(socket, "consumer_messages", new_messages)
        timer_ref = schedule_messages_fetch()
        {:noreply, assign(socket, timer_ref: timer_ref, last_messages_hash: new_hash)}
      end
    else
      {:noreply, socket}
    end
  end

  @impl true
  def handle_in("listen_consumer", %{"consumer_id" => consumer_id, "limit" => limit}, socket) do
    Logger.info("Listening to consumer: #{consumer_id} with limit: #{limit}")

    if socket.assigns.listening_consumer_id do
      Process.cancel_timer(socket.assigns.timer_ref)
    end

    timer_ref = schedule_messages_fetch()
    {:reply, :ok, assign(socket, listening_consumer_id: consumer_id, timer_ref: timer_ref, message_limit: limit)}
  end

  @impl true
  def handle_in("clear_listening_consumer", _payload, socket) do
    Logger.info("Clearing listening consumer")

    if socket.assigns.listening_consumer_id do
      Process.cancel_timer(socket.assigns.timer_ref)
    end

    {:reply, :ok, assign(socket, listening_consumer_id: nil, timer_ref: nil, message_limit: nil, last_messages_hash: nil)}
  end

  defp schedule_messages_fetch do
    Process.send_after(self(), :fetch_messages, 100)
  end

  def broadcast("messages:upserted", {stream_id, messages}) do
    if any_users_present?() do
      Logger.warning("Broadcasting messages:upserted for observation: #{stream_id}")
      keys = Enum.map(messages, & &1.key)
      messages = Streams.list_messages_for_stream(stream_id, %{keys: keys})
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

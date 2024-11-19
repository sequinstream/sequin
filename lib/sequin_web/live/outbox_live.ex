defmodule SequinWeb.OutboxLive do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Consumers
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.HttpPullConsumer
  alias Sequin.Consumers.SinkConsumer
  alias SequinWeb.RouteHelpers

  @page_size 25

  @impl true
  def mount(_params, _session, socket) do
    consumers =
      socket
      |> current_account_id()
      |> Consumers.list_consumers_for_account()

    if connected?(socket), do: schedule_update()
    {:ok, assign(socket, consumer_id: nil, messages: [], consumers: consumers, paused: false, page: 0, total_count: 0)}
  end

  @impl true
  def handle_params(params, _uri, socket) do
    consumer_id = params["consumer"]
    paused = params["paused"] == "true"
    page = String.to_integer(params["page"] || "0")

    socket =
      if is_nil(consumer_id) do
        assign(socket, consumer_id: nil, messages: [], page: page, total_count: 0)
      else
        socket
        |> assign(consumer_id: consumer_id, page: page)
        |> load_consumer_messages()
      end

    {:noreply, assign(socket, paused: paused, page_size: @page_size)}
  end

  @impl true
  def render(assigns) do
    ~H"""
    <div id="outbox-live">
      <.svelte
        name="outbox/Index"
        props={
          %{
            consumer_id: @consumer_id,
            messages: encode_messages(@messages),
            consumers: encode_consumers(@consumers),
            totalCount: @total_count,
            pageSize: @page_size
          }
        }
      />
    </div>
    """
  end

  @impl true
  def handle_event("select_consumer", %{"consumer_id" => consumer_id}, socket) do
    {:noreply,
     socket
     |> assign(consumer_id: consumer_id)
     |> load_consumer_messages()}
  end

  @impl true
  def handle_event("pause_updates", _params, socket) do
    {:noreply, assign(socket, paused: true)}
  end

  @impl true
  def handle_event("resume_updates", _params, socket) do
    {:noreply, assign(socket, paused: false)}
  end

  def handle_event("change_page", %{"page" => page}, socket) do
    {:noreply,
     socket
     |> assign(page: page)
     |> load_consumer_messages()}
  end

  @impl true
  def handle_event("fetch_message_data", %{"message_id" => message_id}, socket) do
    message = Enum.find(socket.assigns.messages, &(&1.id == message_id))
    consumer = Sequin.Enum.find!(socket.assigns.consumers, &(&1.id == socket.assigns.consumer_id))

    case fetch_message_data(message, consumer) do
      {:ok, data} ->
        {:reply, %{data: data}, socket}

      {:error, reason} ->
        {:reply, %{error: reason}, socket}
    end
  end

  @impl true
  def handle_info(:update_messages, %{assigns: %{paused: true}} = socket) do
    schedule_update()
    {:noreply, socket}
  end

  @impl true
  def handle_info(:update_messages, socket) do
    socket =
      if socket.assigns.consumer_id do
        load_consumer_messages(socket)
      else
        socket
      end

    schedule_update()
    {:noreply, socket}
  end

  defp load_consumer_messages(socket) do
    consumer_id = socket.assigns.consumer_id
    page = socket.assigns.page
    consumer = Sequin.Enum.find!(socket.assigns.consumers, fn c -> c.id == consumer_id end)

    params = [
      order_by: {:asc, :id},
      limit: @page_size,
      offset: page * @page_size
    ]

    messages =
      case consumer do
        %{message_kind: :record} -> Consumers.list_consumer_records_for_consumer(consumer_id, params)
        %{message_kind: :event} -> Consumers.list_consumer_events_for_consumer(consumer_id, params)
      end

    assign(socket,
      messages: messages,
      total_count: Consumers.fast_count_messages_for_consumer(consumer)
    )
  end

  defp encode_messages(messages) do
    Enum.map(messages, fn
      %ConsumerRecord{} = message ->
        %{
          id: message.id,
          type: "record",
          consumer_id: message.consumer_id,
          commit_lsn: message.commit_lsn,
          ack_id: message.ack_id,
          deliver_count: message.deliver_count,
          last_delivered_at: message.last_delivered_at,
          record_pks: message.record_pks,
          table_oid: message.table_oid,
          not_visible_until: message.not_visible_until,
          inserted_at: message.inserted_at,
          data: message.data
        }

      %ConsumerEvent{} = message ->
        %{
          id: message.id,
          type: "event",
          consumer_id: message.consumer_id,
          commit_lsn: message.commit_lsn,
          ack_id: message.ack_id,
          deliver_count: message.deliver_count,
          last_delivered_at: message.last_delivered_at,
          record_pks: message.record_pks,
          table_oid: message.table_oid,
          not_visible_until: message.not_visible_until,
          inserted_at: message.inserted_at,
          data: message.data
        }
    end)
  end

  defp encode_consumers(consumers) do
    Enum.map(consumers, fn
      %HttpPullConsumer{} = consumer ->
        %{
          id: consumer.id,
          name: consumer.name,
          type: "http_pull",
          href: RouteHelpers.consumer_path(consumer)
        }

      %SinkConsumer{} = consumer ->
        %{
          id: consumer.id,
          name: consumer.name,
          type: "http_push",
          href: RouteHelpers.consumer_path(consumer)
        }
    end)
  end

  defp schedule_update do
    Process.send_after(self(), :update_messages, 100)
  end

  defp fetch_message_data(nil, _consumer) do
    {:error,
     """
     Message not found.

     The message may have been acknowledged and removed from the outbox.
     """}
  end

  defp fetch_message_data(%ConsumerRecord{} = record, %{message_kind: :record} = consumer) do
    case Consumers.put_source_data(consumer, [record]) do
      {:ok, [record]} ->
        {:ok, record.data}

      {:error, error} when is_exception(error) ->
        {:error, Exception.message(error)}

      {:error, error} when is_atom(error) ->
        {:error, Atom.to_string(error)}

      {:error, error} when is_binary(error) ->
        {:error, error}

      {:error, error} ->
        {:error, inspect(error)}
    end
  end

  defp fetch_message_data(%ConsumerEvent{} = event, %{message_kind: :event}) do
    {:ok, event.data}
  end
end

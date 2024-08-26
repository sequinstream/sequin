defmodule SequinWeb.ConsumersLive.Index do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Consumers
  alias Sequin.Consumers.HttpPullConsumer
  alias Sequin.Consumers.HttpPushConsumer
  alias SequinWeb.ConsumersLive.Form

  @impl Phoenix.LiveView
  def mount(_params, _session, socket) do
    account_id = current_account_id(socket)
    consumers = Consumers.list_consumers_for_account(account_id, :postgres_database)
    encoded_consumers = Enum.map(consumers, &encode_consumer/1)

    socket =
      socket
      |> assign(:consumers, encoded_consumers)
      |> assign(:form_errors, %{})

    {:ok, socket}
  end

  @impl Phoenix.LiveView
  def handle_params(params, _url, socket) do
    {:noreply, apply_action(socket, socket.assigns.live_action, params)}
  end

  @impl Phoenix.LiveView
  def render(%{live_action: :new} = assigns) do
    ~H"""
    <div id="consumers-index">
      <%= render_consumer_form(assigns) %>
    </div>
    """
  end

  def render(assigns) do
    ~H"""
    <div id="consumers-index">
      <.svelte
        name="consumers/Index"
        props={
          %{
            consumers: @consumers,
            formErrors: @form_errors
          }
        }
        socket={@socket}
      />
    </div>
    """
  end

  @impl Phoenix.LiveView
  def handle_event("consumer_clicked", %{"id" => id}, socket) do
    {:noreply, push_navigate(socket, to: ~p"/consumers/#{id}")}
  end

  defp apply_action(socket, :index, _params) do
    socket
    |> assign(:page_title, "Consumers")
    |> assign(:live_action, :index)
  end

  defp apply_action(socket, :new, %{"kind" => kind}) do
    socket
    |> assign(:page_title, "New Consumer")
    |> assign(:live_action, :new)
    |> assign(:form_kind, kind)
  end

  defp apply_action(socket, :new, _params) do
    apply_action(socket, :new, %{"kind" => "wizard"})
  end

  defp render_consumer_form(%{form_kind: "wizard"} = assigns) do
    ~H"""
    <.live_component
      current_account={@current_account}
      module={Form}
      id="new-consumer"
      action={:new}
      on_finish={&handle_create_finish/1}
    />
    """
  end

  defp render_consumer_form(%{form_kind: "pull"} = assigns) do
    ~H"""
    <.live_component
      current_account={@current_account}
      module={Form}
      id="new-consumer"
      action={:new}
      consumer={%HttpPullConsumer{}}
      on_finish={&handle_create_finish/1}
    />
    """
  end

  defp render_consumer_form(%{form_kind: "push"} = assigns) do
    ~H"""
    <.live_component
      current_account={@current_account}
      module={Form}
      id="new-consumer"
      action={:new}
      consumer={%HttpPushConsumer{}}
      on_finish={&handle_create_finish/1}
    />
    """
  end

  defp handle_create_finish(consumer) do
    send(self(), {:consumer_created, consumer})
  end

  @impl Phoenix.LiveView
  def handle_info({:consumer_created, consumer}, socket) do
    encoded_consumer = encode_consumer(consumer)

    {:noreply,
     socket
     |> update(:consumers, fn consumers -> [encoded_consumer | consumers] end)
     |> push_navigate(to: ~p"/consumers/#{consumer.id}")
     |> put_flash(:info, "Consumer created successfully")}
  end

  defp encode_consumer(consumer) do
    %{
      id: consumer.id,
      name: consumer.name,
      insertedAt: consumer.inserted_at,
      type: consumer_type(consumer),
      status: consumer.status,
      database_name: consumer.postgres_database.name
    }
  end

  defp consumer_type(%Consumers.HttpPullConsumer{}), do: "pull"
  defp consumer_type(%Consumers.HttpPushConsumer{}), do: "push"
end

defmodule SequinWeb.ConsumersLive.Index do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Consumers
  alias Sequin.Consumers.HttpPullConsumer
  alias Sequin.Consumers.HttpPushConsumer
  alias SequinWeb.Live.Consumers.HttpPullConsumerForm
  alias SequinWeb.Live.Consumers.HttpPushConsumerForm

  @impl Phoenix.LiveView
  def mount(_params, _session, socket) do
    account_id = current_account_id(socket)
    consumers = Consumers.list_consumers_for_account(account_id)
    encoded_consumers = Enum.map(consumers, &encode_consumer/1)

    socket =
      socket
      |> assign(:consumers, encoded_consumers)
      |> assign(:form_errors, %{})
      |> assign(:http_pull_consumer, %HttpPullConsumer{})
      |> assign(:http_push_consumer, %HttpPushConsumer{})

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
      <%= if @live_action == :new do %>
        <%= render_consumer_form(assigns) %>
      <% end %>
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

  @impl Phoenix.LiveView
  def handle_event("consumer_submitted", %{"name" => name}, socket) do
    account_id = current_account_id(socket)

    case Consumers.create_http_pull_consumer_with_lifecycle(%{
           account_id: account_id,
           name: name
         }) do
      {:ok, consumer} ->
        encoded_consumer = encode_consumer(consumer)

        socket =
          socket
          |> update(:consumers, fn consumers -> [encoded_consumer | consumers] end)
          |> assign(:form_errors, %{})

        {:noreply, push_navigate(socket, to: ~p"/consumers/#{consumer.id}")}

      {:error, changeset} ->
        errors = Sequin.Error.errors_on(changeset)
        {:noreply, assign(socket, :form_errors, errors)}
    end
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
    |> assign(:consumer_kind, kind)
  end

  defp render_consumer_form(%{consumer_kind: "pull"} = assigns) do
    ~H"""
    <.live_component
      current_account={@current_account}
      module={HttpPullConsumerForm}
      id="new-http-pull-consumer"
      action={:new}
      http_pull_consumer={@http_pull_consumer}
      on_finish={&handle_create_finish/1}
    />
    """
  end

  defp render_consumer_form(%{consumer_kind: "push"} = assigns) do
    ~H"""
    <.live_component
      current_account={@current_account}
      module={HttpPushConsumerForm}
      id="new-http-push-consumer"
      action={:new}
      http_push_consumer={@http_push_consumer}
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
      status: consumer.status
    }
  end

  defp consumer_type(%Consumers.HttpPullConsumer{}), do: "pull"
  defp consumer_type(%Consumers.HttpPushConsumer{}), do: "push"
end

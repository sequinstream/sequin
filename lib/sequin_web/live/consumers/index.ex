defmodule SequinWeb.ConsumersLive.Index do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Consumers
  alias Sequin.Consumers.HttpPullConsumer
  alias Sequin.Consumers.HttpPushConsumer
  alias Sequin.Databases
  alias Sequin.Health
  alias SequinWeb.ConsumersLive.Form

  @impl Phoenix.LiveView
  def mount(_params, _session, socket) do
    user = current_user(socket)
    account = current_account(socket)
    consumers = Consumers.list_consumers_for_account(account.id, :postgres_database)
    has_databases? = account.id |> Databases.list_dbs_for_account() |> Enum.any?()
    consumers = load_consumer_health(consumers)

    socket =
      if connected?(socket) do
        Process.send_after(self(), :update_health, 1000)

        push_event(socket, "ph-identify", %{
          userId: user.id,
          userEmail: user.email,
          userName: user.name
        })
      else
        socket
      end

    socket =
      socket
      |> assign(:consumers, consumers)
      |> assign(:form_errors, %{})
      |> assign(:has_databases?, has_databases?)

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
    encoded_consumers = Enum.map(assigns.consumers, &encode_consumer/1)
    assigns = assign(assigns, :encoded_consumers, encoded_consumers)

    ~H"""
    <div id="consumers-index">
      <.svelte
        name="consumers/Index"
        props={
          %{
            consumers: @encoded_consumers,
            formErrors: @form_errors,
            hasDatabases: @has_databases?
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
    <.live_component current_user={@current_user} module={Form} id="new-consumer" action={:new} />
    """
  end

  defp render_consumer_form(%{form_kind: "pull"} = assigns) do
    ~H"""
    <.live_component
      current_user={@current_user}
      module={Form}
      id="new-consumer"
      action={:new}
      consumer={%HttpPullConsumer{}}
    />
    """
  end

  defp render_consumer_form(%{form_kind: "push"} = assigns) do
    ~H"""
    <.live_component
      current_user={@current_user}
      module={Form}
      id="new-consumer"
      action={:new}
      consumer={%HttpPushConsumer{}}
    />
    """
  end

  @impl Phoenix.LiveView
  def handle_info(:update_health, socket) do
    Process.send_after(self(), :update_health, 1000)
    {:noreply, assign(socket, :consumers, load_consumer_health(socket.assigns.consumers))}
  end

  defp load_consumer_health(consumers) do
    Enum.map(consumers, fn consumer ->
      case Health.get(consumer) do
        {:ok, health} -> %{consumer | health: health}
        {:error, _} -> consumer
      end
    end)
  end

  defp encode_consumer(consumer) do
    %{
      id: consumer.id,
      name: consumer.name,
      insertedAt: consumer.inserted_at,
      type: consumer_type(consumer),
      status: consumer.status,
      database_name: consumer.postgres_database.name,
      health: Health.to_external(consumer.health)
    }
  end

  defp consumer_type(%Consumers.HttpPullConsumer{}), do: "pull"
  defp consumer_type(%Consumers.HttpPushConsumer{}), do: "push"
end

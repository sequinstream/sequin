defmodule SequinWeb.SinkConsumersLive.Form do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Consumers
  alias Sequin.Databases
  alias Sequin.Databases.DatabaseUpdateWorker
  alias SequinWeb.Components.ConsumerForm

  require Logger

  @impl true
  def render(assigns) do
    ~H"""
    <div id="consumers-form">
      <%= render_consumer_form(assigns) %>
    </div>
    """
  end

  @impl Phoenix.LiveView
  def mount(params, _session, socket) do
    account = current_account(socket)
    has_databases? = account.id |> Databases.list_dbs_for_account() |> Enum.any?()
    id = Map.get(params, "id")
    kind = Map.get(params, "type") || Map.get(params, "kind")

    case fetch_or_build_consumer(socket, kind, id) do
      {:ok, consumer} ->
        socket =
          socket
          |> assign(:has_databases?, has_databases?)
          |> assign(:self_hosted, Application.get_env(:sequin, :self_hosted))
          |> assign(:consumer, consumer)
          |> assign(:form_kind, kind)

        {:ok, socket, layout: {SequinWeb.Layouts, :app_no_sidenav}}

      {:error, _error} ->
        Logger.error("Sink not found (id=#{id})")
        {:ok, push_navigate(socket, to: ~p"/sinks")}
    end
  end

  defp fetch_or_build_consumer(_socket, kind, nil) do
    {:ok, ConsumerForm.default_for_kind(kind)}
  end

  defp fetch_or_build_consumer(socket, _kind, id) do
    Consumers.get_sink_consumer_for_account(current_account_id(socket), id)
  end

  @impl Phoenix.LiveView
  def handle_params(params, _url, socket) do
    {:noreply, apply_action(socket, socket.assigns.live_action, params)}
  end

  defp handle_edit_finish(updated_consumer) do
    send(self(), {:updated_consumer, updated_consumer})
  end

  defp apply_action(socket, :edit, _params) do
    socket
  end

  defp apply_action(socket, :new, _params) do
    # Refresh tables for all databases in the account
    account = current_account(socket)
    databases = Databases.list_dbs_for_account(account.id)
    Enum.each(databases, &DatabaseUpdateWorker.enqueue(&1.id))

    socket
  end

  defp render_consumer_form(assigns) do
    ~H"""
    <.live_component
      current_user={@current_user}
      module={ConsumerForm}
      id="consumer-form"
      action={@live_action}
      consumer={@consumer}
      on_finish={&handle_edit_finish/1}
    />
    """
  end

  @impl Phoenix.LiveView
  def handle_info({:database_tables_updated, _updated_database}, socket) do
    # Proxy down to ConsumerForm
    send_update(ConsumerForm, id: "consumer-form", event: :database_tables_updated)

    {:noreply, socket}
  end
end

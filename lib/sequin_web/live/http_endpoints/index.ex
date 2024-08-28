defmodule SequinWeb.HttpEndpointsLive.Index do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Consumers
  alias Sequin.Health

  @impl Phoenix.LiveView
  def mount(_params, _session, socket) do
    account_id = current_account_id(socket)
    http_endpoints = Consumers.list_http_endpoints_for_account(account_id, [:http_push_consumers])
    http_endpoints = load_http_endpoint_health(http_endpoints)

    if connected?(socket) do
      Process.send_after(self(), :update_health, 1000)
    end

    socket =
      socket
      |> assign(:http_endpoints, http_endpoints)
      |> assign(:form_errors, %{})

    {:ok, socket}
  end

  @impl Phoenix.LiveView
  def render(assigns) do
    assigns = assign(assigns, :encoded_http_endpoints, Enum.map(assigns.http_endpoints, &encode_http_endpoint/1))

    ~H"""
    <div id="http-endpoints-index">
      <.svelte
        name="http_endpoints/Index"
        props={
          %{
            httpEndpoints: @encoded_http_endpoints,
            formErrors: @form_errors
          }
        }
        socket={@socket}
      />
    </div>
    """
  end

  @impl Phoenix.LiveView
  def handle_event("http_endpoint_clicked", %{"id" => id}, socket) do
    {:noreply, push_navigate(socket, to: ~p"/http-endpoints/#{id}")}
  end

  @impl Phoenix.LiveView
  def handle_info(:update_health, socket) do
    Process.send_after(self(), :update_health, 1000)
    http_endpoints = load_http_endpoint_health(socket.assigns.http_endpoints)
    {:noreply, assign(socket, :http_endpoints, http_endpoints)}
  end

  defp load_http_endpoint_health(http_endpoints) do
    Enum.map(http_endpoints, fn http_endpoint ->
      case Health.get(http_endpoint) do
        {:ok, health} -> %{http_endpoint | health: health}
        {:error, _} -> http_endpoint
      end
    end)
  end

  defp encode_http_endpoint(http_endpoint) do
    %{
      id: http_endpoint.id,
      name: http_endpoint.name,
      baseUrl: http_endpoint.base_url,
      insertedAt: http_endpoint.inserted_at,
      httpPushConsumersCount: length(http_endpoint.http_push_consumers),
      health: Health.to_external(http_endpoint.health)
    }
  end
end

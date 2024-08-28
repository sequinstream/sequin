defmodule SequinWeb.HttpEndpointsLive.Show do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Consumers
  alias Sequin.Health

  @impl Phoenix.LiveView
  def mount(%{"id" => id}, _session, socket) do
    case Consumers.get_http_endpoint_for_account(current_account_id(socket), id) do
      {:ok, http_endpoint} ->
        if connected?(socket) do
          Process.send_after(self(), :update_health, 1000)
        end

        {:ok, health} = Health.get(http_endpoint)
        {:ok, assign(socket, http_endpoint: %{http_endpoint | health: health})}

      {:error, _} ->
        {:ok, push_navigate(socket, to: ~p"/http-endpoints")}
    end
  end

  @impl Phoenix.LiveView
  def handle_info(:update_health, socket) do
    Process.send_after(self(), :update_health, 1000)
    {:ok, health} = Health.get(socket.assigns.http_endpoint)
    {:noreply, assign(socket, :http_endpoint, %{socket.assigns.http_endpoint | health: health})}
  end

  @impl Phoenix.LiveView
  def render(assigns) do
    assigns = assign(assigns, :parent_id, "http-endpoint-show")

    ~H"""
    <div id={@parent_id}>
      <.svelte
        name="http_endpoints/Show"
        props={%{http_endpoint: encode_http_endpoint(@http_endpoint), parent_id: @parent_id}}
      />
    </div>
    """
  end

  @impl Phoenix.LiveView
  def handle_event("edit", _params, socket) do
    {:noreply, push_navigate(socket, to: ~p"/http-endpoints/#{socket.assigns.http_endpoint.id}/edit")}
  end

  def handle_event("delete", _params, socket) do
    if socket.assigns.http_endpoint.id |> Consumers.list_consumers_for_http_endpoint() |> Enum.empty?() do
      Consumers.delete_http_endpoint(socket.assigns.http_endpoint)
      {:reply, %{ok: true}, push_navigate(socket, to: ~p"/http-endpoints")}
    else
      {:reply, %{error: "Cannot delete HTTP endpoint with consumers."}, socket}
    end
  end

  defp encode_http_endpoint(http_endpoint) do
    %{
      id: http_endpoint.id,
      name: http_endpoint.name,
      baseUrl: http_endpoint.base_url,
      headers: http_endpoint.headers,
      health: Health.to_external(http_endpoint.health),
      inserted_at: http_endpoint.inserted_at,
      updated_at: http_endpoint.updated_at
    }
  end
end

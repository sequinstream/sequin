defmodule SequinWeb.HttpEndpointsLive.Index do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Consumers

  @impl Phoenix.LiveView
  def mount(_params, _session, socket) do
    account_id = current_account_id(socket)
    http_endpoints = Consumers.list_http_endpoints_for_account(account_id)
    encoded_http_endpoints = Enum.map(http_endpoints, &encode_http_endpoint/1)

    socket =
      socket
      |> assign(:http_endpoints, encoded_http_endpoints)
      |> assign(:form_errors, %{})

    {:ok, socket}
  end

  @impl Phoenix.LiveView
  def render(assigns) do
    ~H"""
    <div id="http-endpoints-index">
      <.svelte
        name="http_endpoints/Index"
        props={
          %{
            httpEndpoints: @http_endpoints,
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

  defp encode_http_endpoint(http_endpoint) do
    %{
      id: http_endpoint.id,
      name: http_endpoint.name,
      baseUrl: http_endpoint.base_url,
      insertedAt: http_endpoint.inserted_at
    }
  end
end

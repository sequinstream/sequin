defmodule SequinWeb.HttpEndpointsLive.Show do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Consumers
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Health
  alias Sequin.Health.CheckHttpEndpointHealthWorker
  alias Sequin.Metrics

  @impl Phoenix.LiveView
  def mount(%{"id" => id}, _session, socket) do
    case Consumers.find_http_endpoint_for_account(current_account_id(socket), id: id) do
      {:ok, http_endpoint} ->
        if connected?(socket) do
          Process.send_after(self(), :update_health, 1000)
          Process.send_after(self(), :update_metrics, 1000)
        end

        {:ok, health} = Health.health(http_endpoint)
        socket = assign(socket, http_endpoint: %{http_endpoint | health: health})
        {:ok, assign_metrics(socket)}

      {:error, _} ->
        {:ok, push_navigate(socket, to: ~p"/http-endpoints")}
    end
  end

  @impl Phoenix.LiveView
  def handle_info(:update_health, socket) do
    Process.send_after(self(), :update_health, 1000)
    {:noreply, assign_health(socket)}
  end

  def handle_info(:update_metrics, socket) do
    Process.send_after(self(), :update_metrics, 1000)
    {:noreply, assign_metrics(socket)}
  end

  @impl Phoenix.LiveView
  def render(assigns) do
    assigns = assign(assigns, :parent_id, "http-endpoint-show")
    # Get the count of sink consumers for this endpoint
    sink_consumer_count =
      assigns.http_endpoint.id
      |> Consumers.list_sink_consumers_for_http_endpoint()
      |> length()

    assigns = assign(assigns, :sink_consumer_count, sink_consumer_count)

    ~H"""
    <div id={@parent_id}>
      <.svelte
        name="http_endpoints/Show"
        props={
          %{
            http_endpoint: encode_http_endpoint(@http_endpoint),
            parent_id: @parent_id,
            metrics: @metrics,
            sink_consumer_count: @sink_consumer_count
          }
        }
      />
    </div>
    """
  end

  @impl Phoenix.LiveView
  def handle_event("edit", _params, socket) do
    {:noreply, push_navigate(socket, to: ~p"/http-endpoints/#{socket.assigns.http_endpoint.id}/edit")}
  end

  def handle_event("delete", _params, socket) do
    if socket.assigns.http_endpoint.id |> Consumers.list_sink_consumers_for_http_endpoint() |> Enum.empty?() do
      Consumers.delete_http_endpoint(socket.assigns.http_endpoint)
      socket = put_flash(socket, :toast, %{kind: :success, title: "HTTP endpoint deleted."})
      {:reply, %{ok: true}, push_navigate(socket, to: ~p"/http-endpoints")}
    else
      {:reply, %{error: "Cannot delete HTTP endpoint with consumers."}, socket}
    end
  end

  @impl Phoenix.LiveView
  def handle_event("refresh_health", _params, socket) do
    CheckHttpEndpointHealthWorker.enqueue_for_user(socket.assigns.http_endpoint.id)
    {:noreply, assign_health(socket)}
  end

  defp assign_health(socket) do
    case Health.health(socket.assigns.http_endpoint) do
      {:ok, health} ->
        assign(socket, http_endpoint: %{socket.assigns.http_endpoint | health: health})

      {:error, _} ->
        socket
    end
  end

  defp assign_metrics(socket) do
    http_endpoint = socket.assigns.http_endpoint

    {:ok, throughput} = Metrics.get_http_endpoint_throughput(http_endpoint)

    metrics = %{
      throughput: Float.round(throughput * 60, 1)
    }

    assign(socket, :metrics, metrics)
  end

  defp encode_http_endpoint(http_endpoint) do
    base_url =
      if http_endpoint.use_local_tunnel do
        http_endpoint
        |> HttpEndpoint.uri()
        |> Map.put(:host, "(localhost)")
        |> Map.put(:scheme, "http")
        |> Map.put(:port, nil)
        |> URI.to_string()
      else
        HttpEndpoint.url(http_endpoint)
      end

    %{
      id: http_endpoint.id,
      name: http_endpoint.name,
      baseUrl: base_url,
      headers: http_endpoint.headers,
      encryptedHeaders: http_endpoint.encrypted_headers || %{},
      health: Health.to_external(http_endpoint.health),
      inserted_at: http_endpoint.inserted_at,
      updated_at: http_endpoint.updated_at
    }
  end
end

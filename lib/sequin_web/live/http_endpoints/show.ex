defmodule SequinWeb.HttpEndpointsLive.Show do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Consumers

  @impl Phoenix.LiveView
  def mount(%{"id" => id}, _session, socket) do
    case Consumers.get_http_endpoint_for_account(current_account_id(socket), id) do
      {:ok, http_endpoint} ->
        {:ok, assign(socket, http_endpoint: http_endpoint, editing: false)}

      {:error, _} ->
        {:ok, push_navigate(socket, to: ~p"/http-endpoints")}
    end
  end

  @impl Phoenix.LiveView
  def handle_event("edit", _, socket) do
    {:noreply, assign(socket, editing: true)}
  end

  def handle_event("cancel_edit", _, socket) do
    {:noreply, assign(socket, editing: false)}
  end

  def handle_event("save", %{"http_endpoint" => params}, socket) do
    %{http_endpoint: http_endpoint} = socket.assigns

    case Consumers.update_http_endpoint(http_endpoint, params) do
      {:ok, updated_http_endpoint} ->
        {:noreply,
         socket
         |> assign(http_endpoint: updated_http_endpoint, editing: false)
         |> put_flash(:info, "HTTP endpoint updated successfully")}

      {:error, %Ecto.Changeset{} = changeset} ->
        {:noreply, assign(socket, changeset: changeset)}
    end
  end

  def handle_event("delete_http_endpoint", _, socket) do
    %{http_endpoint: http_endpoint} = socket.assigns

    case Consumers.delete_http_endpoint(http_endpoint) do
      {:ok, _} ->
        {:noreply,
         socket
         |> put_flash(:info, "HTTP endpoint deleted successfully")
         |> push_navigate(to: ~p"/http-endpoints")}

      {:error, %Ecto.Changeset{} = changeset} ->
        error_message = error_message_from_changeset(changeset)
        {:noreply, put_flash(socket, :error, error_message)}

      {:error, _} ->
        {:noreply, put_flash(socket, :error, "An unexpected error occurred while deleting the HTTP endpoint")}
    end
  end

  @impl Phoenix.LiveView
  def render(assigns) do
    ~H"""
    <div id="http-endpoint-show">
      <.svelte
        name="http_endpoints/Show"
        props={
          %{
            http_endpoint: encode_http_endpoint(@http_endpoint),
            editing: @editing
          }
        }
      />
    </div>
    """
  end

  defp encode_http_endpoint(http_endpoint) do
    %{
      id: http_endpoint.id,
      name: http_endpoint.name,
      base_url: http_endpoint.base_url,
      headers: http_endpoint.headers,
      inserted_at: http_endpoint.inserted_at,
      updated_at: http_endpoint.updated_at
    }
  end

  defp error_message_from_changeset(%{errors: errors}) do
    Enum.map_join(errors, ", ", fn {field, {message, _}} -> "#{Phoenix.Naming.humanize(field)} #{message}" end)
  end
end

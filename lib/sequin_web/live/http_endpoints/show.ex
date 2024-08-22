defmodule SequinWeb.HttpEndpointsLive.Show do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Consumers

  @impl Phoenix.LiveView
  def mount(%{"id" => id}, _session, socket) do
    case Consumers.get_http_endpoint_for_account(current_account_id(socket), id) do
      {:ok, http_endpoint} ->
        {:ok, assign(socket, http_endpoint: http_endpoint, editing_name: false)}

      {:error, _} ->
        {:ok, push_navigate(socket, to: ~p"/http-endpoints")}
    end
  end

  @impl Phoenix.LiveView
  def handle_event("edit_name", _, socket) do
    {:noreply, assign(socket, editing_name: true)}
  end

  def handle_event("cancel_edit_name", _, socket) do
    {:noreply, assign(socket, editing_name: false)}
  end

  def handle_event("save_name", %{"http_endpoint" => %{"name" => new_name}}, socket) do
    %{http_endpoint: http_endpoint} = socket.assigns

    case Consumers.update_http_endpoint(http_endpoint, %{name: new_name}) do
      {:ok, updated_http_endpoint} ->
        {:noreply, assign(socket, http_endpoint: updated_http_endpoint, editing_name: false)}

      {:error, _changeset} ->
        {:noreply, put_flash(socket, :error, "Failed to update HTTP endpoint name")}
    end
  end

  def handle_event("delete_http_endpoint", _, socket) do
    %{http_endpoint: http_endpoint} = socket.assigns

    case Consumers.delete_http_endpoint(http_endpoint) do
      {:ok, _} ->
        {:noreply, push_navigate(socket, to: ~p"/http-endpoints")}

      {:error, %Ecto.Changeset{} = changeset} ->
        error_message =
          case changeset.errors do
            [http_push_consumers: {"does not exist", _}] ->
              "Cannot delete because there are consumers using this endpoint"

            _ ->
              "Failed to delete HTTP endpoint"
          end

        {:noreply, put_flash(socket, :error, error_message)}

      {:error, _} ->
        {:noreply, put_flash(socket, :error, "An unexpected error occurred while deleting the HTTP endpoint")}
    end
  end

  @impl Phoenix.LiveView
  def render(assigns) do
    ~H"""
    <div class="min-h-screen bg-gray-50">
      <div class="max-w-7xl mx-auto py-12 px-4 sm:px-6 lg:px-8">
        <div class="bg-white shadow-lg rounded-2xl overflow-hidden transition-all duration-300 ease-in-out">
          <div class="px-8 py-10 border-b border-gray-100">
            <div class="flex items-center justify-between">
              <%= if @editing_name do %>
                <form phx-submit="save_name" class="flex-grow">
                  <input
                    id="http-endpoint-name"
                    type="text"
                    name="http_endpoint[name]"
                    value={@http_endpoint.name}
                    class="text-3xl font-bold text-gray-900 bg-transparent border-b-2 border-indigo-500 focus:outline-none focus:border-indigo-600 transition-all duration-300 ease-in-out w-full"
                    autofocus
                    phx-hook="FocusInput"
                  />
                </form>
                <div class="flex items-center ml-4">
                  <button
                    type="submit"
                    form="edit-name-form"
                    class="text-sm font-medium text-indigo-600 hover:text-indigo-700 transition-colors duration-150 ease-in-out mr-3"
                  >
                    Save
                  </button>
                  <button
                    type="button"
                    phx-click="cancel_edit_name"
                    class="text-sm font-medium text-gray-500 hover:text-gray-700 transition-colors duration-150 ease-in-out"
                  >
                    Cancel
                  </button>
                </div>
              <% else %>
                <h1 class="text-3xl font-bold text-gray-900 transition-all duration-300 ease-in-out">
                  <%= @http_endpoint.name %>
                </h1>
                <button
                  phx-click="edit_name"
                  class="ml-4 text-sm font-medium text-gray-500 hover:text-indigo-600 transition-colors duration-150 ease-in-out focus:outline-none"
                >
                  <svg
                    xmlns="http://www.w3.org/2000/svg"
                    class="h-5 w-5"
                    viewBox="0 0 20 20"
                    fill="currentColor"
                  >
                    <path d="M13.586 3.586a2 2 0 112.828 2.828l-.793.793-2.828-2.828.793-.793zM11.379 5.793L3 14.172V17h2.828l8.38-8.379-2.83-2.828z" />
                  </svg>
                </button>
              <% end %>
            </div>
            <p class="mt-3 text-sm text-gray-500 font-medium">
              HTTP Endpoint ID: <%= @http_endpoint.id %>
            </p>
          </div>

          <div class="px-6 py-6 grid grid-cols-1 gap-y-6 gap-x-4 sm:grid-cols-2">
            <div>
              <h2 class="text-lg font-medium text-gray-900">Endpoint Details</h2>
              <dl class="mt-3 space-y-3">
                <div class="flex justify-between">
                  <dt class="text-sm font-medium text-gray-500">Base URL</dt>
                  <dd class="text-sm text-gray-900"><%= @http_endpoint.base_url %></dd>
                </div>
              </dl>
            </div>

            <div>
              <h2 class="text-lg font-medium text-gray-900">Headers</h2>
              <dl class="mt-3 space-y-3">
                <%= for {key, value} <- @http_endpoint.headers do %>
                  <div class="flex justify-between">
                    <dt class="text-sm font-medium text-gray-500"><%= key %></dt>
                    <dd class="text-sm text-gray-900"><%= value %></dd>
                  </div>
                <% end %>
              </dl>
            </div>
          </div>

          <div class="px-6 py-6 border-t border-gray-200">
            <button
              phx-click="delete_http_endpoint"
              data-confirm="Are you sure you want to delete this HTTP endpoint? This action cannot be undone."
              class="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-red-600 hover:bg-red-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-red-500"
            >
              Delete HTTP Endpoint
            </button>
          </div>
        </div>
      </div>
    </div>
    """
  end
end

defmodule SequinWeb.DatabasesLive.Show do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Databases

  @impl Phoenix.LiveView
  def mount(%{"id" => id}, _session, socket) do
    case Databases.get_db_for_account(current_account_id(socket), id) do
      {:ok, database} ->
        {:ok, assign(socket, database: database, refreshing_tables: false)}

      {:error, _} ->
        {:ok, push_navigate(socket, to: ~p"/databases")}
    end
  end

  @impl Phoenix.LiveView
  def handle_event("refresh_tables", _, socket) do
    %{database: database} = socket.assigns

    Task.async(fn -> Databases.update_tables(database) end)

    {:noreply, assign(socket, refreshing_tables: true)}
  end

  def handle_event("delete_database", _, socket) do
    %{database: database} = socket.assigns

    {:ok, _} = Databases.delete_db(database)

    {:noreply, push_navigate(socket, to: ~p"/databases")}
  end

  @impl Phoenix.LiveView
  def handle_info({ref, {:ok, updated_db}}, socket) do
    Process.demonitor(ref, [:flush])
    {:noreply, assign(socket, database: updated_db, refreshing_tables: false)}
  end

  def handle_info({ref, {:error, _reason}}, socket) do
    Process.demonitor(ref, [:flush])
    {:noreply, assign(socket, refreshing_tables: false)}
  end

  @impl Phoenix.LiveView
  def render(assigns) do
    ~H"""
    <div class=" min-h-screen">
      <div class="max-w-7xl mx-auto py-12 px-4 sm:px-6 lg:px-8">
        <div class="bg-white shadow-xl rounded-lg overflow-hidden">
          <div class="px-6 py-8 border-b border-gray-200">
            <h1 class="text-3xl font-bold text-gray-900"><%= @database.name %></h1>
            <p class="mt-2 text-sm text-gray-500">Database ID: <%= @database.id %></p>
          </div>

          <div class="px-6 py-6 grid grid-cols-1 gap-y-6 gap-x-4 sm:grid-cols-2">
            <div>
              <h2 class="text-lg font-medium text-gray-900">Connection Details</h2>
              <dl class="mt-3 space-y-3">
                <div class="flex justify-between">
                  <dt class="text-sm font-medium text-gray-500">Hostname</dt>
                  <dd class="text-sm text-gray-900"><%= @database.hostname %></dd>
                </div>
                <div class="flex justify-between">
                  <dt class="text-sm font-medium text-gray-500">Port</dt>
                  <dd class="text-sm text-gray-900"><%= @database.port %></dd>
                </div>
                <div class="flex justify-between">
                  <dt class="text-sm font-medium text-gray-500">Database</dt>
                  <dd class="text-sm text-gray-900"><%= @database.database %></dd>
                </div>
                <div class="flex justify-between">
                  <dt class="text-sm font-medium text-gray-500">Username</dt>
                  <dd class="text-sm text-gray-900"><%= @database.username %></dd>
                </div>
                <div class="flex justify-between">
                  <dt class="text-sm font-medium text-gray-500">SSL</dt>
                  <dd class="text-sm text-gray-900"><%= @database.ssl %></dd>
                </div>
              </dl>
            </div>

            <div>
              <h2 class="text-lg font-medium text-gray-900">Configuration</h2>
              <dl class="mt-3 space-y-3">
                <div class="flex justify-between">
                  <dt class="text-sm font-medium text-gray-500">Pool Size</dt>
                  <dd class="text-sm text-gray-900"><%= @database.pool_size %></dd>
                </div>
                <div class="flex justify-between">
                  <dt class="text-sm font-medium text-gray-500">Queue Target</dt>
                  <dd class="text-sm text-gray-900"><%= @database.queue_target %></dd>
                </div>
                <div class="flex justify-between">
                  <dt class="text-sm font-medium text-gray-500">Queue Interval</dt>
                  <dd class="text-sm text-gray-900"><%= @database.queue_interval %></dd>
                </div>
                <div class="flex justify-between">
                  <dt class="text-sm font-medium text-gray-500">Tables Refreshed At</dt>
                  <dd class="text-sm text-gray-900"><%= @database.tables_refreshed_at %></dd>
                </div>
              </dl>
            </div>
          </div>

          <div class="px-6 py-6 border-t border-gray-200">
            <div class="flex justify-between items-center mb-4">
              <h2 class="text-lg font-medium text-gray-900">Tables</h2>
              <button
                phx-click="refresh_tables"
                class="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-indigo-600 hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500"
                disabled={@refreshing_tables}
              >
                <%= if @refreshing_tables do %>
                  <svg
                    class="animate-spin -ml-1 mr-3 h-5 w-5 text-white"
                    xmlns="http://www.w3.org/2000/svg"
                    fill="none"
                    viewBox="0 0 24 24"
                  >
                    <circle
                      class="opacity-25"
                      cx="12"
                      cy="12"
                      r="10"
                      stroke="currentColor"
                      stroke-width="4"
                    >
                    </circle>
                    <path
                      class="opacity-75"
                      fill="currentColor"
                      d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
                    >
                    </path>
                  </svg>
                  Refreshing...
                <% else %>
                  Refresh Tables
                <% end %>
              </button>
            </div>
            <div class="space-y-6">
              <%= for table <- @database.tables do %>
                <div class="bg-gray-50 rounded-lg p-4">
                  <h3 class="text-md font-semibold text-gray-900 mb-2">
                    <%= table.schema %>.<%= table.name %>
                    <span class="text-sm font-normal text-gray-500">(OID: <%= table.oid %>)</span>
                  </h3>
                  <ul class="space-y-1">
                    <%= for column <- table.columns do %>
                      <li class="text-sm">
                        <span class="font-medium"><%= column.name %></span>
                        <span class="text-gray-500">(<%= column.type %>)</span>
                        <%= if column.is_pk? do %>
                          <span class="ml-1 px-2 inline-flex text-xs leading-5 font-semibold rounded-full bg-green-100 text-green-800">
                            PK
                          </span>
                        <% end %>
                      </li>
                    <% end %>
                  </ul>
                </div>
              <% end %>
            </div>
          </div>

          <div class="px-6 py-6 border-t border-gray-200">
            <button
              phx-click="delete_database"
              data-confirm="Are you sure you want to delete this database? This action cannot be undone."
              class="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-red-600 hover:bg-red-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-red-500"
            >
              Delete Database
            </button>
          </div>
        </div>
      </div>
    </div>
    """
  end
end

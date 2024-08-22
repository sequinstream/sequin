defmodule SequinWeb.HomeLive do
  @moduledoc false
  use SequinWeb, :live_view

  def mount(_params, _session, socket) do
    if connected?(socket) do
      case check_user_setup() do
        {:ok, :complete} ->
          {:ok, push_navigate(socket, to: "/consumers")}

        {:ok, :incomplete, account} ->
          db_info = get_db_info()

          {:ok,
           socket
           |> assign(step: :welcome_connected, replication_form: %{})
           |> assign(account: account, db_info: db_info), layout: {SequinWeb.Layouts, :app_no_main_no_sidenav}}
      end
    else
      db_info = get_db_info()

      {:ok,
       socket
       |> assign(step: :welcome_connected, replication_form: %{})
       |> assign(account: nil, db_info: db_info), layout: {SequinWeb.Layouts, :app_no_main_no_sidenav}}
    end
  end

  def render(assigns) do
    ~H"""
    <div class="flex flex-col items-center justify-between h-screen w-full dark:bg-zinc-900 relative">
      <div class="flex-grow flex items-center justify-center w-full">
        <div class="w-full max-w-3xl mx-auto px-4">
          <.welcome_connected_component db_info={@db_info} />
        </div>
      </div>
      <div class="fixed bottom-0 left-0 right-0 bg-white dark:bg-zinc-800 shadow-md">
        <div class="w-full max-w-3xl mx-auto px-4 py-4">
          <div class="flex justify-end">
            <button
              phx-click="get_started"
              class="bg-blue-500 hover:bg-blue-600 text-white font-bold py-2 px-4 rounded"
            >
              Get Started
            </button>
          </div>
        </div>
      </div>
    </div>
    """
  end

  def welcome_connected_component(assigns) do
    ~H"""
    <div>
      <h1 class="text-4xl font-bold text-zinc-800 dark:text-zinc-200 mb-4">
        Welcome to Sequin
      </h1>
      <div class="mb-6">
        <div class="flex items-center mb-4">
          <.icon name="hero-check-circle" class="w-8 h-8 text-green-500 mr-2" />
          <p class="text-xl text-zinc-700 dark:text-zinc-300">
            Your database is connected and Sequin is ready to go.
          </p>
        </div>
      </div>
      <div class="mt-6">
        <div class="flex items-center mb-4">
          <h2 class="text-2xl font-bold text-zinc-800 dark:text-zinc-200">Database Configuration</h2>
        </div>
        <div class="bg-white dark:bg-zinc-800 shadow rounded-lg p-6">
          <div class="grid grid-cols-2 gap-4">
            <div>
              <p class="text-sm font-medium text-gray-500 dark:text-gray-400">Database name</p>
              <p class="mt-1 text-sm text-gray-900 dark:text-gray-200"><%= @db_info.database %></p>
            </div>
            <div>
              <p class="text-sm font-medium text-gray-500 dark:text-gray-400">Hostname</p>
              <p class="mt-1 text-sm text-gray-900 dark:text-gray-200"><%= @db_info.hostname %></p>
            </div>
            <div>
              <p class="text-sm font-medium text-gray-500 dark:text-gray-400">Username</p>
              <p class="mt-1 text-sm text-gray-900 dark:text-gray-200"><%= @db_info.username %></p>
            </div>
            <div>
              <p class="text-sm font-medium text-gray-500 dark:text-gray-400">Pool size</p>
              <p class="mt-1 text-sm text-gray-900 dark:text-gray-200"><%= @db_info.pool_size %></p>
            </div>
          </div>
        </div>
      </div>
    </div>
    """
  end

  def handle_event("get_started", _, socket) do
    create_account!()
    {:noreply, push_navigate(socket, to: "/consumers")}
  end

  defp get_db_info do
    config = Sequin.Repo.config()

    %{
      database: config[:database],
      hostname: config[:hostname],
      username: config[:username],
      pool_size: config[:pool_size]
    }
  end

  defp check_user_setup do
    case Sequin.Accounts.list_accounts() do
      [] ->
        {:ok, :incomplete, nil}

      [_ | _] ->
        {:ok, :complete}
    end
  rescue
    DBConnection.ConnectionError ->
      {:ok, :incomplete, nil}
  end

  defp create_account! do
    {:ok, _} = Sequin.Accounts.create_account(%{})
  end
end

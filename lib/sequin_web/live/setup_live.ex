defmodule SequinWeb.SetupLive do
  @moduledoc false
  use SequinWeb, :live_view

  @stub_user_email "admin@sequinstream.com"
  @stub_user_password "password!"

  def mount(_params, _session, socket) do
    db_info = get_db_info()

    case check_user_setup() do
      {:ok, :complete} ->
        {:ok, push_navigate(socket, to: "/consumers")}

      {:ok, :incomplete, account} ->
        {:ok,
         socket
         |> assign(step: :welcome_connected, replication_form: %{})
         |> assign(account: account, db_info: db_info), layout: {SequinWeb.Layouts, :app_no_main_no_sidenav}}

      {:ok, :disconnected} ->
        schedule_db_check()
        {:ok, assign(socket, step: :disconnected, db_info: db_info), layout: {SequinWeb.Layouts, :app_no_main_no_sidenav}}
    end
  end

  def render(assigns) do
    ~H"""
    <div class="flex flex-col items-center justify-between h-screen w-full dark:bg-zinc-900 relative">
      <div class="flex-grow flex items-center justify-center w-full">
        <div class="w-full max-w-3xl mx-auto px-4">
          <%= case @step do %>
            <% :welcome_connected -> %>
              <.welcome_connected_component db_info={@db_info} />
            <% :disconnected -> %>
              <.disconnected_component db_info={@db_info} />
          <% end %>
        </div>
      </div>
      <div class="fixed bottom-20 left-0 right-0 bg-white dark:bg-zinc-800">
        <div class="w-full max-w-3xl mx-auto px-4 py-4">
          <div class="flex justify-end">
            <button
              phx-click="get_started"
              class="bg-blue-500 hover:bg-blue-600 text-white font-bold py-2 px-4 rounded disabled:opacity-50 disabled:cursor-not-allowed"
              disabled={@step == :disconnected}
            >
              Get started
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
        <.database_config_component db_info={@db_info} />
      </div>
    </div>
    """
  end

  def disconnected_component(assigns) do
    ~H"""
    <div>
      <h1 class="text-4xl font-bold text-zinc-800 dark:text-zinc-200 mb-4">
        Welcome to Sequin
      </h1>
      <div class="mb-6">
        <div class="flex items-center mb-4">
          <.icon name="hero-x-circle" class="w-8 h-8 text-red-500 mr-2" />
          <p class="text-xl text-zinc-700 dark:text-zinc-300">
            Unable to connect to your database. Please check your configuration.
          </p>
        </div>
      </div>
      <div class="mt-6">
        <div class="flex items-center mb-4">
          <h2 class="text-2xl font-bold text-zinc-800 dark:text-zinc-200">Database Configuration</h2>
        </div>
        <.database_config_component db_info={@db_info} />
      </div>
    </div>
    """
  end

  def database_config_component(assigns) do
    ~H"""
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
    """
  end

  def handle_event("get_started", _, socket) do
    create_account_with_user!()

    socket =
      socket
      |> push_navigate(to: "/login")
      |> put_flash(:toast, %{
        kind: :info,
        title: "Instance is ready",
        description: "Please login with the user `#{@stub_user_email}` and password `#{@stub_user_password}`",
        duration: 60_000
      })

    {:noreply, socket}
  end

  def handle_info(:check_db_connection, socket) do
    db_info = get_db_info()

    case check_user_setup() do
      {:ok, :complete} ->
        {:noreply, push_navigate(socket, to: "/consumers")}

      {:ok, :incomplete, account} ->
        {:noreply, assign(socket, step: :welcome_connected, replication_form: %{}, account: account, db_info: db_info)}

      {:ok, :disconnected} ->
        schedule_db_check()
        {:noreply, assign(socket, step: :disconnected, db_info: db_info)}
    end
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
      {:ok, :disconnected}
  end

  defp create_account_with_user! do
    {:ok, _user} = Sequin.Accounts.register_user(:identity, %{email: @stub_user_email, password: @stub_user_password})
  end

  defp schedule_db_check do
    Process.send_after(self(), :check_db_connection, 1000)
  end
end

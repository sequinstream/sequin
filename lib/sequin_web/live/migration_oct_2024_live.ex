defmodule SequinWeb.MigrationOct2024Live do
  @moduledoc false
  use SequinWeb, :live_view

  alias Mix.Tasks.Sequin.MigrateToSequences

  def mount(_params, _session, socket) do
    if connected?(socket) do
      send(self(), :perform_migration)
    end

    {:ok,
     assign(socket,
       page_title: "October 2024 Migration",
       status: :migrating
     ), layout: {SequinWeb.Layouts, :app_no_main_no_sidenav}}
  end

  def render(assigns) do
    ~H"""
    <div class="flex flex-col items-center justify-center min-h-screen bg-gray-100 dark:bg-zinc-900 px-4 min-w-full">
      <div class="max-w-3xl w-full bg-white dark:bg-zinc-800 shadow-md rounded-lg p-8">
        <h1 class="text-2xl font-bold mb-4 text-center text-gray-800 dark:text-gray-200">
          October 2024 Migration
        </h1>
        <%= case @status do %>
          <% :migrating -> %>
            <p class="text-center text-gray-600 dark:text-gray-400">
              Migration in progress... Please wait.
            </p>
            <div class="flex justify-center mt-4">
              <.icon name="hero-arrow-path" class="w-8 h-8 animate-spin text-blue-500" />
            </div>
          <% :success -> %>
            <p class="text-center text-green-600 dark:text-green-400">
              Migration completed successfully!
            </p>
            <div class="flex justify-center mt-4">
              <.icon name="hero-check-circle" class="w-8 h-8 text-green-500" />
            </div>
          <% :error -> %>
            <p class="text-center text-red-600 dark:text-red-400">
              An error occurred while migrating your consumers to the latest version of Sequin. Please
              <a href="https://discord.gg/BV8wFXvNtY" target="_blank">join our Discord</a>
              for support.
            </p>
            <div class="flex justify-center mt-4">
              <.icon name="hero-x-circle" class="w-8 h-8 text-red-500" />
            </div>
        <% end %>
      </div>
    </div>
    """
  end

  def handle_info(:perform_migration, socket) do
    case MigrateToSequences.migrate_consumers() do
      {:ok, _} ->
        {:noreply, push_navigate(socket, to: "/")}

      {:error, _} ->
        {:noreply, assign(socket, status: :error)}
    end
  end

  def handle_info(:redirect, socket) do
    {:noreply, push_navigate(socket, to: "/")}
  end
end

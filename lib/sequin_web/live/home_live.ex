defmodule SequinWeb.HomeLive do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Databases
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Replication
  alias Sequin.Replication.PostgresReplicationSlot
  alias Sequin.Repo

  @fade_duration 1000

  def mount(_params, _session, socket) do
    case check_user_setup() do
      {:ok, :complete} ->
        {:ok, push_navigate(socket, to: "/consumers")}

      {:ok, :incomplete, account} ->
        {:ok,
         socket
         |> assign(step: :welcome, opacity: 100, fade_duration: @fade_duration, replication_form: %{})
         |> assign(account: account), layout: {SequinWeb.Layouts, :app_no_sidenav}}
    end
  end

  def render(assigns) do
    ~H"""
    <div class="flex flex-col items-center justify-between min-h-screen w-full dark:bg-zinc-900 relative">
      <div class="flex-grow flex items-center justify-center w-full">
        <div class="w-full max-w-3xl mx-auto px-4">
          <%= case @step do %>
            <% :welcome -> %>
              <.welcome_component opacity={@opacity} fade_duration={@fade_duration} />
            <% :connecting -> %>
              <.connecting_component opacity={@opacity} fade_duration={@fade_duration} />
            <% :connected -> %>
              <.connected_component
                opacity={@opacity}
                fade_duration={@fade_duration}
                db_info={@db_info}
              />
            <% :replication_setup -> %>
              <.replication_setup_component
                opacity={@opacity}
                fade_duration={@fade_duration}
                db_info={@db_info}
                replication_form={@replication_form}
                replication_setup_complete={@replication_setup_complete}
              />
          <% end %>
        </div>
      </div>
      <div class="fixed bottom-0 left-0 right-0 bg-white dark:bg-zinc-800 shadow-md">
        <div class="w-full max-w-3xl mx-auto px-4 py-4">
          <div class="flex justify-end">
            <button
              phx-click="next_step"
              class="bg-blue-500 hover:bg-blue-600 text-white font-bold py-2 px-4 rounded disabled:opacity-50 disabled:hover:bg-blue-500"
              disabled={@step == :replication_setup and not @replication_setup_complete}
            >
              <%= if @step == :replication_setup and @replication_setup_complete,
                do: "Finish",
                else: "Next" %>
            </button>
          </div>
        </div>
      </div>
    </div>
    """
  end

  def welcome_component(assigns) do
    ~H"""
    <h1 class={"text-4xl font-bold text-zinc-800 dark:text-zinc-200 transition-opacity duration-#{@fade_duration} ease-in-out opacity-#{@opacity}"}>
      Welcome to Sequin
    </h1>
    """
  end

  def connecting_component(assigns) do
    ~H"""
    <h1 class={"text-4xl font-bold text-zinc-800 dark:text-zinc-200 transition-opacity duration-#{@fade_duration} ease-in-out opacity-#{@opacity}"}>
      Connecting to database...
    </h1>
    """
  end

  def connected_component(assigns) do
    ~H"""
    <div class={"transition-opacity duration-#{@fade_duration} ease-in-out opacity-#{@opacity}"}>
      <div class="flex items-center space-x-2 mb-4">
        <.icon name="hero-check-circle" class="w-8 h-8 text-green-500" />
        <h1 class="text-4xl font-bold text-zinc-800 dark:text-zinc-200">
          Connected to database
        </h1>
      </div>
      <div class="mt-6">
        <h2 class="text-2xl font-bold text-zinc-800 dark:text-zinc-200 mb-4">Database Information</h2>
        <ul class="space-y-2 text-zinc-700 dark:text-zinc-300">
          <li><strong>Database:</strong> <%= @db_info.database %></li>
          <li><strong>Hostname:</strong> <%= @db_info.hostname %></li>
          <li><strong>Username:</strong> <%= @db_info.username %></li>
        </ul>
      </div>
    </div>
    """
  end

  def replication_setup_component(assigns) do
    ~H"""
    <div class={"transition-opacity duration-#{@fade_duration} ease-in-out opacity-#{@opacity}"}>
      <h1 class="text-4xl font-bold text-zinc-800 dark:text-zinc-200 mb-6">
        Set Up Replication
      </h1>
      <%= if @replication_setup_complete do %>
        <div class="space-y-6">
          <div class="flex items-center space-x-2 mb-4">
            <.icon name="hero-check-circle" class="w-8 h-8 text-green-500" />
            <h2 class="text-2xl font-bold text-zinc-800 dark:text-zinc-200">
              Replication Set Up Successfully
            </h2>
          </div>
          <p class="text-zinc-700 dark:text-zinc-300">
            Your database is now configured for replication with Sequin.
          </p>
          <ul class="space-y-2 text-zinc-700 dark:text-zinc-300">
            <li><strong>Slot Name:</strong> <%= @replication_form.slot_name %></li>
            <li><strong>Publication Name:</strong> <%= @replication_form.publication_name %></li>
          </ul>
        </div>
      <% else %>
        <div class="space-y-6">
          <p class="text-zinc-700 dark:text-zinc-300">
            To set up replication, you need to create a replication slot and publication. Run the following SQL commands on your database:
          </p>
          <pre class="bg-gray-100 dark:bg-gray-800 p-4 rounded-md">
    SELECT pg_create_logical_replication_slot('sequin_slot', 'pgoutput');
    CREATE PUBLICATION sequin_pub FOR ALL TABLES;</pre>
          <form phx-submit="setup_replication" class="space-y-4">
            <div>
              <label
                for="slot_name"
                class="block text-sm font-medium text-gray-700 dark:text-gray-300"
              >
                Slot Name
              </label>
              <input
                type="text"
                id="slot_name"
                name="slot_name"
                value={@replication_form[:slot_name] || "sequin_slot"}
                class="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-300 focus:ring focus:ring-indigo-200 focus:ring-opacity-50"
              />
            </div>
            <div>
              <label
                for="publication_name"
                class="block text-sm font-medium text-gray-700 dark:text-gray-300"
              >
                Publication Name
              </label>
              <input
                type="text"
                id="publication_name"
                name="publication_name"
                value={@replication_form[:publication_name] || "sequin_pub"}
                class="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-300 focus:ring focus:ring-indigo-200 focus:ring-opacity-50"
              />
            </div>
            <button
              type="submit"
              class="bg-blue-500 hover:bg-blue-600 text-white font-bold py-2 px-4 rounded"
            >
              Set Up Replication
            </button>
          </form>
        </div>
      <% end %>
    </div>
    """
  end

  defp handle_step(:welcome, socket) do
    assign(socket, step: :welcome)
  end

  defp handle_step(:connecting, socket) do
    connection_successful =
      try do
        Sequin.Accounts.list_accounts()
        true
      rescue
        _ -> false
      end

    db_info = get_db_info()

    if connection_successful do
      account = socket.assigns[:account] || create_account()

      socket
      |> assign(step: :connected)
      |> assign(db_info: db_info)
      |> assign(account: account)
    else
      socket
      |> assign(step: :not_connected)
      |> assign(db_info: db_info)
    end
  end

  defp handle_step(:connected, socket) do
    assign(socket, step: :connected)
  end

  defp handle_step(:replication_setup, socket) do
    assign(socket,
      step: :replication_setup,
      replication_form: %{
        slot_name: "sequin_slot",
        publication_name: "sequin_pub"
      },
      replication_setup_complete: false
    )
  end

  def handle_event("next_step", _, socket) do
    next_step =
      case socket.assigns.step do
        :welcome ->
          :connecting

        :connecting ->
          :connected

        :connected ->
          :replication_setup

        :replication_setup ->
          if socket.assigns.replication_setup_complete do
            {:halt, push_navigate(socket, to: "/consumers")}
          else
            :replication_setup
          end
      end

    case next_step do
      {:halt, new_socket} -> {:noreply, new_socket}
      _ -> {:noreply, handle_step(next_step, socket)}
    end
  end

  def handle_event("setup_replication", %{"slot_name" => slot_name, "publication_name" => publication_name}, socket) do
    replication_params = %{
      slot_name: slot_name,
      publication_name: publication_name
    }

    case validate_and_create_replication(socket.assigns.account.id, replication_params) do
      :ok ->
        {:noreply,
         socket
         |> put_flash(:info, "Replication set up successfully")
         |> assign(replication_setup_complete: true)
         |> assign(replication_form: replication_params)}

      {:error, reason} ->
        {:noreply,
         socket
         |> put_flash(:error, "Failed to set up replication: #{reason}")
         |> assign(replication_form: replication_params)}
    end
  end

  defp validate_and_create_replication(account_id, replication_params) do
    config = Sequin.Repo.config()

    db = %PostgresDatabase{
      account_id: account_id,
      database: config[:database],
      hostname: config[:hostname],
      username: config[:username],
      pool_size: config[:pool_size],
      password: config[:password],
      port: config[:port] || 5432,
      name: "default"
    }

    with :ok <- Sequin.Replication.validate_replication_config(db, replication_params),
         {:ok, _} <- create_db_and_replication(account_id, db, replication_params) do
      :ok
    else
      {:error, %Ecto.Changeset{} = changeset} ->
        {:error, "Validation failed: #{error_messages(changeset)}"}

      {:error, %Sequin.Error.ValidationError{errors: errors, summary: summary}} ->
        {:error, "Validation error: #{summary}. #{error_messages(errors)}"}

      {:error, %Postgrex.Error{postgres: %{message: message}}} ->
        {:error, "Database error: #{message}"}

      {:error, error} ->
        {:error, "Unknown error: #{inspect(error)}"}
    end
  end

  defp create_db_and_replication(account_id, db, replication_params) do
    Repo.transact(fn ->
      with {:ok, db} <- create_db_for_account(account_id, db),
           replication_params = Map.put(replication_params, :postgres_database_id, db.id),
           {:ok, _} <- Replication.create_pg_replication_for_account_with_lifecycle(account_id, replication_params) do
        :ok
      end
    end)
  end

  defp create_db_for_account(account_id, db) do
    params = Map.from_struct(db)
    Databases.create_db_for_account(account_id, params)
  end

  defp error_messages(changeset_or_errors) when is_map(changeset_or_errors) do
    Enum.map_join(changeset_or_errors, "; ", fn {key, messages} -> "#{key} #{Enum.join(messages, ", ")}" end)
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

      [account | _] ->
        case Databases.list_dbs_for_account(account.id, [:replication_slot]) do
          [] ->
            {:ok, :incomplete, account}

          [db | _] ->
            case db.replication_slot do
              nil -> {:ok, :incomplete, account}
              %PostgresReplicationSlot{} -> {:ok, :complete}
            end
        end
    end
  rescue
    DBConnection.ConnectionError ->
      {:ok, :incomplete, nil}
  end

  defp create_account do
    {:ok, account} = Sequin.Accounts.create_account(%{})
    account
  end
end

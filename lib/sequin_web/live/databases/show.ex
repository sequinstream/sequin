defmodule SequinWeb.DatabasesLive.Show do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Databases
  alias Sequin.Repo

  @impl Phoenix.LiveView
  def mount(%{"id" => id}, _session, socket) do
    case Databases.get_db_for_account(current_account_id(socket), id) do
      {:ok, database} ->
        database = Repo.preload(database, :replication_slot)
        {:ok, assign(socket, database: database, refreshing_tables: false)}

      {:error, _} ->
        {:ok, push_navigate(socket, to: ~p"/databases")}
    end
  end

  @impl Phoenix.LiveView
  def handle_params(params, _url, socket) do
    {:noreply, apply_action(socket, socket.assigns.live_action, params)}
  end

  defp apply_action(socket, :show, _params) do
    assign(socket, :page_title, "Show Database")
  end

  defp apply_action(socket, :edit, _params) do
    assign(socket, :page_title, "Edit Database")
  end

  @impl Phoenix.LiveView
  def handle_event("refresh_tables", _, socket) do
    %{database: database} = socket.assigns

    Task.async(fn -> Databases.update_tables(database) end)

    {:noreply, assign(socket, refreshing_tables: true)}
  end

  def handle_event("delete_database", _, socket) do
    %{database: database} = socket.assigns

    case Databases.delete_db_with_replication_slot(database) do
      {:ok, _} ->
        {:noreply, push_navigate(socket, to: ~p"/databases")}

      {:error, error} ->
        {:reply, %{error: Exception.message(error)}, socket}
    end
  end

  def handle_event("edit", _params, socket) do
    {:noreply, push_navigate(socket, to: ~p"/databases/#{socket.assigns.database.id}/edit")}
  end

  @impl Phoenix.LiveView
  def handle_info({ref, {:ok, updated_db}}, socket) do
    Process.demonitor(ref, [:flush])
    {:noreply, assign(socket, database: updated_db, refreshing_tables: false)}
  end

  def handle_info({:updated_database, updated_database}, socket) do
    {:noreply,
     socket
     |> assign(database: updated_database)
     |> push_patch(to: ~p"/databases/#{updated_database.id}")}
  end

  def handle_info({ref, {:error, _reason}}, socket) do
    Process.demonitor(ref, [:flush])
    {:noreply, assign(socket, refreshing_tables: false)}
  end

  @impl Phoenix.LiveView
  def render(assigns) do
    assigns =
      assign(assigns, :parent, "database-show")

    ~H"""
    <div id={@parent}>
      <%= case @live_action do %>
        <% :edit -> %>
          <.live_component
            module={SequinWeb.Live.Databases.Form}
            id="edit-database"
            database={@database}
            on_finish={&handle_edit_finish/1}
            current_account={@current_account}
          />
        <% :show -> %>
          <.svelte
            name="databases/Show"
            props={%{database: encode_database(@database), parent: @parent}}
          />
      <% end %>
    </div>
    """
  end

  defp handle_edit_finish(updated_database) do
    send(self(), {:updated_database, updated_database})
  end

  defp encode_database(database) do
    %{
      id: database.id,
      name: database.name,
      hostname: database.hostname,
      port: database.port,
      database: database.database,
      username: database.username,
      ssl: database.ssl,
      pool_size: database.pool_size,
      queue_interval: database.queue_interval,
      queue_target: database.queue_target,
      tables: encode_tables(database.tables),
      tables_refreshed_at: database.tables_refreshed_at,
      inserted_at: database.inserted_at,
      updated_at: database.updated_at
    }
  end

  defp encode_tables(tables) do
    Enum.map(tables, fn table ->
      %{
        schema: table.schema,
        name: table.name
      }
    end)
  end
end

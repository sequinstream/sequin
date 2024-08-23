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
    <div id="database-show">
      <.svelte name="databases/Show" props={%{database: encode_database(@database)}} />
    </div>
    """
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

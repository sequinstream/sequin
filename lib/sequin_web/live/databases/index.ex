defmodule SequinWeb.DatabasesLive.Index do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Databases
  alias Sequin.Health

  @impl Phoenix.LiveView
  def mount(_params, _session, socket) do
    account_id = current_account_id(socket)

    databases = Databases.list_dbs_for_account(account_id, [:sequences, :wal_pipelines])

    databases = load_database_health(databases)

    if connected?(socket) do
      Process.send_after(self(), :update_health, 1000)
    end

    {:ok, assign(socket, :databases, databases)}
  end

  @impl Phoenix.LiveView
  def render(assigns) do
    assigns = assign(assigns, :encoded_databases, Enum.map(assigns.databases, &encode_database/1))

    ~H"""
    <div id="databases-index">
      <.svelte
        name="databases/Index"
        props={
          %{
            databases: @encoded_databases
          }
        }
        socket={@socket}
      />
    </div>
    """
  end

  @impl Phoenix.LiveView
  def handle_info(:update_health, socket) do
    Process.send_after(self(), :update_health, 1000)
    {:noreply, assign(socket, :databases, load_database_health(socket.assigns.databases))}
  end

  defp load_database_health(databases) do
    Enum.map(databases, fn database ->
      case Health.get(database) do
        {:ok, health} -> %{database | health: health}
        {:error, _} -> database
      end
    end)
  end

  defp encode_database(database) do
    %{
      id: database.id,
      name: database.name,
      insertedAt: database.inserted_at,
      hostname: database.hostname,
      port: database.port,
      streams: length(database.sequences),
      pipelines: length(database.wal_pipelines),
      health: Health.to_external(database.health)
    }
  end
end

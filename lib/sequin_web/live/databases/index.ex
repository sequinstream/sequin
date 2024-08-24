defmodule SequinWeb.DatabasesLive.Index do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Databases

  @impl Phoenix.LiveView
  def mount(_params, _session, socket) do
    account_id = current_account_id(socket)
    databases = Databases.list_dbs_for_account(account_id)
    encoded_databases = Enum.map(databases, &encode_database/1)

    socket =
      assign(socket, :databases, encoded_databases)

    {:ok, socket}
  end

  @impl Phoenix.LiveView
  def render(assigns) do
    ~H"""
    <div id="databases-index">
      <.svelte
        name="databases/Index"
        props={
          %{
            databases: @databases
          }
        }
        socket={@socket}
      />
    </div>
    """
  end

  defp encode_database(database) do
    %{
      id: database.id,
      name: database.name,
      insertedAt: database.inserted_at,
      hostname: database.hostname,
      port: database.port
    }
  end
end

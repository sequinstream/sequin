defmodule SequinWeb.DatabasesLive.Index do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Databases
  alias Sequin.Error

  @impl Phoenix.LiveView
  def mount(_params, _session, socket) do
    account_id = current_account_id(socket)
    databases = Databases.list_dbs_for_account(account_id)
    encoded_databases = Enum.map(databases, &encode_database/1)

    socket =
      socket
      |> assign(:databases, encoded_databases)
      |> assign(:form_errors, %{})

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
            databases: @databases,
            formErrors: @form_errors
          }
        }
        socket={@socket}
      />
    </div>
    """
  end

  @impl Phoenix.LiveView
  def handle_event("database_clicked", %{"id" => id}, socket) do
    {:noreply, push_navigate(socket, to: ~p"/databases/#{id}")}
  end

  @impl Phoenix.LiveView
  def handle_event("database_submitted", %{"name" => name}, socket) do
    account_id = current_account_id(socket)

    case Databases.create_db_for_account(account_id, %{name: name}) do
      {:ok, database} ->
        encoded_database = encode_database(database)

        socket =
          socket
          |> update(:databases, fn databases -> [encoded_database | databases] end)
          |> assign(:form_errors, %{})

        {:noreply, push_navigate(socket, to: ~p"/databases/#{database.id}")}

      {:error, changeset} ->
        errors = Error.errors_on(changeset)
        {:noreply, assign(socket, :form_errors, errors)}
    end
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

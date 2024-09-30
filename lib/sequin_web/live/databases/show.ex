defmodule SequinWeb.DatabasesLive.Show do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Consumers
  alias Sequin.Databases
  alias Sequin.Health
  alias Sequin.Metrics
  alias Sequin.Repo

  # Add this alias

  @impl Phoenix.LiveView
  def mount(%{"id" => id}, _session, socket) do
    case Databases.get_db_for_account(current_account_id(socket), id) do
      {:ok, database} ->
        database = Repo.preload(database, replication_slot: [:http_pull_consumers, :http_push_consumers])

        # Fetch initial health
        {:ok, health} = Health.get(database)
        database = Map.put(database, :health, health)

        socket = assign(socket, database: database, refreshing_tables: false)
        # Add this line
        socket = assign_metrics(socket)

        if connected?(socket) do
          Process.send_after(self(), :update_health, 1000)
          Process.send_after(self(), :update_metrics, 1000)
        end

        {:ok, socket}

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

    case Databases.update_tables(database) do
      {:ok, updated_database} ->
        {:reply, %{}, assign(socket, database: updated_database)}

      {:error, _reason} ->
        {:reply, %{}, socket}
    end
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
  def handle_info(:update_health, socket) do
    Process.send_after(self(), :update_health, 10_000)

    case Health.get(socket.assigns.database) do
      {:ok, health} ->
        updated_database = Map.put(socket.assigns.database, :health, health)
        {:noreply, assign(socket, database: updated_database)}

      {:error, _} ->
        {:noreply, socket}
    end
  end

  @impl Phoenix.LiveView
  def handle_info(:update_metrics, socket) do
    Process.send_after(self(), :update_metrics, 1000)
    {:noreply, assign_metrics(socket)}
  end

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
            props={%{database: encode_database(@database), parent: @parent, metrics: @metrics}}
          />
      <% end %>
    </div>
    """
  end

  defp assign_metrics(socket) do
    database = socket.assigns.database

    avg_latency =
      case Metrics.get_database_avg_latency(database) do
        {:ok, nil} -> nil
        {:ok, avg_latency} -> round(avg_latency)
        {:error, _} -> nil
      end

    metrics = %{
      avg_latency: avg_latency
    }

    assign(socket, :metrics, metrics)
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
      updated_at: database.updated_at,
      consumers:
        encode_consumers(database.replication_slot.http_pull_consumers, database) ++
          encode_consumers(database.replication_slot.http_push_consumers, database),
      health: Health.to_external(database.health)
    }
  end

  defp encode_consumers(consumers, database) do
    Enum.map(consumers, fn consumer ->
      %{
        id: consumer.id,
        consumer_kind: if(consumer.__struct__ == Sequin.Consumers.HttpPushConsumer, do: :http_push, else: :http_pull),
        name: consumer.name,
        message_kind: consumer.message_kind,
        source_tables: Consumers.enrich_source_tables(consumer.source_tables, database)
      }
    end)
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

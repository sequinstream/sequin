# lib/sequin_web/live/wal_projections/show.ex
defmodule SequinWeb.WalProjectionsLive.Show do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Consumers.SourceTable.ColumnFilter
  alias Sequin.Databases
  alias Sequin.Health
  alias Sequin.Replication
  alias Sequin.Repo

  @impl Phoenix.LiveView
  def mount(%{"id" => id}, _session, socket) do
    account_id = current_account_id(socket)

    case Replication.get_wal_projection_for_account(account_id, id) do
      nil ->
        {:ok, push_navigate(socket, to: ~p"/wal-projections")}

      wal_projection ->
        wal_projection = Repo.preload(wal_projection, [:source_database, :destination_database])

        socket =
          socket
          |> assign(:wal_projection, wal_projection)
          |> assign_health()
          |> assign_metrics()

        if connected?(socket) do
          Process.send_after(self(), :update_health, 1000)
          Process.send_after(self(), :update_metrics, 1500)
        end

        {:ok, socket}
    end
  end

  @impl Phoenix.LiveView
  def handle_event("delete_wal_projection", _, socket) do
    case Replication.delete_wal_projection(socket.assigns.wal_projection) do
      {:ok, _} ->
        {:noreply, push_navigate(socket, to: ~p"/wal-projections")}

      {:error, _} ->
        {:noreply, put_flash(socket, :error, "Failed to delete WAL Projection")}
    end
  end

  @impl Phoenix.LiveView
  def handle_info(:update_health, socket) do
    Process.send_after(self(), :update_health, 10_000)
    {:noreply, assign_health(socket)}
  end

  def handle_info(:update_metrics, socket) do
    Process.send_after(self(), :update_metrics, 10_000)
    {:noreply, assign_metrics(socket)}
  end

  @impl Phoenix.LiveView
  def render(assigns) do
    ~H"""
    <div id="wal-projection-show">
      <.svelte
        name="wal_projections/Show"
        props={
          %{
            walProjection: encode_wal_projection(@wal_projection),
            metrics: encode_metrics(@metrics)
          }
        }
      />
    </div>
    """
  end

  defp assign_health(socket) do
    wal_projection = socket.assigns.wal_projection

    case Health.get(wal_projection) do
      {:ok, health} ->
        assign(socket, :wal_projection, %{wal_projection | health: health})

      {:error, _} ->
        socket
    end
  end

  defp assign_metrics(socket) do
    wal_projection = socket.assigns.wal_projection
    metrics = Replication.wal_events_metrics(wal_projection.id)
    assign(socket, :metrics, metrics)
  end

  defp encode_metrics(metrics) do
    %{
      min: metrics.min,
      max: metrics.max,
      count: metrics.count
    }
  end

  defp encode_wal_projection(wal_projection) do
    {:ok, source_tables} = Databases.tables(wal_projection.source_database)
    {:ok, destination_tables} = Databases.tables(wal_projection.destination_database)

    [source_table] = wal_projection.source_tables
    source_table_info = Enum.find(source_tables, &(&1.oid == source_table.oid))
    destination_table_info = Enum.find(destination_tables, &(&1.oid == wal_projection.destination_oid))

    %{
      id: wal_projection.id,
      name: wal_projection.name,
      status: wal_projection.status,
      source_database: %{
        id: wal_projection.source_database.id,
        name: wal_projection.source_database.name
      },
      destination_database: %{
        id: wal_projection.destination_database.id,
        name: wal_projection.destination_database.name
      },
      source_table: "#{source_table_info.schema}.#{source_table_info.name}",
      source_filters: encode_source_filters(source_table.column_filters),
      destination_table: "#{destination_table_info.schema}.#{destination_table_info.name}",
      inserted_at: wal_projection.inserted_at,
      updated_at: wal_projection.updated_at,
      health: Health.to_external(wal_projection.health)
    }
  end

  defp encode_source_filters(column_filters) do
    Enum.map(column_filters, fn filter ->
      %{
        column: filter.column_name,
        operator: ColumnFilter.to_external_operator(filter.operator),
        value: filter.value.value
      }
    end)
  end
end

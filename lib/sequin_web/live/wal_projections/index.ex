# lib/sequin_web/live/wal_projections/index.ex
defmodule SequinWeb.WalProjectionsLive.Index do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Databases
  alias Sequin.Health
  alias Sequin.Repo

  @impl Phoenix.LiveView
  def mount(_params, _session, socket) do
    account_id = current_account_id(socket)
    databases = Databases.list_dbs_for_account(account_id)

    wal_projections =
      Enum.flat_map(databases, fn database ->
        Repo.preload(database, wal_projections: [:destination_database, :source_database]).wal_projections
      end)

    socket = assign_wal_projections_health(socket, wal_projections)

    if connected?(socket) do
      Process.send_after(self(), :update_health, 1000)
    end

    {:ok, socket}
  end

  @impl Phoenix.LiveView
  def render(assigns) do
    ~H"""
    <div id="wal-projections-index">
      <.svelte
        name="wal_projections/Index"
        props={
          %{
            walProjections: encode_wal_projections(@wal_projections)
          }
        }
      />
    </div>
    """
  end

  @impl Phoenix.LiveView
  def handle_info(:update_health, socket) do
    Process.send_after(self(), :update_health, 10_000)
    {:noreply, assign_wal_projections_health(socket, socket.assigns.wal_projections)}
  end

  defp assign_wal_projections_health(socket, wal_projections) do
    wal_projections_with_health =
      Enum.map(wal_projections, fn projection ->
        case Health.get(projection) do
          {:ok, health} -> %{projection | health: health}
          {:error, _} -> projection
        end
      end)

    assign(socket, :wal_projections, wal_projections_with_health)
  end

  defp encode_wal_projections(wal_projections) do
    Enum.map(wal_projections, fn projection ->
      [%{oid: source_table_oid}] = projection.source_tables
      {:ok, destination_tables} = Databases.tables(projection.destination_database)
      {:ok, source_tables} = Databases.tables(projection.source_database)

      destination_table =
        case Enum.find(destination_tables, &(&1.oid == projection.destination_oid)) do
          nil -> nil
          table -> %{table_name: table.name, schema_name: table.schema}
        end

      source_table =
        case Enum.find(source_tables, &(&1.oid == source_table_oid)) do
          nil -> nil
          table -> %{table_name: table.name, schema_name: table.schema}
        end

      %{
        id: projection.id,
        name: projection.name,
        source_table: source_table,
        destination_table: destination_table,
        inserted_at: projection.inserted_at,
        health: Health.to_external(projection.health)
      }
    end)
  end
end

# lib/sequin_web/live/wal_pipelines/index.ex
defmodule SequinWeb.WalPipelinesLive.Index do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Databases
  alias Sequin.Health
  alias Sequin.Repo

  @impl Phoenix.LiveView
  def mount(_params, _session, socket) do
    account_id = current_account_id(socket)
    databases = Databases.list_dbs_for_account(account_id)

    wal_pipelines =
      Enum.flat_map(databases, fn database ->
        Repo.preload(database, wal_pipelines: [:destination_database, :source_database]).wal_pipelines
      end)

    if connected?(socket) do
      Process.send_after(self(), :update_health, 1000)
    end

    socket =
      socket
      |> assign(wal_pipelines: wal_pipelines, databases: databases)
      |> assign_wal_pipelines_health()
      |> assign(:self_hosted, Application.get_env(:sequin, :self_hosted))

    {:ok, socket}
  end

  @impl Phoenix.LiveView
  def render(assigns) do
    ~H"""
    <div id="wal-pipelines-index">
      <.svelte
        name="wal_pipelines/Index"
        props={
          %{
            walPipelines: encode_wal_pipelines(@wal_pipelines),
            hasDatabases: length(@databases) > 0,
            selfHosted: @self_hosted
          }
        }
      />
    </div>
    """
  end

  @impl Phoenix.LiveView
  def handle_info(:update_health, socket) do
    Process.send_after(self(), :update_health, 10_000)
    {:noreply, assign_wal_pipelines_health(socket)}
  end

  defp assign_wal_pipelines_health(socket) do
    wal_pipelines_with_health =
      Enum.map(socket.assigns.wal_pipelines, fn pipeline ->
        {:ok, health} = Health.health(pipeline)
        %{pipeline | health: health}
      end)

    assign(socket, :wal_pipelines, wal_pipelines_with_health)
  end

  defp encode_wal_pipelines(wal_pipelines) do
    Enum.map(wal_pipelines, fn pipeline ->
      [%{oid: source_table_oid}] = pipeline.source_tables
      {:ok, destination_tables} = Databases.tables(pipeline.destination_database)
      {:ok, source_tables} = Databases.tables(pipeline.source_database)

      destination_table =
        case Enum.find(destination_tables, &(&1.oid == pipeline.destination_oid)) do
          nil -> nil
          table -> %{table_name: table.name, schema_name: table.schema}
        end

      source_table =
        case Enum.find(source_tables, &(&1.oid == source_table_oid)) do
          nil -> nil
          table -> %{table_name: table.name, schema_name: table.schema}
        end

      %{
        id: pipeline.id,
        name: pipeline.name,
        source_table: source_table,
        destination_table: destination_table,
        inserted_at: pipeline.inserted_at,
        health: Health.to_external(pipeline.health)
      }
    end)
  end
end

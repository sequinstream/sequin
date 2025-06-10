# lib/sequin_web/live/wal_pipelines/show.ex
defmodule SequinWeb.WalPipelinesLive.Show do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Consumers.SequenceFilter.ColumnFilter
  alias Sequin.Consumers.SourceTable
  alias Sequin.Databases
  alias Sequin.Databases.PostgresDatabaseTable
  alias Sequin.Health
  alias Sequin.Postgres
  alias Sequin.Replication
  alias Sequin.Repo

  @impl Phoenix.LiveView
  def mount(%{"id" => id}, _session, socket) do
    account_id = current_account_id(socket)

    case Replication.get_wal_pipeline_for_account(account_id, id) do
      nil ->
        {:ok, push_navigate(socket, to: ~p"/change-capture-pipelines")}

      wal_pipeline ->
        wal_pipeline = Repo.preload(wal_pipeline, [:source_database, :destination_database])

        socket =
          socket
          |> assign(:wal_pipeline, wal_pipeline)
          |> assign_health()
          |> assign_metrics()
          |> assign_replica_identity()

        if connected?(socket) do
          Process.send_after(self(), :update_health, 1000)
          Process.send_after(self(), :update_metrics, 1500)
        end

        {:ok, socket}
    end
  end

  @impl Phoenix.LiveView
  def handle_event("delete_wal_pipeline", _, socket) do
    case Replication.delete_wal_pipeline_with_lifecycle(socket.assigns.wal_pipeline) do
      {:ok, _} ->
        {:noreply, push_navigate(socket, to: ~p"/change-capture-pipelines")}

      {:error, _} ->
        {:noreply, put_flash(socket, :error, "Failed to delete WAL Pipeline")}
    end
  end

  @impl Phoenix.LiveView
  def handle_event("refresh_replica_warning", _params, socket) do
    {:reply, %{ok: true}, assign_replica_identity(socket)}
  end

  @impl Phoenix.LiveView
  def handle_event("dismiss_replica_warning", _params, socket) do
    wal_pipeline = socket.assigns.wal_pipeline
    new_annotations = Map.put(wal_pipeline.annotations, "replica_warning_dismissed", true)

    case Replication.update_wal_pipeline(wal_pipeline, %{annotations: new_annotations}) do
      {:ok, updated_wal_pipeline} ->
        {:noreply, assign(socket, :wal_pipeline, updated_wal_pipeline)}

      {:error, _changeset} ->
        {:noreply, put_flash(socket, :error, "Failed to dismiss warning. Please try again.")}
    end
  end

  @impl Phoenix.LiveView
  def handle_event("refresh_health", _params, socket) do
    {:noreply, assign_health(socket)}
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
    replica_warning_dismissed = assigns.wal_pipeline.annotations["replica_warning_dismissed"] || false
    # Show when: we successfully loaded the replica identity, it's not full, and it's not dismissed
    show_replica_warning =
      assigns.replica_identity.ok? and assigns.replica_identity.result != :full and not replica_warning_dismissed

    assigns = assign(assigns, :show_replica_warning, show_replica_warning)
    assigns = assign(assigns, :parent, "change-capture-pipeline-show")

    ~H"""
    <div id={@parent}>
      <.svelte
        name="wal_pipelines/Show"
        props={
          %{
            parent: @parent,
            walPipeline: encode_wal_pipeline(@wal_pipeline),
            metrics: encode_metrics(@metrics),
            showReplicaWarning: @show_replica_warning
          }
        }
      />
    </div>
    """
  end

  defp assign_health(socket) do
    wal_pipeline = socket.assigns.wal_pipeline

    case Health.health(wal_pipeline) do
      {:ok, health} ->
        assign(socket, :wal_pipeline, %{wal_pipeline | health: health})

      {:error, _} ->
        socket
    end
  end

  defp assign_metrics(socket) do
    wal_pipeline = socket.assigns.wal_pipeline
    metrics = Replication.wal_events_metrics(wal_pipeline.id)
    assign(socket, :metrics, metrics)
  end

  defp encode_metrics(metrics) do
    %{
      min: metrics.min,
      max: metrics.max,
      count: metrics.count
    }
  end

  defp encode_wal_pipeline(wal_pipeline) do
    {:ok, source_tables} = Databases.tables(wal_pipeline.source_database)
    {:ok, destination_tables} = Databases.tables(wal_pipeline.destination_database)

    [source_table] = wal_pipeline.source_tables
    source_table_info = Enum.find(source_tables, &(&1.oid == source_table.oid))
    destination_table_info = Enum.find(destination_tables, &(&1.oid == wal_pipeline.destination_oid))

    %{
      id: wal_pipeline.id,
      name: wal_pipeline.name,
      status: wal_pipeline.status,
      source_database: %{
        id: wal_pipeline.source_database.id,
        name: wal_pipeline.source_database.name
      },
      destination_database: %{
        id: wal_pipeline.destination_database.id,
        name: wal_pipeline.destination_database.name
      },
      source_table: encode_source_table(source_table, source_table_info),
      source_filters: encode_source_filters(source_table.column_filters),
      destination_table: "#{destination_table_info.schema}.#{destination_table_info.name}",
      inserted_at: wal_pipeline.inserted_at,
      updated_at: wal_pipeline.updated_at,
      health: Health.to_external(wal_pipeline.health)
    }
  end

  defp encode_source_table(%SourceTable{}, nil) do
    nil
  end

  defp encode_source_table(%SourceTable{} = table, %PostgresDatabaseTable{} = source_table_info) do
    %{
      oid: table.oid,
      name: "#{source_table_info.schema}.#{source_table_info.name}",
      quoted_name: Postgres.quote_name(source_table_info.schema, source_table_info.name)
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

  defp assign_replica_identity(socket) do
    wal_pipeline = socket.assigns.wal_pipeline
    [source_table] = wal_pipeline.source_tables
    table_oid = source_table.oid

    assign_async(socket, :replica_identity, fn ->
      case Databases.check_replica_identity(wal_pipeline.source_database, table_oid) do
        {:ok, replica_identity} ->
          {:ok, %{replica_identity: replica_identity}}

        {:error, _} ->
          {:ok, %{replica_identity: nil}}
      end
    end)
  end
end

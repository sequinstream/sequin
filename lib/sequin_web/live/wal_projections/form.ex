defmodule SequinWeb.WalProjectionsLive.Form do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Consumers.SourceTable.ColumnFilter
  alias Sequin.Databases
  alias Sequin.Databases.PostgresDatabase.Table
  alias Sequin.Error
  alias Sequin.Name
  alias Sequin.Postgres
  alias Sequin.Posthog
  alias Sequin.Replication
  alias Sequin.Replication.WalProjection

  require Logger

  @impl Phoenix.LiveView
  def mount(params, _session, socket) do
    wal_projection_id = Map.get(params, "id")
    is_edit = not is_nil(wal_projection_id)
    socket = assign_databases(socket)

    case fetch_or_build_wal_projection(socket, wal_projection_id) do
      {:ok, wal_projection} ->
        {:ok,
         socket
         |> assign(wal_projection: wal_projection)
         |> assign(is_edit: is_edit)
         |> assign(show_errors?: false)
         |> assign(submit_error: nil)
         |> assign_changeset(%{})}

      {:error, _reason} ->
        {:ok, push_navigate(socket, to: ~p"/wal-projections")}
    end
  end

  @impl Phoenix.LiveView
  def render(assigns) do
    encoded_errors =
      if assigns.show_errors? do
        encode_errors(assigns.changeset)
      else
        %{}
      end

    assigns =
      assign(assigns, :encoded_errors, encoded_errors)

    dbg(assigns.databases)

    ~H"""
    <div id="wal-projection-form">
      <.svelte
        name="wal_projections/Form"
        props={
          %{
            walProjection: encode_wal_projection(assigns),
            databases: Enum.map(assigns.databases, &encode_database/1),
            errors: @encoded_errors,
            parent: "wal-projection-form",
            isEdit: @is_edit
          }
        }
      />
    </div>
    """
  end

  @impl Phoenix.LiveView
  def handle_event("validate", %{"wal_projection" => params}, socket) do
    params =
      params
      |> decode_params()
      |> maybe_put_replication_slot_id(socket)

    {:noreply, assign_changeset(socket, params)}
  end

  @impl Phoenix.LiveView
  def handle_event("save", %{"wal_projection" => params}, socket) do
    params =
      params
      |> decode_params()
      |> maybe_put_replication_slot_id(socket)

    socket = assign_changeset(socket, params)

    if socket.assigns.changeset.valid? do
      case create_or_update_wal_projection(socket, socket.assigns.wal_projection, params) do
        {:ok, wal_projection} ->
          {:noreply,
           socket
           |> put_flash(:toast, %{kind: :info, title: "WAL Projection saved successfully"})
           |> push_navigate(to: ~p"/wal-projections/#{wal_projection.id}")}

        {:error, %Ecto.Changeset{} = changeset} ->
          {:noreply, assign(socket, changeset: changeset, show_errors?: true)}

          # {:error, error} ->
          #   {:noreply, assign(socket, submit_error: error_msg(error), show_errors: true)}
      end
    else
      {:noreply, assign(socket, show_errors?: true)}
    end
  end

  @impl Phoenix.LiveView
  def handle_event("refresh_databases", _params, socket) do
    {:noreply, assign_databases(socket)}
  end

  def handle_event("refresh_tables", %{"database_id" => database_id}, socket) do
    with index when not is_nil(index) <- Enum.find_index(socket.assigns.databases, &(&1.id == database_id)),
         database = Enum.at(socket.assigns.databases, index),
         {:ok, updated_database} <- Databases.update_tables(database) do
      updated_databases = List.replace_at(socket.assigns.databases, index, updated_database)
      {:noreply, assign(socket, databases: updated_databases)}
    else
      _ ->
        {:noreply, put_flash(socket, :toast, %{kind: :error, title: "Failed to refresh tables"})}
    end
  end

  @impl Phoenix.LiveView
  def handle_event("form_closed", _params, socket) do
    socket =
      if socket.assigns.is_edit? do
        push_navigate(socket, to: ~p"/wal-projections/#{socket.assigns.wal_projection.id}")
      else
        push_navigate(socket, to: ~p"/wal-projections")
      end

    {:noreply, socket}
  end

  defp assign_databases(socket) do
    databases = Databases.list_dbs_for_account(current_account_id(socket), [:replication_slot])
    assign(socket, databases: databases)
  end

  defp fetch_or_build_wal_projection(%{assigns: %{databases: []}} = socket, nil) do
    {:ok, %WalProjection{account_id: current_account_id(socket)}}
  end

  defp fetch_or_build_wal_projection(%{assigns: %{databases: [db | _rest]}} = socket, nil) do
    {:ok, %WalProjection{account_id: current_account_id(socket), replication_slot: db.replication_slot}}
  end

  defp fetch_or_build_wal_projection(socket, wal_projection_id) do
    Replication.get_wal_projection_for_account(current_account_id(socket), wal_projection_id, [
      :replication_slot,
      :source_database
    ])
  end

  defp create_or_update_wal_projection(socket, wal_projection, params) do
    account_id = current_account_id(socket)

    case_result =
      if wal_projection.id do
        Replication.update_wal_projection_with_lifecycle(wal_projection, params)
      else
        Replication.create_wal_projection_with_lifecycle(account_id, params)
      end

    case case_result do
      {:ok, updated_wal_projection} ->
        action = if wal_projection.id, do: "Updated", else: "Created"

        Posthog.capture("WAL Projection #{action}", %{
          distinct_id: current_user_id(socket),
          properties: %{
            wal_projection_id: updated_wal_projection.id,
            wal_projection_name: updated_wal_projection.name,
            "$groups": %{account: updated_wal_projection.account_id}
          }
        })

        {:ok, updated_wal_projection}

      {:error, %Ecto.Changeset{} = changeset} ->
        {:error, changeset}

      {:error, error} ->
        Logger.error("Error creating/updating WAL Projection: #{inspect(error)}")
        {:error, "An unexpected error occurred. Please try again."}
    end
  end

  defp decode_params(form) do
    source_table_actions = form["sourceTableActions"] || []

    %{
      "name" => form["name"],
      "status" => form["status"],
      "source_database_id" => form["sourceDatabaseId"],
      "destination_database_id" => form["destinationDatabaseId"],
      "destination_oid" => form["destinationTableOid"],
      "source_tables" => [
        %{
          "oid" => form["sourceTableOid"],
          "column_filters" => Enum.map(form["sourceTableFilters"], &ColumnFilter.from_external/1),
          "actions" => source_table_actions
        }
      ]
    }
  end

  defp maybe_put_replication_slot_id(%{"source_database_id" => nil} = params, _socket) do
    params
  end

  defp maybe_put_replication_slot_id(%{"source_database_id" => source_database_id} = params, socket) do
    case Enum.find(socket.assigns.databases, &(&1.id == source_database_id)) do
      %_{replication_slot: replication_slot} ->
        Map.put(params, "replication_slot_id", replication_slot.id)

      _ ->
        Map.merge(params, %{"replication_slot_id" => nil, "source_database_id" => nil})
    end
  end

  defp encode_wal_projection(assigns) do
    wal_projection = assigns.wal_projection
    source_table = List.first(wal_projection.source_tables || [])
    replication_slot = Ecto.Changeset.get_field(assigns.changeset, :replication_slot)

    %{
      "id" => wal_projection.id,
      "name" => wal_projection.name || Name.generate(999),
      "status" => wal_projection.status,
      "sourceDatabaseId" => replication_slot && replication_slot.postgres_database_id,
      "sourceTableOid" => source_table && source_table.oid,
      "destinationDatabaseId" => wal_projection.destination_database_id,
      "destinationTableOid" => wal_projection.destination_oid,
      "sourceTableActions" => (source_table && source_table.actions) || [:insert, :update, :delete],
      "sourceTableFilters" => source_table && Enum.map(source_table.column_filters, &ColumnFilter.to_external/1),
      "retentionDays" => 30
    }
  end

  defp encode_database(database) do
    %{
      "id" => database.id,
      "name" => database.name,
      "tables" =>
        Enum.map(database.tables, fn %Table{} = table ->
          %{
            "oid" => table.oid,
            "schema" => table.schema,
            "name" => table.name,
            "columns" =>
              Enum.map(table.columns, fn %Table.Column{} = column ->
                %{
                  "attnum" => column.attnum,
                  "isPk?" => column.is_pk?,
                  "name" => column.name,
                  "type" => column.type,
                  "filterType" => Postgres.pg_simple_type_to_filter_type(column.type)
                }
              end)
          }
        end)
    }
  end

  defp encode_errors(%Ecto.Changeset{} = changeset) do
    Error.errors_on(changeset)
  end

  defp assign_changeset(socket, params) do
    wal_projection = socket.assigns.wal_projection

    changeset =
      if socket.assigns.is_edit do
        WalProjection.update_changeset(wal_projection, params)
      else
        WalProjection.create_changeset(wal_projection, params)
      end

    assign(socket, :changeset, changeset)
  end

  # defp error_msg(error) do
  #   case error do
  #     {:error, error} -> error_msg(error)
  #     %Ecto.Changeset{} -> "Invalid params. Please check the errors below."
  #     error when is_binary(error) -> error
  #     _ -> "An unexpected error occurred. Please try again."
  #   end
  # end
end

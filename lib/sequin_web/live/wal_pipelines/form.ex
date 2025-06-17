defmodule SequinWeb.WalPipelinesLive.Form do
  @moduledoc false

  use SequinWeb, :live_view

  alias Sequin.Databases
  alias Sequin.Databases.PostgresDatabaseTable
  alias Sequin.Error
  alias Sequin.Error.InvariantError
  alias Sequin.Error.NotFoundError
  alias Sequin.Name
  alias Sequin.Postgres
  alias Sequin.Posthog
  alias Sequin.Replication
  alias Sequin.Replication.WalPipeline
  alias Sequin.WalPipeline.SourceTable.ColumnFilter

  require Logger

  @impl Phoenix.LiveView
  def mount(params, _session, socket) do
    wal_pipeline_id = Map.get(params, "id")
    is_edit = not is_nil(wal_pipeline_id)
    socket = assign_databases(socket)

    case fetch_or_build_wal_pipeline(socket, wal_pipeline_id) do
      {:ok, %WalPipeline{} = wal_pipeline} ->
        socket =
          socket
          |> assign(wal_pipeline: wal_pipeline)
          |> assign(is_edit: is_edit)
          |> assign(show_errors?: false)
          |> assign(submit_error: nil)
          |> assign_changeset(%{})

        {:ok, socket, layout: {SequinWeb.Layouts, :app_no_sidenav}}

      {:ok, nil} ->
        {:ok, push_navigate(socket, to: ~p"/change-capture-pipelines"), layout: {SequinWeb.Layouts, :app_no_sidenav}}
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

    assigns = assign(assigns, :encoded_errors, encoded_errors)

    ~H"""
    <div id="wal-pipeline-form">
      <.svelte
        name="wal_pipelines/Form"
        props={
          %{
            walPipeline: encode_wal_pipeline(assigns),
            databases: Enum.map(assigns.databases, &encode_database/1),
            errors: @encoded_errors,
            parent: "wal-pipeline-form",
            isEdit: @is_edit,
            submitError: @submit_error
          }
        }
      />
    </div>
    """
  end

  @impl Phoenix.LiveView
  def handle_event("validate", %{"wal_pipeline" => params}, socket) do
    params =
      params
      |> decode_params()
      |> maybe_put_replication_slot_id(socket)

    {:noreply, assign_changeset(socket, params)}
  end

  @impl Phoenix.LiveView
  def handle_event("save", %{"wal_pipeline" => params}, socket) do
    params =
      params
      |> decode_params()
      |> maybe_put_replication_slot_id(socket)

    source_table_oid = List.first(params["source_tables"])["oid"]

    socket = assign_changeset(socket, params)

    if socket.assigns.changeset.valid? do
      database = Sequin.Enum.find!(socket.assigns.databases, &(&1.id == params["source_database_id"]))
      table = Sequin.Enum.find!(database.tables, &(&1.oid == source_table_oid))
      toast_title = if socket.assigns.is_edit, do: "WAL Pipeline updated", else: "WAL Pipeline created"

      with :ok <- Databases.verify_table_in_publication(database, source_table_oid),
           :ok <- verify_source_not_event_table(table),
           {:ok, wal_pipeline} <- create_or_update_wal_pipeline(socket, socket.assigns.wal_pipeline, params) do
        {:noreply,
         socket
         |> put_flash(:toast, %{kind: :info, title: toast_title})
         |> push_navigate(to: ~p"/change-capture-pipelines/#{wal_pipeline.id}")}
      else
        {:error, %Ecto.Changeset{} = changeset} ->
          {:noreply, assign(socket, changeset: changeset, show_errors?: true)}

        {:error, %InvariantError{} = error} ->
          {:noreply, assign(socket, submit_error: Exception.message(error))}

        {:error, %NotFoundError{entity: :publication_membership}} ->
          submit_error =
            "Selected source table is not in your publication, so Sequin won't receive changes from it. Please add it with `alter publication #{database.replication_slot.publication_name} add table {table_name}`"

          {:noreply, assign(socket, submit_error: submit_error)}
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
      if socket.assigns.is_edit do
        push_navigate(socket, to: ~p"/change-capture-pipelines/#{socket.assigns.wal_pipeline.id}")
      else
        push_navigate(socket, to: ~p"/change-capture-pipelines")
      end

    {:noreply, socket}
  end

  defp verify_source_not_event_table(%PostgresDatabaseTable{} = table) do
    if Postgres.event_table?(table) do
      {:error, Error.invariant(message: "Source table cannot be an event table.")}
    else
      :ok
    end
  end

  defp assign_databases(socket) do
    databases = Databases.list_dbs_for_account(current_account_id(socket), [:replication_slot])
    assign(socket, databases: databases)
  end

  defp fetch_or_build_wal_pipeline(%{assigns: %{databases: []}} = socket, nil) do
    {:ok, %WalPipeline{account_id: current_account_id(socket)}}
  end

  defp fetch_or_build_wal_pipeline(%{assigns: %{databases: [db | _rest]}} = socket, nil) do
    {:ok,
     %WalPipeline{
       account_id: current_account_id(socket),
       replication_slot: db.replication_slot,
       destination_database_id: db.id
     }}
  end

  defp fetch_or_build_wal_pipeline(socket, wal_pipeline_id) do
    {:ok,
     Replication.get_wal_pipeline_for_account(current_account_id(socket), wal_pipeline_id, [
       :replication_slot,
       :source_database
     ])}
  end

  defp create_or_update_wal_pipeline(socket, wal_pipeline, params) do
    account_id = current_account_id(socket)

    case_result =
      if wal_pipeline.id do
        Replication.update_wal_pipeline_with_lifecycle(wal_pipeline, params)
      else
        Replication.create_wal_pipeline_with_lifecycle(account_id, params)
      end

    case case_result do
      {:ok, updated_wal_pipeline} ->
        action = if wal_pipeline.id, do: "Updated", else: "Created"

        Posthog.capture("WAL Pipeline #{action}", %{
          distinct_id: current_user_id(socket),
          properties: %{
            wal_pipeline_id: updated_wal_pipeline.id,
            wal_pipeline_name: updated_wal_pipeline.name,
            "$groups": %{account: updated_wal_pipeline.account_id}
          }
        })

        {:ok, updated_wal_pipeline}

      {:error, %Ecto.Changeset{} = changeset} ->
        {:error, changeset}

      {:error, error} ->
        Logger.error("Error creating/updating WAL Pipeline: #{inspect(error)}")
        {:error, "An unexpected error occurred. Please try again."}
    end
  end

  defp decode_params(form) do
    source_table_actions = form["sourceTableActions"] || []

    %{
      "name" => form["name"],
      "status" => form["status"],
      "source_database_id" => form["postgresDatabaseId"],
      "destination_database_id" => form["destinationDatabaseId"],
      "destination_oid" => form["destinationTableOid"],
      "source_tables" => [
        %{
          "oid" => form["tableOid"],
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

  defp encode_wal_pipeline(assigns) do
    wal_pipeline = assigns.wal_pipeline
    source_table = List.first(wal_pipeline.source_tables || [])
    replication_slot = Ecto.Changeset.get_field(assigns.changeset, :replication_slot)

    %{
      "id" => wal_pipeline.id,
      "name" => wal_pipeline.name || Name.generate(999),
      "status" => wal_pipeline.status,
      # Called tableOid and postgresDatabaseId in the form to match expectations of existing components
      "postgresDatabaseId" => replication_slot && replication_slot.postgres_database_id,
      "tableOid" => source_table && source_table.oid,
      "destinationDatabaseId" => wal_pipeline.destination_database_id,
      "destinationTableOid" => wal_pipeline.destination_oid,
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
        database.tables
        |> Databases.reject_sequin_internal_tables()
        |> Enum.map(fn %PostgresDatabaseTable{} = table ->
          %{
            "oid" => table.oid,
            "schema" => table.schema,
            "name" => table.name,
            "isEventTable" => Postgres.event_table?(table),
            "eventTableErrors" => if(Postgres.event_table?(table), do: Postgres.event_table_errors(table)),
            "columns" =>
              Enum.map(table.columns, fn %PostgresDatabaseTable.Column{} = column ->
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
    wal_pipeline = socket.assigns.wal_pipeline

    changeset =
      if socket.assigns.is_edit do
        WalPipeline.update_changeset(wal_pipeline, params)
      else
        WalPipeline.create_changeset(wal_pipeline, params)
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

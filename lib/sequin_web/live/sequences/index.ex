defmodule SequinWeb.SequencesLive.Index do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Consumers
  alias Sequin.Databases
  alias Sequin.Repo

  @impl Phoenix.LiveView
  def mount(_params, _session, socket) do
    account_id = current_account_id(socket)

    sequence_frequencies =
      account_id
      |> Consumers.list_consumers_for_account()
      |> Enum.frequencies_by(& &1.sequence_id)

    sequences =
      account_id
      |> Databases.list_sequences_for_account()
      |> Repo.preload(:postgres_database)

    databases = Databases.list_dbs_for_account(account_id)

    {:ok,
     assign(
       socket,
       sequences: sequences,
       sequence_frequencies: sequence_frequencies,
       databases: databases,
       changeset: Databases.Sequence.changeset(%Databases.Sequence{}, %{})
     )}
  end

  @impl Phoenix.LiveView
  def handle_params(params, _url, socket) do
    {:noreply, apply_action(socket, socket.assigns.live_action, params)}
  end

  defp apply_action(socket, :new, _params) do
    socket
    |> assign(:page_title, "New Sequence")
    |> assign(:sequence, %Databases.Sequence{})
  end

  defp apply_action(socket, :index, _params) do
    socket
    |> assign(:page_title, "Sequences")
    |> assign(:sequence, nil)
  end

  @impl Phoenix.LiveView
  def handle_event("form_updated", %{"form" => sequence_params}, socket) do
    changeset =
      %Databases.Sequence{}
      |> Databases.Sequence.changeset(sequence_params)
      |> Map.put(:action, :validate)

    {:noreply, assign(socket, :changeset, changeset)}
  end

  def handle_event("form_closed", _, socket) do
    {:noreply,
     socket
     |> assign(:changeset, Databases.Sequence.changeset(%Databases.Sequence{}, %{}))
     |> push_navigate(to: "/sequences")}
  end

  def handle_event("form_submitted", %{"form" => %{"postgres_database_id" => nil} = params}, socket) do
    changeset =
      %Databases.Sequence{}
      |> Databases.Sequence.changeset(params)
      |> Map.put(:action, :insert)

    {:noreply, assign(socket, :changeset, changeset)}
  end

  def handle_event("form_submitted", %{"form" => sequence_params}, socket) do
    account_id = current_account_id(socket)

    # It's okay if this raises on 404, because the form doesn't let you select a database that doesn't exist
    {:ok, database} = Databases.get_db_for_account(account_id, sequence_params["postgres_database_id"])

    # Update sequence_params with the required fields
    # Use placeholder values for table and column names
    sequence_params = Map.put(sequence_params, "postgres_database_id", database.id)

    case Databases.create_sequence(sequence_params) do
      {:ok, sequence} ->
        # This will populate the sequence with the correct table and column names
        Databases.update_sequence_from_db(sequence, database)

        {:noreply,
         socket
         |> put_flash(:toast, %{kind: :success, title: "Sequence created successfully"})
         |> push_navigate(to: "/sequences")}

      {:error, %Ecto.Changeset{} = changeset} ->
        {:noreply, assign(socket, changeset: changeset)}
    end
  end

  @impl Phoenix.LiveView
  def handle_event("delete_sequence", %{"id" => id}, socket) do
    account_id = current_account_id(socket)

    case Databases.get_sequence_for_account(account_id, id) do
      {:ok, sequence} ->
        case Databases.delete_sequence(sequence) do
          {:ok, _} ->
            {:noreply,
             socket
             |> put_flash(:toast, %{kind: :success, title: "Sequence deleted successfully"})
             |> assign(:sequences, list_sequences(account_id))}

          {:error, _} ->
            {:noreply, put_flash(socket, :toast, %{kind: :error, title: "Failed to delete sequence"})}
        end

      {:error, _} ->
        {:noreply, put_flash(socket, :toast, %{kind: :error, title: "Sequence not found"})}
    end
  end

  @impl Phoenix.LiveView
  def render(assigns) do
    assigns =
      assign(assigns, :encoded_sequences, Enum.map(assigns.sequences, &encode_sequence(&1, assigns.sequence_frequencies)))

    assigns = assign(assigns, :encoded_databases, Enum.map(assigns.databases, &encode_database/1))
    assigns = assign(assigns, :parent, "sequences-index")

    ~H"""
    <div id={@parent}>
      <.svelte
        name="sequences/Index"
        props={
          %{
            parent: @parent,
            sequences: @encoded_sequences,
            databases: @encoded_databases,
            live_action: @live_action
          }
        }
        socket={@socket}
      />
    </div>
    """
  end

  defp encode_sequence(sequence, sequence_frequencies) do
    %{
      id: sequence.id,
      table_name: sequence.table_name,
      table_schema: sequence.table_schema,
      sort_column_name: sequence.sort_column_name,
      inserted_at: sequence.inserted_at,
      consumer_count: Map.get(sequence_frequencies, sequence.id, 0),
      postgres_database: %{
        id: sequence.postgres_database.id,
        name: sequence.postgres_database.name
      }
    }
  end

  defp encode_database(database) do
    %{
      id: database.id,
      name: database.name,
      tables: Enum.map(database.tables, &encode_table/1)
    }
  end

  defp encode_table(table) do
    %{
      oid: table.oid,
      schema: table.schema,
      name: table.name,
      columns: Enum.map(table.columns, &encode_column/1)
    }
  end

  defp encode_column(column) do
    %{
      attnum: column.attnum,
      name: column.name,
      type: column.type
    }
  end

  defp list_sequences(account_id) do
    account_id
    |> Databases.list_sequences_for_account()
    |> Repo.preload(:postgres_database)
  end
end

defmodule SequinWeb.DatabasesLive.New do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Databases
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Replication
  alias Sequin.Replication.PostgresReplicationSlot

  @impl Phoenix.LiveView
  def mount(_params, _session, socket) do
    changeset = Databases.PostgresDatabase.changeset(%PostgresDatabase{}, %{})

    replication_changeset =
      Replication.PostgresReplicationSlot.create_changeset(%PostgresReplicationSlot{}, %{
        slot_name: "sequin_slot",
        publication_name: "sequin_pub"
      })

    socket =
      socket
      |> assign(:changeset, changeset)
      |> assign(:replication_changeset, replication_changeset)
      |> assign(:form_data, changeset_to_form_data(changeset, replication_changeset))
      |> assign(:form_errors, %{})
      |> assign(:validating, false)
      |> assign(:show_errors?, false)

    {:ok, socket}
  end

  @impl Phoenix.LiveView
  def render(assigns) do
    ~H"""
    <div id="new-database" class="w-full">
      <.svelte
        name="databases/New"
        props={
          %{
            formData: @form_data,
            formErrors: if(@show_errors?, do: @form_errors, else: %{}),
            validating: @validating,
            parent: "new-database"
          }
        }
        socket={@socket}
      />
    </div>
    """
  end

  @impl Phoenix.LiveView
  def handle_event(
        "validate",
        %{"postgres_database" => db_params, "postgres_replication_slot" => replication_params},
        socket
      ) do
    changeset =
      %PostgresDatabase{}
      |> PostgresDatabase.changeset(db_params)
      |> Map.put(:action, :validate)

    replication_changeset =
      %PostgresReplicationSlot{}
      |> PostgresReplicationSlot.create_changeset(replication_params)
      |> Map.put(:action, :validate)

    form_data = changeset_to_form_data(changeset, replication_changeset)

    form_errors = %{
      database: errors_on(changeset),
      replication: errors_on(replication_changeset)
    }

    {:noreply,
     socket
     |> assign(:changeset, changeset)
     |> assign(:replication_changeset, replication_changeset)
     |> assign(:form_data, form_data)
     |> assign(:form_errors, form_errors)}
  end

  @impl Phoenix.LiveView
  def handle_event("save", %{"postgres_database" => db_params, "postgres_replication_slot" => replication_params}, socket) do
    account_id = current_account_id(socket)
    db_params = Map.put(db_params, "account_id", account_id)

    socket =
      socket
      |> assign(:validating, true)
      |> assign(:show_errors?, true)
      |> push_event("validating", %{})

    # Perform async validation
    Task.async(fn -> validate_and_create(account_id, db_params, replication_params) end)

    {:noreply, socket}
  end

  @impl Phoenix.LiveView
  def handle_event("form_closed", _params, socket) do
    {:noreply, push_navigate(socket, to: ~p"/databases")}
  end

  @impl Phoenix.LiveView
  def handle_info({ref, result}, socket) when is_reference(ref) do
    Process.demonitor(ref, [:flush])
    handle_validation_result(result, socket)
  end

  defp validate_and_create(account_id, db_params, replication_params) do
    # Create a temporary struct for validation
    temp_db = %PostgresDatabase{
      account_id: account_id,
      database: db_params["database"],
      hostname: db_params["hostname"],
      port: db_params["port"],
      username: db_params["username"],
      password: db_params["password"],
      ssl: db_params["ssl"]
    }

    replication_params = %{
      publication_name: replication_params["publication_name"],
      slot_name: replication_params["slot_name"]
    }

    with :ok <- Databases.test_tcp_reachability(temp_db),
         :ok <- Databases.test_connect(temp_db),
         :ok <- Replication.validate_replication_config(temp_db, replication_params),
         {:ok, db} <- Databases.create_db_for_account(account_id, db_params),
         {:ok, db} <- Databases.update_tables(db),
         {:ok, replication} <-
           Replication.create_pg_replication_for_account_with_lifecycle(
             account_id,
             Map.put(replication_params, "postgres_database_id", db.id)
           ) do
      {:ok, %{database: db, replication: replication}}
    else
      {:error, %Ecto.Changeset{} = changeset} ->
        {:error, changeset}

      {:error, %Sequin.Error.ValidationError{errors: errors, summary: summary}} ->
        {:error, :validation_error, errors, summary}

      {:error, %Postgrex.Error{message: "ssl not available"}} ->
        {:error, :ssl_not_available}

      {:error, %Postgrex.Error{postgres: %{message: message}}} ->
        {:error, :database_error, message}

      {:error, error} ->
        {:error, :unknown_error, inspect(error)}
    end
  end

  defp handle_validation_result({:ok, %{database: database, replication: _replication}}, socket) do
    {:noreply,
     socket
     |> assign(:validating, false)
     |> put_flash(:info, "Database created successfully")
     |> push_navigate(to: ~p"/databases/#{database.id}")}
  end

  defp handle_validation_result({:error, %Ecto.Changeset{} = changeset}, socket) do
    replication_changeset = socket.assigns.replication_changeset
    form_data = changeset_to_form_data(changeset, replication_changeset)

    form_errors = %{
      database: errors_on(changeset),
      replication: errors_on(replication_changeset)
    }

    {:noreply,
     socket
     |> assign(:validating, false)
     |> assign(:changeset, changeset)
     |> assign(:form_data, form_data)
     |> assign(:form_errors, form_errors)
     |> assign(:show_errors?, true)
     |> push_event("validation_complete", %{})}
  end

  defp handle_validation_result({:error, :validation_error, errors, summary}, socket) do
    {:noreply,
     socket
     |> assign(:validating, false)
     |> assign(:form_errors, errors)
     |> put_flash(:error, summary || "Validation failed")
     |> push_event("validation_complete", %{})}
  end

  defp handle_validation_result({:error, :database_error, message}, socket) do
    {:noreply,
     socket
     |> assign(:validating, false)
     |> put_flash(:error, "Database error: #{message}")
     |> push_event("validation_complete", %{})}
  end

  defp handle_validation_result({:error, :ssl_not_available}, socket) do
    {:noreply,
     socket
     |> assign(:validating, false)
     |> put_flash(
       :error,
       "SSL connection is not available. Please check your database configuration and ensure SSL is properly set up."
     )
     |> push_event("validation_complete", %{})}
  end

  defp handle_validation_result({:error, :unknown_error, error_message}, socket) do
    {:noreply,
     socket
     |> assign(:validating, false)
     |> put_flash(:error, "An unexpected error occurred: #{error_message}")
     |> push_event("validation_complete", %{})}
  end

  defp changeset_to_form_data(changeset, replication_changeset) do
    %{
      database: %{
        database: Ecto.Changeset.get_field(changeset, :database) || "",
        hostname: Ecto.Changeset.get_field(changeset, :hostname) || "",
        port: Ecto.Changeset.get_field(changeset, :port) || 5432,
        name: Ecto.Changeset.get_field(changeset, :name) || "",
        ssl: Ecto.Changeset.get_field(changeset, :ssl) || false,
        username: Ecto.Changeset.get_field(changeset, :username) || "",
        password: Ecto.Changeset.get_field(changeset, :password) || ""
      },
      replication: %{
        publication_name: Ecto.Changeset.get_field(replication_changeset, :publication_name) || "",
        slot_name: Ecto.Changeset.get_field(replication_changeset, :slot_name) || ""
      }
    }
  end

  defp errors_on(changeset) do
    Ecto.Changeset.traverse_errors(changeset, fn {message, opts} ->
      Regex.replace(~r"%{(\w+)}", message, fn _, key ->
        opts |> Keyword.get(String.to_existing_atom(key), key) |> to_string()
      end)
    end)
  end
end

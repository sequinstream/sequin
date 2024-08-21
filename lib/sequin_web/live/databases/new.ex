defmodule SequinWeb.DatabasesLive.New do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Databases
  alias Sequin.Databases.PostgresDatabase

  @impl Phoenix.LiveView
  def mount(_params, _session, socket) do
    changeset = Databases.PostgresDatabase.changeset(%PostgresDatabase{}, %{})

    socket =
      socket
      |> assign(:changeset, changeset)
      |> assign(:form_data, changeset_to_form_data(changeset))
      |> assign(:form_errors, %{})
      |> assign(:validating, false)

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
            formErrors: @form_errors,
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
  def handle_event("validate", %{"postgres_database" => params}, socket) do
    changeset =
      %PostgresDatabase{}
      |> PostgresDatabase.changeset(params)
      |> Map.put(:action, :validate)

    form_data = changeset_to_form_data(changeset)
    form_errors = errors_on(changeset)

    {:noreply,
     socket
     |> assign(:changeset, changeset)
     |> assign(:form_data, form_data)
     |> assign(:form_errors, form_errors)}
  end

  @impl Phoenix.LiveView
  def handle_event("save", %{"postgres_database" => params}, socket) do
    account_id = current_account_id(socket)
    params = Map.put(params, "account_id", account_id)

    socket =
      socket
      |> assign(:validating, true)
      |> push_event("validating", %{})

    # Perform async validation
    Task.async(fn -> validate_and_create(account_id, params) end)

    {:noreply, socket}
  end

  @impl Phoenix.LiveView
  def handle_info({ref, result}, socket) when is_reference(ref) do
    Process.demonitor(ref, [:flush])
    handle_validation_result(result, socket)
  end

  defp validate_and_create(account_id, params) do
    # Create a temporary struct for validation
    temp_db = %PostgresDatabase{
      account_id: account_id,
      database: params["database"],
      hostname: params["hostname"],
      port: params["port"],
      username: params["username"],
      password: params["password"],
      ssl: params["ssl"]
    }

    with :ok <- Databases.test_tcp_reachability(temp_db),
         :ok <- Databases.test_connect(temp_db),
         {:ok, db} <- Databases.create_db_for_account(account_id, params),
         {:ok, db} <- Databases.update_tables(db) do
      {:ok, db}
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

  defp handle_validation_result({:ok, database}, socket) do
    {:noreply,
     socket
     |> assign(:validating, false)
     |> put_flash(:info, "Database created successfully")
     |> push_navigate(to: ~p"/databases/#{database.id}")}
  end

  defp handle_validation_result({:error, %Ecto.Changeset{} = changeset}, socket) do
    form_data = changeset_to_form_data(changeset)
    form_errors = errors_on(changeset)

    {:noreply,
     socket
     |> assign(:validating, false)
     |> assign(:changeset, changeset)
     |> assign(:form_data, form_data)
     |> assign(:form_errors, form_errors)
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

  defp changeset_to_form_data(changeset) do
    %{
      database: Ecto.Changeset.get_field(changeset, :database) || "",
      hostname: Ecto.Changeset.get_field(changeset, :hostname) || "",
      port: Ecto.Changeset.get_field(changeset, :port) || 5432,
      name: Ecto.Changeset.get_field(changeset, :name) || "",
      ssl: Ecto.Changeset.get_field(changeset, :ssl) || false,
      username: Ecto.Changeset.get_field(changeset, :username) || "",
      password: Ecto.Changeset.get_field(changeset, :password) || ""
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

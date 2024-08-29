defmodule SequinWeb.DatabasesLive.Form do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Databases
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Error
  alias Sequin.Error.NotFoundError
  alias Sequin.Health
  alias Sequin.Name
  alias Sequin.Replication
  alias Sequin.Replication.PostgresReplicationSlot
  alias Sequin.Repo

  require Logger

  @impl Phoenix.LiveView
  def mount(params, _session, socket) do
    is_edit? = Map.has_key?(params, "id")
    base_params = if is_edit?, do: %{}, else: %{"name" => Name.generate(99)}

    case fetch_or_build_database(socket, params) do
      {:ok, database} ->
        socket =
          socket
          |> assign(
            is_edit?: is_edit?,
            show_errors?: false,
            submit_error: nil,
            database: database
          )
          |> put_changesets(%{
            "database" => base_params,
            "replication_slot" => %{"slot_name" => "sequin_slot", "publication_name" => "sequin_pub"}
          })

        {:ok, socket}

      {:error, %NotFoundError{}} ->
        Logger.error("Database not found (id=#{params["id"]})")
        {:ok, push_navigate(socket, to: ~p"/databases")}
    end
  end

  defp fetch_or_build_database(socket, %{"id" => id}) do
    with {:ok, database} <- Databases.get_db_for_account(current_account_id(socket), id) do
      {:ok, Repo.preload(database, :replication_slot)}
    end
  end

  defp fetch_or_build_database(_socket, _) do
    {:ok, %PostgresDatabase{}}
  end

  @parent_id "databases_form"
  @impl Phoenix.LiveView
  def render(assigns) do
    %{changeset: changeset, replication_changeset: replication_changeset} = assigns

    assigns =
      assigns
      |> assign(:form_data, changeset_to_form_data(changeset, replication_changeset))
      |> assign(:parent_id, @parent_id)
      |> assign(
        :form_errors,
        %{
          database: Error.errors_on(changeset),
          replication: Error.errors_on(replication_changeset)
        }
      )

    ~H"""
    <div id={@parent_id}>
      <.svelte
        name="databases/Form"
        ssr={false}
        props={
          %{
            database: @form_data,
            errors: if(@show_errors?, do: @form_errors, else: %{}),
            parent: @parent_id,
            submitError: @submit_error
          }
        }
      />
    </div>
    """
  end

  @impl Phoenix.LiveView
  def handle_event("form_updated", %{"form" => form}, socket) do
    params = decode_params(form)

    socket = put_changesets(socket, params)

    {:noreply, socket}
  end

  @impl Phoenix.LiveView
  def handle_event("form_submitted", %{"form" => form}, socket) do
    params = decode_params(form)

    socket =
      socket
      |> put_changesets(params)
      |> assign(:show_errors?, true)

    with true <- socket.assigns.changeset.valid?,
         true <- socket.assigns.replication_changeset.valid? do
      case validate_and_create_or_update(socket, params) do
        {:ok, database} ->
          {:noreply, push_navigate(socket, to: ~p"/databases/#{database.id}")}

        {:error, %Ecto.Changeset{} = changeset} ->
          {:noreply, assign(socket, :changeset, changeset)}

        {:error, error} when is_exception(error) ->
          {:noreply, assign(socket, :submit_error, Exception.message(error))}

        {:error, error} ->
          {:noreply, assign(socket, :submit_error, error_msg(error))}
      end
    else
      _ ->
        {:noreply, socket}
    end
  end

  @impl Phoenix.LiveView
  def handle_event("form_closed", _params, socket) do
    socket =
      if socket.assigns.is_edit? do
        push_navigate(socket, to: ~p"/databases/#{socket.assigns.changeset.data.id}")
      else
        push_navigate(socket, to: ~p"/databases")
      end

    {:noreply, socket}
  end

  defp put_changesets(socket, params) do
    is_edit? = socket.assigns.is_edit?
    database = socket.assigns.database

    changeset =
      if is_edit? do
        PostgresDatabase.update_changeset(database, params["database"])
      else
        PostgresDatabase.create_changeset(%PostgresDatabase{account_id: current_account_id(socket)}, params["database"])
      end

    replication_changeset =
      if is_edit? do
        PostgresReplicationSlot.update_changeset(database.replication_slot, params["replication_slot"])
      else
        PostgresReplicationSlot.create_changeset(%PostgresReplicationSlot{}, params["replication_slot"])
      end

    socket
    |> assign(:changeset, changeset)
    |> assign(:replication_changeset, replication_changeset)
  end

  def error_msg(error) do
    case error do
      {:error, :econnrefused} ->
        "Connection refused. Please check if the database server is running and accessible."

      {:error, :timeout} ->
        "Connection timed out. Please verify the hostname and port are correct."

      {:error, :nxdomain} ->
        "Unable to resolve the hostname. Please check if the hostname is correct."

      %Postgrex.Error{postgres: %{code: :invalid_authorization_specification}} ->
        "Invalid username or password. Please check your credentials."

      %Postgrex.Error{postgres: %{code: :invalid_catalog_name}} ->
        "Database does not exist. Please verify the database name."

      {:error, :database_connect_forbidden} ->
        "The provided user does not have permission to connect to the database."

      {:error, :database_create_forbidden} ->
        "The provided user does not have permission to create objects in the database."

      {:error, :transaction_read_only} ->
        "The database is in read-only mode. Please ensure the user has write permissions."

      {:error, :namespace_usage_forbidden} ->
        "The provided user does not have usage permission on the specified schema."

      {:error, :namespace_create_forbidden} ->
        "The provided user does not have permission to create objects in the specified schema."

      {:error, :unknown_privileges} ->
        "Unable to determine user privileges. Please ensure the user has necessary permissions."

      _ ->
        "An unexpected error occurred. Please try again or contact us."
    end
  end

  defp validate_and_create_or_update(socket, params) do
    account_id = current_account_id(socket)
    db_params = Map.put(params["database"], "account_id", account_id)
    replication_params = params["replication_slot"]

    temp_db = struct(PostgresDatabase, Sequin.Map.atomize_keys(db_params))
    temp_slot = struct(PostgresReplicationSlot, Sequin.Map.atomize_keys(replication_params))

    with :ok <- Databases.test_tcp_reachability(temp_db),
         :ok <- Databases.test_connect(temp_db, 10_000),
         :ok <- Databases.test_permissions(temp_db),
         :ok <- Databases.test_slot_permissions(temp_db, temp_slot) do
      if socket.assigns.is_edit? do
        update_database(socket.assigns.database, db_params, replication_params)
      else
        create_database(account_id, db_params, replication_params)
      end
    end
  end

  defp update_database(database, db_params, replication_params) do
    Repo.transact(fn ->
      database
      |> Databases.update_db(db_params)
      |> case do
        {:ok, updated_db} ->
          replication_slot = Repo.preload(updated_db, :replication_slot).replication_slot

          case Replication.update_pg_replication(replication_slot, replication_params) do
            {:ok, _replication} -> {:ok, updated_db}
            {:error, error} -> {:error, error}
          end

        {:error, error} ->
          {:error, error}
      end
    end)
  end

  defp create_database(account_id, db_params, replication_params) do
    Repo.transact(fn ->
      account_id
      |> Databases.create_db_for_account_with_lifecycle(db_params)
      |> case do
        {:ok, db} ->
          # Safe to update health here because we just validated that the database is reachable
          # TODO: Implement background health updates for reachability
          Health.update(db, :reachable, :healthy)

          case Replication.create_pg_replication_for_account_with_lifecycle(
                 account_id,
                 Map.put(replication_params, "postgres_database_id", db.id)
               ) do
            {:ok, _replication} -> {:ok, db}
            {:error, error} -> {:error, error}
          end

        {:error, error} ->
          {:error, error}
      end
    end)
  end

  defp changeset_to_form_data(changeset, replication_changeset) do
    %{
      id: Ecto.Changeset.get_field(changeset, :id),
      name: Ecto.Changeset.get_field(changeset, :name),
      database: Ecto.Changeset.get_field(changeset, :database),
      hostname: Ecto.Changeset.get_field(changeset, :hostname),
      port: Ecto.Changeset.get_field(changeset, :port) || 5432,
      username: Ecto.Changeset.get_field(changeset, :username),
      password: Ecto.Changeset.get_field(changeset, :password),
      ssl: Ecto.Changeset.get_field(changeset, :ssl) || false,
      publication_name: Ecto.Changeset.get_field(replication_changeset, :publication_name),
      slot_name: Ecto.Changeset.get_field(replication_changeset, :slot_name)
    }
  end

  defp decode_params(form) do
    port =
      case form["port"] do
        nil -> nil
        "" -> nil
        port when is_binary(port) -> String.to_integer(port)
        port when is_integer(port) -> port
      end

    %{
      "database" => %{
        "name" => form["name"],
        "hostname" => form["hostname"],
        "port" => port,
        "database" => form["database"],
        "username" => form["username"],
        "password" => form["password"],
        "ssl" => form["ssl"]
      },
      "replication_slot" => %{
        "publication_name" => form["publication_name"],
        "slot_name" => form["slot_name"]
      }
    }
  end
end

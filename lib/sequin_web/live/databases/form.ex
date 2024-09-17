defmodule SequinWeb.DatabasesLive.Form do
  @moduledoc false
  use SequinWeb, :live_view

  import Sequin.Error.Guards, only: [is_error: 1]

  alias Sequin.Databases
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Error
  alias Sequin.Error.NotFoundError
  alias Sequin.Health
  alias Sequin.Name
  alias Sequin.Posthog
  alias Sequin.Replication
  alias Sequin.Replication.PostgresReplicationSlot
  alias Sequin.Repo

  require Logger

  @impl Phoenix.LiveView
  def mount(params, _session, socket) do
    id = Map.get(params, "id")

    case fetch_or_build_database(socket, id) do
      {:ok, database} ->
        socket =
          socket
          |> assign(
            is_edit?: not is_nil(id),
            show_errors?: false,
            submit_error: nil,
            database: database
          )
          |> put_changesets(%{"database" => %{}, "replication_slot" => %{}})
          |> assign(:show_supabase_pooler_prompt, false)

        {:ok, socket}

      {:error, %NotFoundError{}} ->
        Logger.error("Database not found (id=#{id})")
        {:ok, push_navigate(socket, to: ~p"/databases")}
    end
  end

  defp fetch_or_build_database(socket, nil) do
    {:ok, %PostgresDatabase{account_id: current_account_id(socket), replication_slot: %PostgresReplicationSlot{}}}
  end

  defp fetch_or_build_database(socket, id) do
    with {:ok, database} <- Databases.get_db_for_account(current_account_id(socket), id) do
      {:ok, Repo.preload(database, :replication_slot)}
    end
  end

  @parent_id "databases_form"
  @impl Phoenix.LiveView
  def render(assigns) do
    %{changeset: changeset, replication_changeset: replication_changeset} = assigns

    assigns =
      assigns
      |> assign(:encoded_database, encode_database(assigns.database))
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
            database: @encoded_database,
            errors: if(@show_errors?, do: @form_errors, else: %{}),
            parent: @parent_id,
            submitError: @submit_error,
            showSupabasePoolerPrompt: @show_supabase_pooler_prompt
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

    show_supabase_pooler_prompt = detect_supabase_pooled(params["database"])
    socket = assign(socket, :show_supabase_pooler_prompt, show_supabase_pooler_prompt)

    {:noreply, socket}
  end

  @impl Phoenix.LiveView
  def handle_event("convert_supabase_connection", %{"form" => form}, socket) do
    params = decode_params(form)
    converted_params = convert_supabase_connection(params["database"])

    socket = assign(socket, :show_supabase_pooler_prompt, false)

    {:reply, %{converted: converted_params}, socket}
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
          Posthog.capture("Database Created", %{
            distinct_id: socket.assigns.current_user.id,
            properties: %{
              database_id: database.id,
              database_name: database.name,
              "$groups": %{account: database.account_id}
            }
          })

          {:reply, %{ok: true}, push_navigate(socket, to: ~p"/databases/#{database.id}")}

        {:error, %Ecto.Changeset{} = changeset} ->
          {:reply, %{ok: false}, assign(socket, :changeset, changeset)}

        {:error, %Postgrex.Error{postgres: %{code: :invalid_catalog_name}}} ->
          changeset = socket.assigns.changeset

          changeset =
            Ecto.Changeset.add_error(changeset, :database, "Database does not exist.")

          {:reply, %{ok: false},
           socket
           |> assign(:submit_error, "Database does not exist. See errors above.")
           |> assign(:changeset, changeset)}

        {:error, error} when is_error(error) ->
          {:reply, %{ok: false}, assign(socket, :submit_error, Exception.message(error))}

        {:error, error} ->
          {:reply, %{ok: false}, assign(socket, :submit_error, error_msg(error))}
      end
    else
      _ ->
        {:reply, %{ok: false}, socket}
    end
  rescue
    error ->
      Logger.error("Crashed in databases/form.ex:handle_event/2: #{inspect(error)}")
      {:noreply, assign(socket, :submit_error, error_msg(error))}
  end

  @impl Phoenix.LiveView
  def handle_event("form_closed", _params, socket) do
    socket =
      if socket.assigns.is_edit? do
        push_navigate(socket, to: ~p"/databases/#{socket.assigns.database.id}")
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
        PostgresDatabase.create_changeset(database, params["database"])
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
      {:error, error} ->
        error_msg(error)

      :econnrefused ->
        "Connection refused. Please check if the database server is running and accessible."

      :timeout ->
        "Connection timed out. Please verify the hostname and port are correct."

      :nxdomain ->
        "Unable to resolve the hostname. Please check if the hostname is correct."

      %Postgrex.Error{postgres: %{code: :invalid_authorization_specification}} ->
        "Authorization failed. This means either the username/password is invalid or the database requires SSL, which you can enable above."

      %Postgrex.Error{postgres: %{code: :invalid_password}} ->
        "Authorization failed. This means either the username/password is invalid or the database requires SSL, which you can enable above."

      %Postgrex.Error{postgres: %{code: :invalid_catalog_name}} ->
        "Database does not exist. Please verify the database name."

      %Postgrex.Error{} = error ->
        Logger.warning("Unhandled Postgrex error in databases/form.ex:error_msg/1: #{inspect(error)}")
        Exception.message(error)

      :database_connect_forbidden ->
        "The provided user does not have permission to connect to the database."

      :database_create_forbidden ->
        "The provided user does not have permission to create objects in the database."

      :transaction_read_only ->
        "The database is in read-only mode. Please ensure the user has write permissions."

      :namespace_usage_forbidden ->
        "The provided user does not have usage permission on the specified schema."

      :namespace_create_forbidden ->
        "The provided user does not have permission to create objects in the specified schema."

      :unknown_privileges ->
        "Unable to determine user privileges. Please ensure the user has necessary permissions."

      unexpected ->
        Logger.error("Unexpected error in databases/form.ex:error_msg/1: #{inspect(unexpected)}")
        "An unexpected error occurred. Please try again or contact us."
    end
  end

  defp validate_and_create_or_update(socket, params) do
    account_id = current_account_id(socket)
    db_params = Map.put(params["database"], "account_id", account_id)
    replication_params = params["replication_slot"]

    temp_db = struct(PostgresDatabase, Sequin.Map.atomize_keys(db_params))
    temp_slot = struct(PostgresReplicationSlot, Sequin.Map.atomize_keys(replication_params))

    with {:ok, ipv6?} <- Sequin.NetworkUtils.check_ipv6(temp_db.hostname),
         temp_db = %PostgresDatabase{temp_db | ipv6: ipv6?},
         db_params = Map.put(db_params, "ipv6", ipv6?),
         :ok <- Databases.test_tcp_reachability(temp_db),
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

  defp encode_database(%PostgresDatabase{} = database) do
    %{
      "id" => database.id,
      "name" => database.name || Name.generate(99),
      "database" => database.database,
      "hostname" => database.hostname,
      "port" => database.port || 5432,
      "username" => database.username,
      "password" => database.password,
      "ssl" => database.ssl || false,
      "publication_name" => database.replication_slot.publication_name || "sequin_pub",
      "slot_name" => database.replication_slot.slot_name || "sequin_slot"
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

  # Add these helper functions
  defp detect_supabase_pooled(%{"username" => username, "hostname" => hostname}) do
    username && hostname && String.contains?(username, ".") && String.contains?(hostname, "pooler.supabase.com")
  end

  defp detect_supabase_pooled(_), do: false

  defp convert_supabase_connection(%{"username" => username} = params) do
    [_, project_name] = String.split(username, ".")

    params
    |> Map.put("username", "postgres")
    |> Map.put("hostname", "db.#{project_name}.supabase.co")
    |> Map.put("port", 5432)
  end
end

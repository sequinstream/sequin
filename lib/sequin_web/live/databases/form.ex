defmodule SequinWeb.DatabasesLive.Form do
  @moduledoc false
  use SequinWeb, :live_view

  import Sequin.Error.Guards, only: [is_error: 1]

  alias Sequin.Accounts
  alias Sequin.Accounts.User
  alias Sequin.ApiTokens
  alias Sequin.Databases
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.DatabasesRuntime.Supervisor, as: DatabasesRuntimeSupervisor
  alias Sequin.Error
  alias Sequin.Error.NotFoundError
  alias Sequin.Name
  alias Sequin.Posthog
  alias Sequin.Replication
  alias Sequin.Replication.PostgresReplicationSlot
  alias Sequin.Repo

  require Logger

  @impl Phoenix.LiveView
  def mount(params, _session, socket) do
    current_account = User.current_account(socket.assigns.current_user)
    id = Map.get(params, "id")

    case fetch_or_build_database(socket, id) do
      {:ok, database} ->
        api_tokens = encode_api_tokens(ApiTokens.list_tokens_for_account(current_account.id))

        socket =
          socket
          |> assign(
            is_edit?: not is_nil(id),
            show_errors?: false,
            submit_error: nil,
            database: database,
            api_tokens: api_tokens,
            allocated_bastion_port: nil,
            update_allocated_bastion_port_timer: nil,
            pg_major_version: nil
          )
          |> put_changesets(%{"database" => %{}, "replication_slot" => %{}})
          |> assign(:show_supabase_pooler_prompt, false)

        {:ok, socket}

      {:error, %NotFoundError{}} ->
        Logger.error("Database not found (id=#{id})")
        {:ok, push_navigate(socket, to: ~p"/databases")}
    end
  end

  @impl Phoenix.LiveView
  def handle_params(params, _uri, socket) do
    if Map.get(params, "localhost") do
      database = socket.assigns.database
      {:noreply, assign(socket, :database, %{database | use_local_tunnel: true})}
    else
      {:noreply, socket}
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
      |> assign(:show_local_tunnel_prompt, not Application.get_env(:sequin, :self_hosted))
      |> assign(:show_pg_version_warning, assigns.pg_major_version && assigns.pg_major_version < 14)

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
            showSupabasePoolerPrompt: @show_supabase_pooler_prompt,
            api_tokens: @api_tokens,
            showLocalTunnelPrompt: @show_local_tunnel_prompt,
            showPgVersionWarning: @show_pg_version_warning
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

    socket = maybe_allocate_bastion_port(socket)

    {:noreply, socket}
  end

  @impl Phoenix.LiveView
  def handle_event("test_connection", %{"form" => form}, socket) do
    params = decode_params(form)

    with :ok <- test_db_conn(params, socket),
         {:ok, major_version} <- Databases.get_major_pg_version(params_to_db(params, socket)) do
      {:reply, %{ok: true}, assign(socket, :pg_major_version, major_version)}
    else
      {:error, error} ->
        {:reply, %{ok: false, error: error_msg(error, false)}, socket}
    end
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
              database_hostname: database.hostname,
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
          Logger.error("Error in DatabasesLive.Form#validate_and_create_or_update", error: error)
          {:reply, %{ok: false}, assign(socket, :submit_error, error_msg(error, params["database"]["use_local_tunnel"]))}
      end
    else
      _ ->
        {:reply, %{ok: false}, socket}
    end
  rescue
    error ->
      Logger.error("Crashed in databases/form.ex:handle_event/2: #{inspect(error)}")
      {:noreply, assign(socket, :submit_error, error_msg(error, false))}
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

  @impl Phoenix.LiveView
  def handle_info(:allocate_bastion_port, socket) do
    name = Ecto.Changeset.get_change(socket.assigns.changeset, :name)
    {:ok, abp} = Accounts.get_or_allocate_bastion_port_for_account(current_account_id(socket), name)
    {:noreply, socket |> assign(:allocated_bastion_port, abp) |> assign(:update_allocated_bastion_port_timer, nil)}
  end

  def handle_info({:EXIT, pid, :normal}, socket) do
    # We are still tracing down why we receive EXIT messages, seemed to happen when the Repo.transact
    # in this LiveView timed out
    Logger.warning("Received EXIT message with :normal from #{inspect(pid)}")
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

  def error_msg(error, use_local_tunnel) do
    case error do
      {:error, error} ->
        error_msg(error, use_local_tunnel)

      :econnrefused ->
        maybe_append_network_instructions(
          "Connection refused. Please check if the database server is running and accessible.",
          use_local_tunnel
        )

      :timeout ->
        maybe_append_network_instructions(
          "Connection timed out. Please verify the hostname and port are correct.",
          use_local_tunnel
        )

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

      %DBConnection.ConnectionError{reason: :queue_timeout} ->
        maybe_append_network_instructions(
          "The database is not reachable. Please ensure the database server is running and accessible.",
          use_local_tunnel
        )

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

      %ArgumentError{message: message} ->
        message

      %Sequin.Error.ValidationError{summary: summary} ->
        summary

      unexpected ->
        Logger.error("Unexpected error in databases/form.ex:error_msg/1: #{inspect(unexpected)}")
        "An unexpected error occurred. Please try again or contact us."
    end
  end

  defp maybe_append_network_instructions(error, use_local_tunnel) do
    cond do
      use_local_tunnel ->
        "#{error} Also, please make sure your tunnel is running via the Sequin CLI."

      Application.get_env(:sequin, :self_hosted) ->
        "#{error} If you are running Sequin inside of Docker and trying to connect to a database on your local machine, use the host `host.docker.internal` instead of `localhost`."

      true ->
        error
    end
  end

  defp validate_and_create_or_update(socket, params) do
    account_id = current_account_id(socket)

    db_params =
      params["database"]
      |> Map.put("account_id", account_id)
      |> put_ipv6()

    replication_params = params["replication_slot"]

    Repo.transact(
      fn ->
        res =
          if socket.assigns.is_edit? do
            update_database(socket.assigns.database, db_params, replication_params)
          else
            create_database(account_id, db_params, replication_params)
          end

        with {:ok, db} <- res do
          # It's now safe to start the replication slot
          DatabasesRuntimeSupervisor.start_replication(db.replication_slot)

          {:ok, db}
        end
      end,
      timeout: :timer.seconds(30)
    )
  end

  defp test_db_conn(params, socket) do
    db = params_to_db(params, socket)

    replication_slot =
      params["replication_slot"]
      |> Sequin.Map.atomize_keys()
      |> then(&struct(PostgresReplicationSlot, &1))

    with :ok <- Databases.test_tcp_reachability(db),
         :ok <- Databases.test_connect(db, 10_000),
         :ok <- Databases.test_permissions(db) do
      Databases.verify_slot(db, replication_slot)
    end
  end

  defp params_to_db(params, socket) do
    params["database"]
    |> put_ipv6()
    |> Sequin.Map.atomize_keys()
    |> Map.put(:account_id, current_account_id(socket))
    |> then(&struct(PostgresDatabase, &1))
  end

  defp put_ipv6(%{"use_local_tunnel" => true} = db_params), do: db_params

  defp put_ipv6(db_params) do
    case Sequin.NetworkUtils.check_ipv6(db_params["hostname"]) do
      {:ok, true} -> Map.put(db_params, "ipv6", true)
      {:ok, false} -> Map.put(db_params, "ipv6", false)
      {:error, _error} -> db_params
    end
  end

  defp update_database(database, db_params, replication_params) do
    database
    |> Databases.update_db(db_params)
    |> case do
      {:ok, updated_db} ->
        replication_slot = Repo.preload(updated_db, :replication_slot).replication_slot

        case Replication.update_pg_replication(replication_slot, replication_params) do
          {:ok, replication} -> {:ok, %PostgresDatabase{updated_db | replication_slot: replication}}
          {:error, error} -> {:error, error}
        end

      {:error, error} ->
        {:error, error}
    end
  end

  defp create_database(account_id, db_params, replication_params) do
    account_id
    |> Databases.create_db_for_account_with_lifecycle(db_params)
    |> case do
      {:ok, db} ->
        case Replication.create_pg_replication_for_account_with_lifecycle(
               account_id,
               Map.put(replication_params, "postgres_database_id", db.id)
             ) do
          {:ok, replication} -> {:ok, %PostgresDatabase{db | replication_slot: replication}}
          {:error, error} -> {:error, error}
        end

      {:error, error} ->
        {:error, error}
    end
  end

  defp encode_database(%PostgresDatabase{} = database) do
    %{
      "id" => database.id,
      "name" => database.name || Name.generate(99),
      "database" => database.database,
      "hostname" => database.hostname,
      "port" => database.port || 5432,
      "username" => database.username || "postgres",
      "password" => database.password,
      "ssl" => database.ssl || true,
      "publication_name" => database.replication_slot.publication_name || "sequin_pub",
      "slot_name" => database.replication_slot.slot_name || "sequin_slot",
      "useLocalTunnel" => database.use_local_tunnel || false
    }
  end

  defp encode_api_tokens(api_tokens) when is_list(api_tokens) do
    Enum.map(api_tokens, fn api_token ->
      %{
        id: api_token.id,
        name: api_token.name,
        inserted_at: api_token.inserted_at,
        token: api_token.token
      }
    end)
  end

  defp decode_params(form) do
    port =
      case form["port"] do
        nil -> nil
        "" -> nil
        port when is_binary(port) -> String.to_integer(port)
        port when is_integer(port) -> port
      end

    ssl = if form["useLocalTunnel"], do: false, else: form["ssl"]
    hostname = if form["useLocalTunnel"], do: Application.get_env(:sequin, :portal_hostname), else: form["hostname"]

    %{
      "database" => %{
        "name" => form["name"],
        "hostname" => maybe_trim(hostname),
        "port" => port,
        "database" => maybe_trim(form["database"]),
        "username" => maybe_trim(form["username"]),
        "password" => form["password"],
        "ssl" => ssl,
        "use_local_tunnel" => form["useLocalTunnel"]
      },
      "replication_slot" => %{
        "publication_name" => maybe_trim(form["publication_name"]),
        "slot_name" => maybe_trim(form["slot_name"])
      }
    }
  end

  defp maybe_trim(nil), do: nil
  defp maybe_trim(value), do: String.trim(value)

  defp detect_supabase_pooled(%{"username" => username, "hostname" => hostname}) do
    username && hostname && String.contains?(username, ".") && String.contains?(hostname, "pooler.supabase")
  end

  defp detect_supabase_pooled(_), do: false

  defp convert_supabase_connection(%{"username" => username} = params) do
    [_, project_name] = String.split(username, ".")

    params
    |> Map.put("username", "postgres")
    |> Map.put("hostname", "db.#{project_name}.supabase.co")
    |> Map.put("port", 5432)
  end

  defp maybe_allocate_bastion_port(socket) do
    %Ecto.Changeset{} = changeset = socket.assigns.changeset
    use_local_tunnel = Ecto.Changeset.get_change(changeset, :use_local_tunnel, false)
    name = Ecto.Changeset.get_change(changeset, :name)
    allocated_bastion_port = socket.assigns.allocated_bastion_port
    existing_timer = socket.assigns.update_allocated_bastion_port_timer

    allocated_bastion_port_changed? = is_nil(allocated_bastion_port) or allocated_bastion_port.name != name

    if use_local_tunnel and not socket.assigns.is_edit? and allocated_bastion_port_changed? do
      # Debounce change
      existing_timer && Process.cancel_timer(existing_timer)
      timer = Process.send_after(self(), :allocate_bastion_port, 500)
      assign(socket, :update_allocated_bastion_port_timer, timer)
    else
      socket
    end
  end
end

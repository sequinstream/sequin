defmodule SequinWeb.DatabasesLive.Form do
  @moduledoc false
  use SequinWeb, :live_view

  import Sequin.Error.Guards, only: [is_error: 1]

  alias Sequin.Accounts
  alias Sequin.Accounts.User
  alias Sequin.ApiTokens
  alias Sequin.Databases
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Error
  alias Sequin.Error.NotFoundError
  alias Sequin.Name
  alias Sequin.Posthog
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
            pg_major_version: nil,
            self_hosted: Application.get_env(:sequin, :self_hosted)
          )
          |> put_changesets(%{"database" => %{}, "replication_slot" => %{}})
          |> assign(:pooler_type, nil)

        {:ok, socket, layout: {SequinWeb.Layouts, :app_no_sidenav}}

      {:error, %NotFoundError{}} ->
        Logger.error("Database not found (id=#{id})")
        {:ok, push_navigate(socket, to: ~p"/databases"), layout: {SequinWeb.Layouts, :app_no_sidenav}}
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
      |> assign(:show_pg_version_warning, assigns.pg_major_version && assigns.pg_major_version < 12)

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
            api_tokens: @api_tokens,
            showLocalTunnelPrompt: @show_local_tunnel_prompt,
            showPgVersionWarning: @show_pg_version_warning,
            selfHosted: @self_hosted,
            poolerType: @pooler_type
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

    pooler_type = detect_pooled_connection(params["database"])
    socket = assign(socket, :pooler_type, pooler_type)

    socket = maybe_allocate_bastion_port(socket)

    {:noreply, socket}
  end

  @impl Phoenix.LiveView
  def handle_event("test_connection", %{"form" => form}, socket) do
    params = decode_params(form)

    with :ok <- test_db_conn(params, socket),
         {:ok, major_version} <- Databases.get_pg_major_version(params_to_db(params, socket)) do
      {:reply, %{ok: true}, assign(socket, :pg_major_version, major_version)}
    else
      {:error, error} ->
        {:reply, %{ok: false, error: error_msg(error, false)}, socket}
    end
  end

  @impl Phoenix.LiveView
  def handle_event("convert_pooled_connection", %{"form" => form}, socket) do
    params = decode_params(form)
    converted_params = convert_pooled_connection(params["database"])

    socket = assign(socket, :pooler_type, nil)

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

      %Error.NotFoundError{entity: :replication_slot, params: %{name: name}} ->
        "Replication slot `#{name}` does not exist. Please check the slot name and ensure it exists."

      %Error.NotFoundError{entity: :publication, params: %{name: name}} ->
        "Publication `#{name}` does not exist. Please check the publication name and ensure it exists."

      %Sequin.Error.ValidationError{summary: summary} ->
        summary

      error when is_exception(error) ->
        Exception.message(error)

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

    if socket.assigns.is_edit? do
      Databases.update_db_with_slot(socket.assigns.database, db_params, replication_params)
    else
      Databases.create_db_with_slot(account_id, db_params, replication_params)
    end
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

  defp encode_database(%PostgresDatabase{} = database) do
    ssl = if database.ssl == false, do: false, else: true

    %{
      "id" => database.id,
      "name" => database.name || Name.generate(99),
      "database" => database.database,
      "hostname" => database.hostname,
      "port" => database.port || 5432,
      "username" => database.username || "postgres",
      "password" => database.password,
      "ssl" => ssl,
      "pool_size" => database.pool_size || 10,
      "publication_name" => database.replication_slot.publication_name || "sequin_pub",
      "slot_name" => database.replication_slot.slot_name || "sequin_slot",
      "useLocalTunnel" => database.use_local_tunnel || false,
      "is_replica" => not is_nil(database.primary),
      "primary" =>
        case database.primary do
          nil ->
            %{"url" => nil, "ssl" => true}

          primary ->
            %{
              "hostname" => primary.hostname,
              "database" => primary.database,
              "port" => primary.port || 5432,
              "username" => primary.username || "postgres",
              "password" => primary.password,
              "ssl" => primary.ssl
            }
        end
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

    pool_size =
      case form["pool_size"] do
        nil -> nil
        "" -> nil
        pool_size when is_binary(pool_size) -> String.to_integer(pool_size)
        pool_size when is_integer(pool_size) -> pool_size
      end

    ssl = if form["useLocalTunnel"], do: false, else: form["ssl"]
    hostname = if form["useLocalTunnel"], do: Application.get_env(:sequin, :portal_hostname), else: form["hostname"]

    primary = if form["is_replica"], do: form["primary"]

    %{
      "database" => %{
        "name" => form["name"],
        "hostname" => maybe_trim(hostname),
        "port" => port,
        "database" => maybe_trim(form["database"]),
        "username" => maybe_trim(form["username"]),
        "password" => form["password"],
        "ssl" => ssl,
        "pool_size" => pool_size,
        "use_local_tunnel" => form["useLocalTunnel"],
        "primary" => primary
      },
      "replication_slot" => %{
        "publication_name" => maybe_trim(form["publication_name"]),
        "slot_name" => maybe_trim(form["slot_name"])
      }
    }
  end

  defp maybe_trim(nil), do: nil
  defp maybe_trim(value), do: String.trim(value)

  defp detect_pooled_connection(%{"username" => username, "hostname" => hostname}) do
    cond do
      username && hostname && String.contains?(username, ".") && String.contains?(hostname, "pooler.supabase") ->
        "supabase"

      hostname && String.contains?(hostname, "-pooler.") && String.contains?(hostname, ".neon.tech") ->
        "neon"

      true ->
        nil
    end
  end

  defp detect_pooled_connection(_), do: nil

  defp convert_pooled_connection(%{"hostname" => hostname} = params) when is_binary(hostname) do
    cond do
      String.contains?(hostname, "pooler.supabase") ->
        convert_supabase_pooled_connection(params)

      String.contains?(hostname, "-pooler.") && String.contains?(hostname, ".neon.tech") ->
        convert_neon_pooled_connection(params)

      true ->
        params
    end
  end

  defp convert_pooled_connection(params), do: params

  defp convert_supabase_pooled_connection(%{"username" => username} = params) do
    [_, project_name] = String.split(username, ".")

    params
    |> Map.put("username", "postgres")
    |> Map.put("hostname", "db.#{project_name}.supabase.co")
    |> Map.put("port", 5432)
  end

  defp convert_neon_pooled_connection(%{"hostname" => hostname} = params) do
    # Replace -pooler. with . to get direct connection
    direct_hostname = String.replace(hostname, "-pooler.", ".")

    Map.put(params, "hostname", direct_hostname)
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

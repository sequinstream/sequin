defmodule SequinWeb.HttpEndpointsLive.Form do
  @moduledoc false
  use SequinWeb, :live_view

  alias Ecto.Changeset
  alias Sequin.Accounts
  alias Sequin.Accounts.User
  alias Sequin.ApiTokens
  alias Sequin.Consumers
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Error
  alias Sequin.Health.CheckHttpEndpointHealthWorker
  alias Sequin.Name
  alias Sequin.Repo

  @parent_id "http_endpoints_form"

  @impl Phoenix.LiveView
  def mount(params, _session, socket) do
    current_account = User.current_account(socket.assigns.current_user)
    is_edit? = Map.has_key?(params, "id")

    case fetch_or_build_http_endpoint(socket, params) do
      {:ok, http_endpoint} ->
        socket =
          socket
          |> assign(
            is_edit?: is_edit?,
            show_errors?: false,
            allocated_bastion_port: nil,
            submit_error: nil,
            http_endpoint: http_endpoint,
            update_allocated_bastion_port_timer: nil
          )
          |> assign(:api_tokens, encode_api_tokens(ApiTokens.list_tokens_for_account(current_account.id)))
          |> put_changeset(%{"http_endpoint" => %{}})

        {:ok, socket, layout: {SequinWeb.Layouts, :app_no_sidenav}}

      {:error, _} ->
        {:ok, push_navigate(socket, to: ~p"/http-endpoints"), layout: {SequinWeb.Layouts, :app_no_sidenav}}
    end
  end

  defp fetch_or_build_http_endpoint(socket, %{"id" => id}) do
    case Consumers.find_http_endpoint_for_account(current_account_id(socket), id: id) do
      {:ok, http_endpoint} ->
        {:ok, http_endpoint}

      error ->
        error
    end
  end

  defp fetch_or_build_http_endpoint(_socket, _) do
    {:ok, %HttpEndpoint{}}
  end

  @impl Phoenix.LiveView
  def render(assigns) do
    assigns =
      assigns
      |> assign(:encoded_http_endpoint, encode_http_endpoint(assigns.http_endpoint))
      |> assign(:parent_id, @parent_id)
      |> assign(:form_errors, Error.errors_on(assigns.changeset))

    ~H"""
    <div id={@parent_id}>
      <.svelte
        name="http_endpoints/Form"
        ssr={false}
        props={
          %{
            httpEndpoint: @encoded_http_endpoint,
            errors: if(@show_errors?, do: @form_errors, else: %{}),
            parent: @parent_id,
            api_tokens: @api_tokens
          }
        }
      />
    </div>
    """
  end

  @impl Phoenix.LiveView
  def handle_event("form_updated", %{"form" => form}, socket) do
    params = decode_params(form, socket.assigns.http_endpoint)

    socket =
      socket
      |> put_changeset(params)
      |> maybe_allocate_bastion_port()

    {:noreply, socket}
  end

  @impl Phoenix.LiveView
  def handle_event("form_submitted", %{"form" => form}, socket) do
    params = decode_params(form, socket.assigns.http_endpoint)

    socket =
      socket
      |> put_changeset(params)
      |> assign(:show_errors?, true)

    if socket.assigns.changeset.valid? do
      case create_or_update_http_endpoint(socket, params["http_endpoint"]) do
        {:ok, http_endpoint} ->
          CheckHttpEndpointHealthWorker.enqueue(http_endpoint.id)
          {:reply, %{ok: true}, push_navigate(socket, to: ~p"/http-endpoints/#{http_endpoint.id}")}

        {:error, %Ecto.Changeset{} = changeset} ->
          {:reply, %{ok: false}, assign(socket, :changeset, changeset)}
      end
    else
      {:reply, %{ok: false}, socket}
    end
  end

  @impl Phoenix.LiveView
  def handle_event("form_closed", _params, socket) do
    if socket.assigns.is_edit? do
      {:noreply, push_navigate(socket, to: ~p"/http-endpoints/#{socket.assigns.http_endpoint.id}")}
    else
      {:noreply, push_navigate(socket, to: ~p"/http-endpoints")}
    end
  end

  @impl Phoenix.LiveView
  def handle_info(:allocate_bastion_port, socket) do
    name = Changeset.get_change(socket.assigns.changeset, :name)
    {:ok, abp} = Accounts.get_or_allocate_bastion_port_for_account(current_account_id(socket), name)
    {:noreply, socket |> assign(:allocated_bastion_port, abp) |> assign(:update_allocated_bastion_port_timer, nil)}
  end

  defp put_changeset(socket, params) do
    changeset =
      if socket.assigns.is_edit? do
        HttpEndpoint.update_changeset(socket.assigns.http_endpoint, params["http_endpoint"])
      else
        HttpEndpoint.create_changeset(%HttpEndpoint{account_id: current_account_id(socket)}, params["http_endpoint"])
      end

    assign(socket, :changeset, changeset)
  end

  defp create_or_update_http_endpoint(socket, params) do
    Repo.transact(fn ->
      with {:ok, http_endpoint} <- create_or_update_base_endpoint(socket, params),
           :ok <- test_endpoint_reachability(http_endpoint) do
        {:ok, http_endpoint}
      end
    end)
  end

  defp create_or_update_base_endpoint(socket, http_endpoint_params) do
    if socket.assigns.is_edit? do
      Consumers.update_http_endpoint(socket.assigns.http_endpoint, http_endpoint_params)
    else
      Consumers.create_http_endpoint(current_account_id(socket), http_endpoint_params)
    end
  end

  defp test_endpoint_reachability(http_endpoint) do
    with {:ok, :reachable} <- Consumers.test_reachability(http_endpoint),
         {:ok, :connected} <- Consumers.test_connect(http_endpoint) do
      :ok
    else
      {:error, reason} ->
        changeset =
          http_endpoint
          |> Ecto.Changeset.change()
          |> Ecto.Changeset.add_error(:host, "Endpoint is not reachable or connectable: #{inspect(reason)}")

        {:error, changeset}
    end
  end

  defp encode_http_endpoint(%HttpEndpoint{} = http_endpoint) do
    %{
      "id" => http_endpoint.id,
      "name" => http_endpoint.name || Name.generate(99),
      "baseUrl" => HttpEndpoint.url(http_endpoint),
      "headers" => http_endpoint.headers || %{},
      "encryptedHeaders" => http_endpoint.encrypted_headers || %{},
      "useLocalTunnel" => http_endpoint.use_local_tunnel
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

  defp decode_params(form, http_endpoint) do
    use_local_tunnel = form["useLocalTunnel"]
    uri = URI.parse(form["baseUrl"])
    host = if use_local_tunnel, do: Application.get_env(:sequin, :portal_hostname), else: uri.host
    scheme = if use_local_tunnel, do: "http", else: uri.scheme
    port = if use_local_tunnel, do: http_endpoint.port, else: uri.port

    %{
      "http_endpoint" => %{
        "name" => form["name"],
        "scheme" => scheme,
        "userinfo" => uri.userinfo,
        "host" => host,
        "port" => port,
        "path" => uri.path,
        "query" => uri.query,
        "fragment" => uri.fragment,
        "headers" => form["headers"] || %{},
        "encrypted_headers" => form["encryptedHeaders"] || %{},
        "use_local_tunnel" => use_local_tunnel
      }
    }
  end

  defp maybe_allocate_bastion_port(socket) do
    %Changeset{} = changeset = socket.assigns.changeset
    use_local_tunnel = Changeset.get_change(changeset, :use_local_tunnel, false)
    name = Changeset.get_change(changeset, :name)
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

defmodule SequinWeb.HttpEndpointsLive.Form do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Consumers
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Error
  alias Sequin.Health
  alias Sequin.Name
  alias Sequin.Repo

  @parent_id "http_endpoints_form"

  @impl Phoenix.LiveView
  def mount(params, _session, socket) do
    is_edit? = Map.has_key?(params, "id")

    case fetch_or_build_http_endpoint(socket, params) do
      {:ok, http_endpoint} ->
        socket =
          socket
          |> assign(
            is_edit?: is_edit?,
            show_errors?: false,
            submit_error: nil,
            http_endpoint: http_endpoint
          )
          |> put_changeset(%{"http_endpoint" => %{}})

        {:ok, socket}

      {:error, _} ->
        {:ok, push_navigate(socket, to: ~p"/http-endpoints")}
    end
  end

  defp fetch_or_build_http_endpoint(socket, %{"id" => id}) do
    case Consumers.get_http_endpoint_for_account(current_account_id(socket), id) do
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
            parent: @parent_id
          }
        }
      />
    </div>
    """
  end

  @impl Phoenix.LiveView
  def handle_event("form_updated", %{"form" => form}, socket) do
    params = decode_params(form)
    socket = put_changeset(socket, params)
    {:noreply, socket}
  end

  @impl Phoenix.LiveView
  def handle_event("form_submitted", %{"form" => form}, socket) do
    params = decode_params(form)

    socket =
      socket
      |> put_changeset(params)
      |> assign(:show_errors?, true)

    if socket.assigns.changeset.valid? do
      case create_or_update_http_endpoint(socket, params["http_endpoint"]) do
        {:ok, http_endpoint} ->
          Health.update(http_endpoint, :reachable, :healthy)
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
      Consumers.create_http_endpoint_for_account(current_account_id(socket), http_endpoint_params)
    end
  end

  defp test_endpoint_reachability(%HttpEndpoint{use_local_tunnel: true}) do
    :ok
  end

  defp test_endpoint_reachability(http_endpoint) do
    case Consumers.test_reachability(http_endpoint) do
      {:ok, :reachable} ->
        :ok

      {:error, reason} ->
        changeset =
          http_endpoint
          |> Ecto.Changeset.change()
          |> Ecto.Changeset.add_error(:host, "Endpoint is not reachable: #{inspect(reason)}")

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

  defp decode_params(form) do
    use_local_tunnel = form["useLocalTunnel"]
    uri = URI.parse(form["baseUrl"])
    host = if use_local_tunnel, do: Application.get_env(:sequin, :portal_hostname), else: uri.host
    scheme = if use_local_tunnel, do: "http", else: uri.scheme

    %{
      "http_endpoint" => %{
        "name" => form["name"],
        "scheme" => scheme,
        "userinfo" => uri.userinfo,
        "host" => host,
        "port" => uri.port,
        "path" => uri.path,
        "query" => uri.query,
        "fragment" => uri.fragment,
        "headers" => form["headers"] || %{},
        "encrypted_headers" => form["encryptedHeaders"] || %{},
        "use_local_tunnel" => use_local_tunnel
      }
    }
  end
end

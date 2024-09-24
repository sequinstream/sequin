defmodule SequinWeb.HttpEndpointsLive.Form do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Consumers
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Error
  alias Sequin.Health
  alias Sequin.Name

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
    Consumers.get_http_endpoint_for_account(current_account_id(socket), id)
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
    changeset =
      if socket.assigns.is_edit? do
        HttpEndpoint.update_changeset(socket.assigns.http_endpoint, params)
      else
        HttpEndpoint.create_changeset(%HttpEndpoint{account_id: current_account_id(socket)}, params)
      end

    with {:ok, valid_changes} <- Ecto.Changeset.apply_action(changeset, :validate),
         {:ok, :reachable} <- Consumers.test_reachability(valid_changes) do
      if socket.assigns.is_edit? do
        Consumers.update_http_endpoint_with_lifecycle(socket.assigns.http_endpoint, params)
      else
        Consumers.create_http_endpoint_for_account(current_account_id(socket), params)
      end
    else
      {:error, %Ecto.Changeset{} = invalid_changeset} ->
        {:error, invalid_changeset}

      {:error, reason} ->
        changeset =
          Ecto.Changeset.add_error(changeset, :host, "Endpoint is not reachable: #{inspect(reason)}")

        {:error, changeset}
    end
  end

  defp encode_http_endpoint(%HttpEndpoint{} = http_endpoint) do
    %{
      "id" => http_endpoint.id,
      "name" => http_endpoint.name || Name.generate(99),
      "baseUrl" => HttpEndpoint.url(http_endpoint),
      "headers" => http_endpoint.headers || %{},
      # Add this line
      "encryptedHeaders" => http_endpoint.encrypted_headers || %{}
    }
  end

  defp decode_params(form) do
    uri = URI.parse(form["baseUrl"])

    %{
      "http_endpoint" => %{
        "name" => form["name"],
        "scheme" => uri.scheme,
        "userinfo" => uri.userinfo,
        "host" => uri.host,
        "port" => uri.port,
        "path" => uri.path,
        "query" => uri.query,
        "fragment" => uri.fragment,
        "headers" => form["headers"] || %{},
        "encrypted_headers" => form["encryptedHeaders"] || %{}
      }
    }
  end
end

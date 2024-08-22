defmodule SequinWeb.HttpEndpointsLive.New do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Consumers
  alias Sequin.Consumers.HttpEndpoint

  @impl Phoenix.LiveView
  def mount(_params, _session, socket) do
    changeset = HttpEndpoint.create_changeset(%HttpEndpoint{}, %{})

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
    <div id="new-http-endpoint" class="w-full">
      <.svelte
        name="http_endpoints/HttpEndpointContainer"
        props={
          %{
            formData: @form_data,
            formErrors: @form_errors,
            validating: @validating,
            parent: "new-http-endpoint"
          }
        }
        socket={@socket}
      />
    </div>
    """
  end

  @impl Phoenix.LiveView
  def handle_event("validate", %{"http_endpoint" => params}, socket) do
    changeset =
      %HttpEndpoint{}
      |> HttpEndpoint.create_changeset(params)
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
  def handle_event("save", %{"http_endpoint" => params}, socket) do
    account_id = current_account_id(socket)

    case Consumers.create_http_endpoint_for_account(account_id, params) do
      {:ok, http_endpoint} ->
        {:noreply,
         socket
         |> put_flash(:info, "HTTP Endpoint created successfully")
         |> push_navigate(to: ~p"/http-endpoints/#{http_endpoint.id}")}

      {:error, %Ecto.Changeset{} = changeset} ->
        form_data = changeset_to_form_data(changeset)
        form_errors = errors_on(changeset)

        {:noreply,
         socket
         |> assign(:changeset, changeset)
         |> assign(:form_data, form_data)
         |> assign(:form_errors, form_errors)}
    end
  end

  defp changeset_to_form_data(changeset) do
    %{
      name: Ecto.Changeset.get_field(changeset, :name) || "",
      base_url: Ecto.Changeset.get_field(changeset, :base_url) || "",
      headers: Ecto.Changeset.get_field(changeset, :headers) || %{}
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

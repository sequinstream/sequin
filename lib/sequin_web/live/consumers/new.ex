defmodule SequinWeb.ConsumersLive.New do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Consumers

  @impl Phoenix.LiveView
  def mount(_params, _session, socket) do
    # changeset = Consumers.change_http_pull_consumer(%HttpPullConsumer{})

    socket =
      socket
      |> assign(:changeset, %{})
      |> assign(:form_errors, %{})

    {:ok, socket, layout: {SequinWeb.Layouts, :app_no_main_no_sidenav}}
  end

  @impl Phoenix.LiveView
  def render(assigns) do
    ~H"""
    <div id="new-consumer">
      <.svelte
        name="consumers/New"
        props={
          %{
            changeset: @changeset,
            formErrors: @form_errors
          }
        }
        socket={@socket}
      />
    </div>
    """
  end

  @impl Phoenix.LiveView
  def handle_event("validate", _parmas, socket) do
    # changeset =
    #   %HttpPullConsumer{}
    #   |> Consumers.change_http_pull_consumer(consumer_params)
    #   |> Map.put(:action, :validate)

    {:noreply, assign(socket, :changeset, %{})}
  end

  @impl Phoenix.LiveView
  def handle_event("save", %{"http_pull_consumer" => consumer_params}, socket) do
    account_id = current_account_id(socket)
    consumer_params = Map.put(consumer_params, "account_id", account_id)

    case Consumers.create_http_pull_consumer_with_lifecycle(consumer_params) do
      {:ok, consumer} ->
        {:noreply,
         socket
         |> put_flash(:info, "Consumer created successfully")
         |> push_navigate(to: ~p"/consumers/#{consumer.id}")}

      {:error, %Ecto.Changeset{} = changeset} ->
        {:noreply, assign(socket, changeset: changeset, form_errors: errors_on(changeset))}
    end
  end

  defp errors_on(changeset) do
    Ecto.Changeset.traverse_errors(changeset, fn {message, opts} ->
      Regex.replace(~r"%{(\w+)}", message, fn _, key ->
        opts |> Keyword.get(String.to_existing_atom(key), key) |> to_string()
      end)
    end)
  end
end

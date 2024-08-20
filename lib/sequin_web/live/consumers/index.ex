defmodule SequinWeb.ConsumersLive.Index do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Consumers

  @impl Phoenix.LiveView
  def mount(_params, _session, socket) do
    account_id = current_account_id(socket)
    consumers = Consumers.list_consumers_for_account(account_id)
    encoded_consumers = Enum.map(consumers, &encode_consumer/1)

    socket =
      socket
      |> assign(:consumers, encoded_consumers)
      |> assign(:form_errors, %{})

    {:ok, socket}
  end

  @impl Phoenix.LiveView
  def render(assigns) do
    ~H"""
    <div id="consumers-index">
      <.svelte
        name="consumers/Index"
        props={
          %{
            consumers: @consumers,
            formErrors: @form_errors
          }
        }
        socket={@socket}
      />
    </div>
    """
  end

  @impl Phoenix.LiveView
  def handle_event("consumer_clicked", %{"id" => id}, socket) do
    {:noreply, push_navigate(socket, to: ~p"/consumers/#{id}")}
  end

  @impl Phoenix.LiveView
  def handle_event("consumer_submitted", %{"name" => name}, socket) do
    account_id = current_account_id(socket)

    case Consumers.create_http_pull_consumer_with_lifecycle(%{
           account_id: account_id,
           name: name
         }) do
      {:ok, consumer} ->
        encoded_consumer = encode_consumer(consumer)

        socket =
          socket
          |> update(:consumers, fn consumers -> [encoded_consumer | consumers] end)
          |> assign(:form_errors, %{})

        {:noreply, push_navigate(socket, to: ~p"/consumers/#{consumer.id}")}

      {:error, changeset} ->
        errors = Sequin.Error.errors_on(changeset)
        {:noreply, assign(socket, :form_errors, errors)}
    end
  end

  defp encode_consumer(consumer) do
    %{
      id: consumer.id,
      name: consumer.name,
      insertedAt: consumer.inserted_at,
      type: consumer_type(consumer),
      status: consumer.status
    }
  end

  defp consumer_type(%Consumers.HttpPullConsumer{}), do: "pull"
  defp consumer_type(%Consumers.HttpPushConsumer{}), do: "push"
end

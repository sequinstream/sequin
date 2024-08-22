defmodule SequinWeb.Live.Consumers.HttpPullConsumerForm do
  @moduledoc false
  use SequinWeb, :live_component

  alias Sequin.Consumers
  alias Sequin.Consumers.HttpPullConsumer

  @impl Phoenix.LiveComponent
  def render(assigns) do
    encoded_errors =
      if assigns.show_errors? do
        encode_errors(assigns.changeset)
      else
        %{}
      end

    assigns =
      assigns
      |> assign(:encoded_http_pull_consumer, encode_http_pull_consumer(assigns.http_pull_consumer))
      |> assign(:encoded_errors, encoded_errors)

    ~H"""
    <div id={@id}>
      <.svelte
        name="consumers/HttpPullForm"
        ssr={false}
        props={
          %{
            http_pull_consumer: @encoded_http_pull_consumer,
            errors: @encoded_errors,
            submitError: @submit_error,
            parent: @id
          }
        }
        socket={@socket}
      />
    </div>
    """
  end

  @impl Phoenix.LiveComponent
  def update(assigns, socket) do
    http_pull_consumer =
      assigns[:http_pull_consumer] || %HttpPullConsumer{account_id: current_account_id(assigns)}

    socket =
      socket
      |> assign(assigns)
      |> assign(
        http_pull_consumer: http_pull_consumer,
        show_errors?: false,
        submit_error: nil
      )
      |> reset_changeset()

    {:ok, socket}
  end

  @impl Phoenix.LiveComponent
  def handle_event("form_updated", %{"form" => form}, socket) do
    params = decode_params(form)
    socket = merge_changeset(socket, params)
    {:noreply, socket}
  end

  def handle_event("form_submitted", %{"form" => form}, socket) do
    socket = assign(socket, :submit_error, nil)
    params = decode_params(form)

    socket =
      if socket.assigns.http_pull_consumer.id do
        update_http_pull_consumer(socket, params)
      else
        create_http_pull_consumer(socket, params)
      end

    socket = assign(socket, :show_errors?, true)
    {:noreply, socket}
  end

  def handle_event("delete_submitted", _params, socket) do
    consumer = socket.assigns.http_pull_consumer

    case Consumers.delete_consumer_with_lifecycle(consumer) do
      {:ok, _} ->
        socket =
          socket
          |> put_flash(:info, "HTTP Pull Consumer deleted successfully")
          |> push_navigate(to: ~p"/consumers")

        {:noreply, socket}

      {:error, _} ->
        socket = put_flash(socket, :error, "Failed to delete HTTP Pull Consumer")
        {:noreply, socket}
    end
  end

  def handle_event("form_closed", _params, socket) do
    socket = push_patch(socket, to: ~p"/consumers")
    {:noreply, socket}
  end

  defp decode_params(form) do
    form
  end

  defp encode_http_pull_consumer(%HttpPullConsumer{} = http_pull_consumer) do
    %{
      "id" => http_pull_consumer.id,
      "name" => http_pull_consumer.name,
      "ack_wait_s" => div(http_pull_consumer.ack_wait_ms, 1000),
      "max_ack_pending" => http_pull_consumer.max_ack_pending,
      "max_deliver" => http_pull_consumer.max_deliver,
      "max_waiting" => http_pull_consumer.max_waiting,
      "message_kind" => http_pull_consumer.message_kind,
      "status" => http_pull_consumer.status
    }
  end

  defp encode_errors(%Ecto.Changeset{} = changeset) do
    Ecto.Changeset.traverse_errors(changeset, fn {msg, opts} ->
      Enum.reduce(opts, msg, fn {key, value}, acc ->
        String.replace(acc, "%{#{key}}", to_string(value))
      end)
    end)
  end

  defp update_http_pull_consumer(socket, params) do
    consumer = socket.assigns.http_pull_consumer

    case Consumers.update_consumer_with_lifecycle(consumer, params) do
      {:ok, http_pull_consumer} ->
        socket.assigns.on_finish.(http_pull_consumer)
        assign(socket, :http_pull_consumer, http_pull_consumer)

      {:error, %Ecto.Changeset{} = changeset} ->
        assign(socket, :changeset, changeset)
    end
  end

  defp create_http_pull_consumer(socket, params) do
    account_id = current_account_id(socket)

    case Consumers.create_http_pull_consumer_for_account_with_lifecycle(account_id, params) do
      {:ok, http_pull_consumer} ->
        socket.assigns.on_finish.(http_pull_consumer)
        push_navigate(socket, to: ~p"/consumers/#{http_pull_consumer.id}")

      {:error, %Ecto.Changeset{} = changeset} ->
        assign(socket, :changeset, changeset)
    end
  end

  defp reset_changeset(socket) do
    account_id = current_account_id(socket)

    changeset =
      if socket.assigns.http_pull_consumer.id do
        HttpPullConsumer.update_changeset(socket.assigns.http_pull_consumer, %{})
      else
        HttpPullConsumer.create_changeset(%HttpPullConsumer{account_id: account_id}, %{})
      end

    assign(socket, :changeset, changeset)
  end

  defp merge_changeset(socket, params) do
    account_id = current_account_id(socket)

    changeset =
      if socket.assigns.http_pull_consumer.id do
        HttpPullConsumer.update_changeset(socket.assigns.http_pull_consumer, params)
      else
        HttpPullConsumer.create_changeset(%HttpPullConsumer{account_id: account_id}, params)
      end

    assign(socket, :changeset, changeset)
  end
end

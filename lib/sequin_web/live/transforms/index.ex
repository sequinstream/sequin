defmodule SequinWeb.TransformsLive.Index do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Consumers

  require Logger

  @impl Phoenix.LiveView
  def mount(_params, _session, socket) do
    account_id = current_account_id(socket)
    transforms = Consumers.list_transforms_for_account(account_id)

    {:ok, assign(socket, :transforms, transforms)}
  end

  @impl Phoenix.LiveView
  def render(assigns) do
    assigns = assign(assigns, :encoded_transforms, Enum.map(assigns.transforms, &encode_transform/1))

    ~H"""
    <div id="transforms-index">
      <.svelte
        name="transforms/Index"
        props={
          %{
            transforms: @encoded_transforms,
            parent: "transforms-index"
          }
        }
        socket={@socket}
      />
    </div>
    """
  end

  defp encode_transform(transform) do
    account_id = transform.account_id
    consumers = Consumers.list_consumers_for_transform(account_id, transform.id)
    consumer_count = length(consumers)
    consumer_names = Enum.map(consumers, &%{name: &1.name})

    %{
      id: transform.id,
      name: transform.name,
      type: transform.type,
      insertedAt: transform.inserted_at,
      updatedAt: transform.updated_at,
      consumerCount: consumer_count,
      consumers: consumer_names
    }
  end

  # defp truncate(string, length \\ 512) do
  #   if String.length(string) > length do
  #     String.slice(string, 0, length) <> "..."
  #   else
  #     string
  #   end
  # end

  @impl Phoenix.LiveView
  def handle_event("delete", %{"id" => id}, socket) do
    case Consumers.delete_transform(current_account_id(socket), id) do
      {:ok, _} ->
        {:noreply,
         socket
         |> put_flash(:toast, %{kind: :info, title: "Function deleted"})
         |> assign(:transforms, Consumers.list_transforms_for_account(current_account_id(socket)))}

      {:error, error} ->
        Logger.error("[Transform.Index] Failed to delete transform", error: error)
        {:noreply, put_flash(socket, :toast, %{kind: :error, title: "Failed to delete function"})}
    end
  end
end

defmodule SequinWeb.TransformsLive.Index do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Consumers

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
            transforms: @encoded_transforms
          }
        }
        socket={@socket}
      />
    </div>
    """
  end

  defp encode_transform(transform) do
    %{
      id: transform.id,
      name: transform.name,
      type: transform.type,
      snippet:
        case transform.type do
          "path" -> truncate(transform.transform.path, 40)
        end,
      insertedAt: transform.inserted_at,
      updatedAt: transform.updated_at
    }
  end

  defp truncate(string, length) do
    if String.length(string) > length do
      String.slice(string, 0, length) <> "..."
    else
      string
    end
  end
end

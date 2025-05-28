defmodule SequinWeb.FunctionsLive.Index do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Consumers

  require Logger

  @impl Phoenix.LiveView
  def mount(_params, _session, socket) do
    account_id = current_account_id(socket)
    functions = Consumers.list_functions_for_account(account_id)

    {:ok, assign(socket, :functions, functions)}
  end

  @impl Phoenix.LiveView
  def render(assigns) do
    assigns = assign(assigns, :encoded_functions, Enum.map(assigns.functions, &encode_function/1))

    ~H"""
    <div id="functions-index">
      <.svelte
        name="functions/Index"
        props={
          %{
            functions: @encoded_functions,
            parent: "functions-index"
          }
        }
        socket={@socket}
      />
    </div>
    """
  end

  defp encode_function(function) do
    account_id = function.account_id
    consumers = Consumers.list_consumers_for_function(account_id, function.id)
    consumer_count = length(consumers)
    consumer_names = Enum.map(consumers, &%{name: &1.name})

    %{
      id: function.id,
      name: function.name,
      type: function.type,
      insertedAt: function.inserted_at,
      updatedAt: function.updated_at,
      consumerCount: consumer_count,
      consumers: consumer_names
    }
  end

  @impl Phoenix.LiveView
  def handle_event("delete", %{"id" => id}, socket) do
    case Consumers.delete_function(current_account_id(socket), id) do
      {:ok, _} ->
        {:noreply,
         socket
         |> put_flash(:toast, %{kind: :info, title: "Function deleted"})
         |> assign(:functions, Consumers.list_functions_for_account(current_account_id(socket)))}

      {:error, error} ->
        Logger.error("[Function.Index] Failed to delete function", error: error)
        {:noreply, put_flash(socket, :toast, %{kind: :error, title: "Failed to delete function"})}
    end
  end
end

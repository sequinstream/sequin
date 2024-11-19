defmodule SequinWeb.RedirectLive do
  @moduledoc false
  use SequinWeb, :live_view

  @impl Phoenix.LiveView
  def mount(_params, _session, socket) do
    {:ok, socket}
  end

  @impl Phoenix.LiveView
  def handle_params(_params, _url, socket) do
    new_path = redirect_path(socket.assigns.current_path)
    {:noreply, push_navigate(socket, to: new_path)}
  end

  defp redirect_path(path) do
    case path do
      "/consumers" -> "/"
      "/sinks" -> "/"
    end
  end
end

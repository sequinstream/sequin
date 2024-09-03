defmodule SequinWeb.LiveHooks do
  @moduledoc false
  import Phoenix.Component, only: [assign: 3]
  import Phoenix.LiveView, only: [attach_hook: 4]

  alias SequinWeb.LiveHelpers

  def on_mount(:global, _params, _session, socket) do
    socket =
      socket
      |> attach_hook(:assign_current_path, :handle_params, &assign_current_path/3)
      |> attach_hook(:push_toast, :after_render, &push_toast/1)
      |> attach_hook(:assign_account_name, :handle_params, &assign_account_name/3)

    {:cont, socket}
  end

  defp assign_current_path(_params, uri, socket) do
    uri = URI.parse(uri)
    {:cont, assign(socket, :current_path, uri.path)}
  end

  defp assign_account_name(_params, _uri, socket) do
    account = LiveHelpers.current_account(socket)
    {:cont, assign(socket, :account_name, account.name)}
  end

  defp push_toast(socket) do
    if toast = Phoenix.Flash.get(socket.assigns.flash, :toast) do
      Phoenix.LiveView.push_event(socket, "toast", toast)
    else
      socket
    end
  end
end

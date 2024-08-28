defmodule SequinWeb.UserLogoutLive do
  @moduledoc false
  use SequinWeb, :live_view

  @impl Phoenix.LiveView
  def mount(_params, _session, socket) do
    {:ok, socket, layout: {SequinWeb.Layouts, :app_no_sidenav}}
  end

  @impl Phoenix.LiveView
  def render(assigns) do
    ~H"""
    <div class="flex items-center justify-center h-[80vh]">
      <div class="mx-auto max-w-sm">
        <.header class="text-center">
          Logging out...
        </.header>
        <.form
          for={%{}}
          as={:user}
          action={~p"/logout"}
          method="delete"
          phx-submit="logout"
          phx-trigger-action={true}
        >
        </.form>
      </div>
    </div>
    """
  end
end

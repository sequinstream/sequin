defmodule SequinWeb.HomeLive do
  @moduledoc false
  use SequinWeb, :live_view

  @impl Phoenix.LiveView
  def mount(_params, _session, socket) do
    if Application.get_env(:sequin, :self_hosted) do
      {:ok, push_navigate(socket, to: "/setup")}
    else
      {:ok, push_navigate(socket, to: ~p"/consumers")}
    end
  end
end

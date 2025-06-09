defmodule SequinWeb.HomeLive do
  @moduledoc false
  use SequinWeb, :live_view

  @impl Phoenix.LiveView
  def mount(_params, _session, socket) do
    if Application.get_env(:sequin, :self_hosted) do
      if Sequin.Accounts.any_accounts?() do
        {:ok, push_navigate(socket, to: ~p"/sinks")}
      else
        {:ok, push_navigate(socket, to: "/setup")}
      end
    else
      {:ok, push_navigate(socket, to: ~p"/sinks")}
    end
  end
end

defmodule SequinWeb.HomeLive do
  @moduledoc false
  use SequinWeb, :live_view

  @impl Phoenix.LiveView
  def mount(_params, _session, socket) do
    if Application.get_env(:sequin, :self_hosted) do
      cond do
        Sequin.Accounts.any_accounts?() ->
          {:ok, push_navigate(socket, to: ~p"/sinks")}

        # Some of these routes have to be strings because they are only defined when self_hosted
        Sequin.Consumers.any_unmigrated_consumers?() ->
          {:ok, push_navigate(socket, to: "/migration-oct-2024")}

        true ->
          {:ok, push_navigate(socket, to: "/setup")}
      end
    else
      {:ok, push_navigate(socket, to: ~p"/sinks")}
    end
  end
end

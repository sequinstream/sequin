defmodule SequinWeb.LocalTunnelController do
  use SequinWeb, :controller

  alias Sequin.Accounts
  alias SequinWeb.ApiFallbackPlug

  action_fallback ApiFallbackPlug

  def index(conn, _params) do
    account_id = conn.assigns.account_id

    render(conn, "index.json", tunnels: Accounts.list_local_tunnels_for_account(account_id))
  end
end

defmodule SequinWeb.LocalTunnelController do
  use SequinWeb, :controller

  alias Sequin.Accounts
  alias Sequin.Consumers
  alias Sequin.Databases
  alias SequinWeb.ApiFallbackPlug

  action_fallback ApiFallbackPlug

  def index(conn, _params) do
    account_id = conn.assigns.account_id

    http_endpoints = Consumers.list_local_tunnel_http_endpoints_for_account(account_id)
    databases = Databases.list_local_tunnel_dbs_for_account(account_id)
    allocated_bastion_ports = Accounts.list_allocated_bastion_ports_for_account(account_id)

    render(conn, "index.json", entities: http_endpoints ++ databases ++ allocated_bastion_ports)
  end
end

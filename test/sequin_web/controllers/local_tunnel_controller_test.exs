defmodule SequinWeb.LocalTunnelControllerTest do
  use SequinWeb.ConnCase, async: true

  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory

  setup :authenticated_conn

  setup %{account: account} do
    tunnel = AccountsFactory.insert_local_tunnel!(account_id: account.id)

    %{tunnel: tunnel}
  end

  describe "index" do
    test "lists local tunnels in the given account", %{
      conn: conn,
      tunnel: tunnel1,
      account: account
    } do
      tunnel2 = AccountsFactory.insert_local_tunnel!(account_id: account.id)
      other_account = AccountsFactory.insert_account!()
      other_tunnel = AccountsFactory.insert_local_tunnel!(account_id: other_account.id)

      conn = get(conn, ~p"/api/local_tunnels")
      assert %{"data" => local_tunnels} = json_response(conn, 200)
      assert length(local_tunnels) == 2

      tunnel_ids = Enum.map(local_tunnels, & &1["id"])
      assert tunnel1.id in tunnel_ids
      assert tunnel2.id in tunnel_ids
      refute other_tunnel.id in tunnel_ids
    end

    test "if an http_endpoint belongs to a local tunnel, lists its name and id", %{
      conn: conn,
      tunnel: tunnel,
      account: account
    } do
      http_endpoint = ConsumersFactory.insert_http_endpoint!(account_id: account.id, local_tunnel_id: tunnel.id)

      conn = get(conn, ~p"/api/local_tunnels")
      assert %{"data" => [tunnel]} = json_response(conn, 200)

      assert tunnel["entity_id"] == http_endpoint.id
      assert tunnel["entity_name"] == http_endpoint.name
    end

    test "if a postgres_database belongs to a local tunnel, lists its name and id", %{
      conn: conn,
      tunnel: tunnel,
      account: account
    } do
      postgres_database = DatabasesFactory.insert_postgres_database!(account_id: account.id, local_tunnel_id: tunnel.id)

      conn = get(conn, ~p"/api/local_tunnels")
      assert %{"data" => [tunnel]} = json_response(conn, 200)

      assert tunnel["entity_id"] == postgres_database.id
      assert tunnel["entity_name"] == postgres_database.name
    end
  end
end

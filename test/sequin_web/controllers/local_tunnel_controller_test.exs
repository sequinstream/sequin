defmodule SequinWeb.LocalTunnelControllerTest do
  use SequinWeb.ConnCase, async: true

  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory

  setup :authenticated_conn

  describe "index" do
    test "lists entities using local tunnels in the given account", %{
      conn: conn,
      account: account
    } do
      http_endpoint = ConsumersFactory.insert_http_endpoint!(account_id: account.id, use_local_tunnel: true)
      postgres_database = DatabasesFactory.insert_postgres_database!(account_id: account.id, use_local_tunnel: true)

      # Create entities that should not be included
      ConsumersFactory.insert_http_endpoint!(account_id: account.id, use_local_tunnel: false)
      DatabasesFactory.insert_postgres_database!(account_id: account.id, use_local_tunnel: false)

      other_account = AccountsFactory.insert_account!()
      ConsumersFactory.insert_http_endpoint!(account_id: other_account.id, use_local_tunnel: true)

      conn = get(conn, ~p"/api/local_tunnels")
      assert %{"data" => entities} = json_response(conn, 200)
      assert length(entities) == 2

      entity_ids = Enum.map(entities, & &1["entity_id"])
      assert http_endpoint.id in entity_ids
      assert postgres_database.id in entity_ids
    end

    test "includes correct information for http_endpoints", %{
      conn: conn,
      account: account
    } do
      http_endpoint =
        ConsumersFactory.insert_http_endpoint!(
          account_id: account.id,
          use_local_tunnel: true,
          port: nil
        )

      conn = get(conn, ~p"/api/local_tunnels")
      assert %{"data" => [entity]} = json_response(conn, 200)

      assert entity["entity_id"] == http_endpoint.id
      assert entity["bastion_port"] == http_endpoint.port
    end

    test "includes correct information for postgres_databases", %{
      conn: conn,
      account: account
    } do
      postgres_database =
        DatabasesFactory.insert_postgres_database!(
          account_id: account.id,
          use_local_tunnel: true,
          port: nil
        )

      conn = get(conn, ~p"/api/local_tunnels")
      assert %{"data" => [entity]} = json_response(conn, 200)

      assert entity["entity_id"] == postgres_database.id
      assert entity["bastion_port"] == postgres_database.port
    end
  end
end

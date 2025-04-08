defmodule SequinWeb.HttpEndpointControllerTest do
  use SequinWeb.ConnCase, async: true

  alias Sequin.Consumers
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory

  setup :authenticated_conn

  setup %{account: account} do
    other_account = AccountsFactory.insert_account!()
    http_endpoint = ConsumersFactory.insert_http_endpoint!(account_id: account.id)

    other_http_endpoint =
      ConsumersFactory.insert_http_endpoint!(account_id: other_account.id)

    %{http_endpoint: http_endpoint, other_http_endpoint: other_http_endpoint, other_account: other_account}
  end

  describe "index" do
    test "lists http endpoints in the given account", %{
      conn: conn,
      account: account,
      http_endpoint: http_endpoint
    } do
      another_endpoint = ConsumersFactory.insert_http_endpoint!(account_id: account.id)

      conn = get(conn, ~p"/api/destinations/http_endpoints")
      assert %{"data" => endpoints} = json_response(conn, 200)
      assert length(endpoints) == 2
      atomized_endpoints = Enum.map(endpoints, &Sequin.Map.atomize_keys/1)
      assert_lists_equal([http_endpoint, another_endpoint], atomized_endpoints, &(&1.id == &2.id))
    end

    test "does not list http endpoints from another account", %{
      conn: conn,
      other_http_endpoint: other_http_endpoint
    } do
      conn = get(conn, ~p"/api/destinations/http_endpoints")
      assert %{"data" => endpoints} = json_response(conn, 200)
      refute Enum.any?(endpoints, &(&1["id"] == other_http_endpoint.id))
    end
  end

  describe "show" do
    test "shows http endpoint details", %{conn: conn, http_endpoint: http_endpoint} do
      conn = get(conn, ~p"/api/destinations/http_endpoints/#{http_endpoint.id}")
      assert http_endpoint_json = json_response(conn, 200)

      assert http_endpoint.name == http_endpoint_json["name"]
    end

    test "returns 404 if http endpoint belongs to another account", %{
      conn: conn,
      other_http_endpoint: other_http_endpoint
    } do
      conn = get(conn, ~p"/api/destinations/http_endpoints/#{other_http_endpoint.id}")
      assert json_response(conn, 404)
    end
  end

  describe "create" do
    test "creates a http endpoint under the authenticated account", %{
      conn: conn,
      account: account
    } do
      http_endpoint_attrs = %{
        name: "ea_1733",
        url: "https://enim.com/iusto",
        headers: %{"Content-Type" => "application/json"},
        encrypted_headers: %{"Authorization" => "Bearer iusto"}
      }

      conn = post(conn, ~p"/api/destinations/http_endpoints", http_endpoint_attrs)
      assert %{"name" => name} = json_response(conn, 200)

      {:ok, http_endpoint} = Consumers.find_http_endpoint_for_account(account.id, name: name)
      assert http_endpoint.account_id == account.id
    end

    test "returns validation error for invalid attributes", %{conn: conn} do
      invalid_attrs = %{name: nil}
      conn = post(conn, ~p"/api/destinations/http_endpoints", invalid_attrs)
      assert json_response(conn, 422)["errors"] != %{}
    end

    test "ignores provided account_id and uses authenticated account", %{
      conn: conn,
      account: account,
      other_account: other_account
    } do
      http_endpoint_attrs = %{
        name: "ea_1733",
        url: "https://enim.com/iusto",
        headers: %{"Content-Type" => "application/json"},
        encrypted_headers: %{"Authorization" => "Bearer iusto"},
        account_id: other_account.id
      }

      conn = post(conn, ~p"/api/destinations/http_endpoints", http_endpoint_attrs)

      assert %{"id" => id} = json_response(conn, 200)

      {:ok, http_endpoint} = Consumers.find_http_endpoint_for_account(account.id, id: id)
      assert http_endpoint.account_id == account.id
      assert http_endpoint.account_id != other_account.id
    end
  end

  describe "update" do
    test "updates the http endpoint with valid attributes", %{conn: conn, http_endpoint: http_endpoint} do
      name = http_endpoint.name
      {:ok, _} = Consumers.update_http_endpoint(http_endpoint, %{name: "some-old-name"})
      update_attrs = %{name: name}
      conn = put(conn, ~p"/api/destinations/http_endpoints/#{http_endpoint.id}", update_attrs)
      assert %{"id" => id} = json_response(conn, 200)

      {:ok, updated_endpoint} = Consumers.find_http_endpoint_for_account(http_endpoint.account_id, id: id)
      assert updated_endpoint.name == name
    end

    test "returns validation error for invalid attributes", %{conn: conn, http_endpoint: http_endpoint} do
      invalid_attrs = %{url: "not a url !"}
      conn = put(conn, ~p"/api/destinations/http_endpoints/#{http_endpoint.id}", invalid_attrs)
      assert json_response(conn, 422)["errors"] != %{}
    end

    test "returns 404 if http endpoint belongs to another account", %{
      conn: conn,
      other_http_endpoint: other_http_endpoint
    } do
      conn = put(conn, ~p"/api/destinations/http_endpoints/#{other_http_endpoint.id}", %{name: "new-name"})
      assert json_response(conn, 404)
    end
  end

  describe "delete" do
    test "deletes the http endpoint", %{conn: conn, http_endpoint: http_endpoint} do
      conn = delete(conn, ~p"/api/destinations/http_endpoints/#{http_endpoint.id}")
      assert %{"id" => id, "deleted" => true} = json_response(conn, 200)

      assert {:error, _} = Consumers.find_http_endpoint_for_account(http_endpoint.account_id, id: id)
    end

    test "returns 404 if http endpoint belongs to another account", %{
      conn: conn,
      other_http_endpoint: other_http_endpoint
    } do
      conn = delete(conn, ~p"/api/destinations/http_endpoints/#{other_http_endpoint.id}")
      assert json_response(conn, 404)
    end
  end
end

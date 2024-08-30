defmodule SequinWeb.ApiKeyControllerTest do
  use SequinWeb.ConnCase, async: true

  alias Sequin.ApiTokens
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ApiTokensFactory

  setup :authenticated_conn

  setup %{account: account} do
    other_account = AccountsFactory.insert_account!()
    api_key = ApiTokensFactory.insert_token!(account_id: account.id)
    other_api_key = ApiTokensFactory.insert_token!(account_id: other_account.id)
    %{api_key: api_key, other_api_key: other_api_key, other_account: other_account}
  end

  describe "index" do
    test "lists api keys in the given account", %{
      conn: conn,
      api_key: api_key,
      other_api_key: other_api_key
    } do
      conn = get(conn, ~p"/api/api_keys")
      assert %{"data" => api_keys} = json_response(conn, 200)

      # We also have an ApiToken inserted during authenitcated conn test setup, hence 2
      assert length(api_keys) == 2

      atomized_api_keys = Enum.map(api_keys, &Sequin.Map.atomize_keys/1)
      assert api_key.id in Enum.map(atomized_api_keys, & &1.id)
      refute other_api_key.id in Enum.map(atomized_api_keys, & &1.id)
    end

    test "does not list api keys from another account", %{conn: conn, other_api_key: other_api_key} do
      conn = get(conn, ~p"/api/api_keys")
      assert %{"data" => api_keys} = json_response(conn, 200)
      refute Enum.any?(api_keys, &(&1["id"] == other_api_key.id))
    end
  end

  describe "create" do
    test "creates an api key under the authenticated account", %{conn: conn, account: account} do
      attrs = ApiTokensFactory.token_attrs()
      conn = post(conn, ~p"/api/api_keys", attrs)
      assert %{"id" => id} = json_response(conn, 200)

      {:ok, api_key} = ApiTokens.get_token_by(id: id)
      assert api_key.account_id == account.id
      assert api_key.name == attrs[:name]
    end

    test "returns validation error for invalid attributes", %{conn: conn} do
      invalid_attrs = %{name: nil}
      conn = post(conn, ~p"/api/api_keys", invalid_attrs)
      assert json_response(conn, 422)["errors"] != %{}
    end
  end

  describe "delete" do
    test "deletes the api key", %{conn: conn, api_key: api_key} do
      conn = delete(conn, ~p"/api/api_keys/#{api_key.id}")
      assert json_response(conn, 200) == %{"success" => true}

      assert {:error, _} = ApiTokens.get_token_by(id: api_key.id)
    end

    test "returns 404 if api key belongs to another account", %{conn: conn, other_api_key: other_api_key} do
      conn = delete(conn, ~p"/api/api_keys/#{other_api_key.id}")
      assert json_response(conn, 404)
    end
  end
end

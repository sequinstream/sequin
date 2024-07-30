defmodule SequinWeb.WebhookControllerTest do
  use SequinWeb.ConnCase, async: true

  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.SourcesFactory
  alias Sequin.Sources

  setup :authenticated_conn

  setup %{account: _account} do
    other_account = AccountsFactory.insert_account!()
    %{other_account: other_account}
  end

  describe "index" do
    test "lists webhooks for the authenticated account", %{conn: conn, account: account} do
      webhook1 = SourcesFactory.insert_webhook!(account_id: account.id)
      webhook2 = SourcesFactory.insert_webhook!(account_id: account.id)

      conn = get(conn, ~p"/api/webhooks")
      assert %{"data" => webhooks} = json_response(conn, 200)
      assert length(webhooks) == 2
      atomized_webhooks = Enum.map(webhooks, &Sequin.Map.atomize_keys/1)
      assert_lists_equal([webhook1, webhook2], atomized_webhooks, &(&1.id == &2.id))
    end

    test "does not list webhooks from another account", %{conn: conn, other_account: other_account} do
      SourcesFactory.insert_webhook!(account_id: other_account.id)

      conn = get(conn, ~p"/api/webhooks")
      assert %{"data" => webhooks} = json_response(conn, 200)
      assert Enum.empty?(webhooks)
    end
  end

  describe "show" do
    test "shows webhook details", %{conn: conn, account: account} do
      webhook = SourcesFactory.insert_webhook!(account_id: account.id)

      conn = get(conn, ~p"/api/webhooks/#{webhook.id}")
      assert json_response = json_response(conn, 200)
      atomized_response = Sequin.Map.atomize_keys(json_response)

      assert_maps_equal(webhook, atomized_response, [:id, :name, :stream_id, :auth_strategy])
    end

    test "shows webhook details by name", %{conn: conn, account: account} do
      webhook = SourcesFactory.insert_webhook!(account_id: account.id)

      conn = get(conn, ~p"/api/webhooks/#{webhook.name}")
      assert json_response = json_response(conn, 200)
      assert json_response["id"] == webhook.id
      assert json_response["name"] == webhook.name
      assert json_response["auth_strategy"] == webhook.auth_strategy
    end

    test "returns 404 if webhook belongs to another account", %{conn: conn, other_account: other_account} do
      webhook = SourcesFactory.insert_webhook!(account_id: other_account.id)

      conn = get(conn, ~p"/api/webhooks/#{webhook.id}")
      assert json_response(conn, 404)
    end
  end

  describe "create" do
    test "creates a webhook under the authenticated account", %{conn: conn, account: account} do
      stream = SourcesFactory.insert_stream!(account_id: account.id)
      attrs = SourcesFactory.webhook_attrs(stream_id: stream.id)

      conn = post(conn, ~p"/api/webhooks", attrs)
      assert %{"id" => id} = json_response(conn, 201)

      webhook = Sources.get_webhook!(id)
      assert webhook.account_id == account.id
      assert webhook.stream_id == stream.id
    end

    test "returns validation error for invalid attributes", %{conn: conn} do
      invalid_attrs = %{name: nil}

      conn = post(conn, ~p"/api/webhooks", invalid_attrs)
      assert json_response(conn, 422)["errors"] != %{}
    end

    test "ignores provided account_id and uses authenticated account", %{
      conn: conn,
      account: account,
      other_account: other_account
    } do
      stream = SourcesFactory.insert_stream!(account_id: account.id)
      attrs = SourcesFactory.webhook_attrs(account_id: other_account.id, stream_id: stream.id)

      conn = post(conn, ~p"/api/webhooks", attrs)
      assert %{"id" => id} = json_response(conn, 201)

      webhook = Sources.get_webhook!(id)
      assert webhook.account_id == account.id
      assert webhook.account_id != other_account.id
    end

    test "creates a webhook with HMAC auth strategy", %{conn: conn, account: account} do
      stream = SourcesFactory.insert_stream!(account_id: account.id)

      attrs =
        SourcesFactory.webhook_attrs(
          stream_id: stream.id,
          auth_strategy: %{"type" => "hmac", "header_name" => "X-HMAC-Signature", "secret" => "some-secret"}
        )

      conn = post(conn, ~p"/api/webhooks", attrs)
      assert %{"id" => id} = json_response(conn, 201)

      webhook = Sources.get_webhook!(id)
      assert webhook.account_id == account.id
      assert webhook.stream_id == stream.id
      assert webhook.auth_strategy == %{"type" => "hmac", "header_name" => "X-HMAC-Signature", "secret" => "some-secret"}
    end
  end

  describe "update" do
    setup %{account: account} do
      webhook = SourcesFactory.insert_webhook!(account_id: account.id)
      %{webhook: webhook}
    end

    test "updates the webhook with valid attributes", %{conn: conn, webhook: webhook} do
      attrs = %{name: "updated-name"}
      conn = put(conn, ~p"/api/webhooks/#{webhook.id}", attrs)
      assert %{"id" => id} = json_response(conn, 200)

      updated_webhook = Sources.get_webhook!(id)
      assert updated_webhook.name == "updated-name"
    end

    test "returns validation error for invalid attributes", %{conn: conn, webhook: webhook} do
      invalid_attrs = %{name: nil}
      conn = put(conn, ~p"/api/webhooks/#{webhook.id}", invalid_attrs)
      assert json_response(conn, 422)["errors"] != %{}
    end

    test "returns 404 if webhook belongs to another account", %{conn: conn, other_account: other_account} do
      other_webhook = SourcesFactory.insert_webhook!(account_id: other_account.id)

      conn = put(conn, ~p"/api/webhooks/#{other_webhook.id}", %{name: "new-name"})
      assert json_response(conn, 404)
    end

    test "ignores account_id if provided", %{
      conn: conn,
      webhook: webhook,
      other_account: other_account
    } do
      attrs = %{account_id: other_account.id, name: "new-name"}

      conn = put(conn, ~p"/api/webhooks/#{webhook.id}", attrs)
      assert %{"id" => id} = json_response(conn, 200)

      updated_webhook = Sources.get_webhook!(id)
      assert updated_webhook.account_id == webhook.account_id
      assert updated_webhook.account_id != other_account.id
      assert updated_webhook.name == "new-name"
    end
  end

  describe "delete" do
    test "deletes the webhook", %{conn: conn, account: account} do
      webhook = SourcesFactory.insert_webhook!(account_id: account.id)

      conn = delete(conn, ~p"/api/webhooks/#{webhook.id}")
      assert %{"id" => id, "deleted" => true} = json_response(conn, 200)

      assert_raise Sequin.Error.NotFoundError, fn -> Sources.get_webhook!(id) end
    end

    test "returns 404 if webhook belongs to another account", %{conn: conn, other_account: other_account} do
      other_webhook = SourcesFactory.insert_webhook!(account_id: other_account.id)

      conn = delete(conn, ~p"/api/webhooks/#{other_webhook.id}")
      assert json_response(conn, 404)
    end
  end
end

defmodule SequinWeb.Plugs.VerifyApiTokenTest do
  use SequinWeb.ConnCase, async: true

  alias Sequin.ApiTokens.ApiToken
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ApiTokensFactory
  alias SequinWeb.Plugs.VerifyApiToken

  test "sets the org_id when the token is valid" do
    account = AccountsFactory.insert_account!()

    %ApiToken{} = token = ApiTokensFactory.insert_token!(account_id: account.id)

    conn = build_conn()
    conn = put_req_header(conn, "authorization", "Bearer #{token.token}")

    conn = VerifyApiToken.call(conn, [])

    assert conn.assigns[:account_id] == account.id
  end

  @tag capture_log: true
  test "returns unauthorized when the token is invalid" do
    conn = build_conn()
    conn = put_req_header(conn, "authorization", "invalid")

    conn = VerifyApiToken.call(conn, [])

    assert conn.status == 401
  end

  @tag capture_log: true
  test "returns unauthorized when the token is missing" do
    conn = build_conn()

    conn = VerifyApiToken.call(conn, [])

    assert conn.status == 401
  end
end

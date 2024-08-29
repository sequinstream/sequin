defmodule Sequin.ApiTokensTest do
  use Sequin.DataCase, async: true

  alias Sequin.ApiTokens
  alias Sequin.ApiTokens.ApiToken
  alias Sequin.Error.NotFoundError
  alias Sequin.Factory
  alias Sequin.Factory.AccountsFactory

  test "creates and finds a token by the hashed token" do
    account = AccountsFactory.insert_account!()

    %ApiToken{} = token = ApiTokens.create_for_account!(account.id, %{name: Factory.word()})

    assert {:ok, token} = ApiTokens.find_by_token(token.token)

    assert token.id == token.id
    assert token.token == token.token
    assert token.account_id == token.account_id
    assert token.inserted_at == token.inserted_at
  end

  test "returns not found when the token is not found" do
    assert {:error, %NotFoundError{}} = ApiTokens.find_by_token("not-found")
  end
end

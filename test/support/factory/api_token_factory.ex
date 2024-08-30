defmodule Sequin.Factory.ApiTokensFactory do
  @moduledoc false
  import Sequin.Factory.Support

  alias Sequin.ApiTokens.ApiToken
  alias Sequin.Factory
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Repo

  def token_attrs(attrs \\ []) do
    attrs
    |> token()
    |> Sequin.Map.from_ecto()
  end

  def token(attrs \\ []) do
    attrs = Map.new(attrs)

    {account_id, attrs} = Map.pop_lazy(attrs, :account_id, fn -> AccountsFactory.insert_account!().id end)

    token = ApiToken.build_token(account_id)

    merge_attributes(
      %ApiToken{
        account_id: account_id,
        name: Factory.word(),
        token: token.token,
        hashed_token: token.hashed_token
      },
      attrs
    )
  end

  def insert_token!(attrs \\ []) do
    attrs
    |> Map.new()
    |> Map.put_new_lazy(:account_id, fn -> AccountsFactory.insert_account!().id end)
    |> token()
    |> Repo.insert!()
  end
end

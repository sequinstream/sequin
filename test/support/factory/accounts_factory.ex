defmodule Sequin.Factory.AccountsFactory do
  @moduledoc false
  import Sequin.Factory.Support

  alias Sequin.Accounts.Account
  alias Sequin.Factory
  alias Sequin.Repo

  def account(attrs \\ []) do
    merge_attributes(
      %Account{
        inserted_at: Factory.timestamp(),
        updated_at: Factory.timestamp()
      },
      attrs
    )
  end

  def account_attrs(attrs \\ []) do
    attrs
    |> account()
    |> Sequin.Map.from_ecto()
  end

  def insert_account!(attrs \\ []) do
    attrs
    |> account()
    |> Repo.insert!()
  end
end

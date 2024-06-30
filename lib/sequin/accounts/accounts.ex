defmodule Sequin.Accounts do
  @moduledoc false
  alias Sequin.Accounts.Account
  alias Sequin.Error
  alias Sequin.Repo

  def get_account(id) do
    case Repo.get(Account, id) do
      nil -> {:error, Error.not_found(entity: :account)}
      account -> {:ok, account}
    end
  end

  def list_accounts, do: Repo.all(Account)

  def create_account(attrs) do
    %Account{}
    |> Account.changeset(attrs)
    |> Repo.insert()
  end
end

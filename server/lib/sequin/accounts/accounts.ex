defmodule Sequin.Accounts do
  @moduledoc false
  alias Sequin.Accounts.Account
  alias Sequin.Accounts.ApiKey
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

  def list_api_keys_for_account(account_id) do
    account_id
    |> ApiKey.where_account()
    |> Repo.all()
  end

  def create_api_key(account_id, attrs) do
    %ApiKey{account_id: account_id}
    |> ApiKey.changeset(attrs)
    |> Repo.insert()
  end

  def get_api_key_for_account(account_id, id) do
    case Repo.get_by(ApiKey, id: id, account_id: account_id) do
      nil -> {:error, Error.not_found(entity: :api_key)}
      api_key -> {:ok, api_key}
    end
  end

  def delete_api_key(%ApiKey{} = api_key) do
    Repo.delete(api_key)
  end
end

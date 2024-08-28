defmodule Sequin.Accounts do
  @moduledoc false
  alias Sequin.Accounts.Account
  alias Sequin.Accounts.ApiKey
  alias Sequin.Accounts.User
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

  # User functions

  @doc """
  Returns the list of users for a given account.
  """
  def list_users_for_account(account_id) do
    Repo.all(User.where_account_id(account_id))
  end

  @doc """
  Gets a single user.
  """
  def get_user(id) do
    case Repo.get(User, id) do
      nil -> {:error, Error.not_found(entity: :user)}
      user -> {:ok, user}
    end
  end

  @doc """
  Gets a single user by email.
  """
  def get_user_by_email(email) do
    case Repo.get_by(User, email: email) do
      nil -> {:error, Error.not_found(entity: :user)}
      user -> {:ok, user}
    end
  end

  @doc """
  Creates a user.
  """
  def create_user(attrs \\ %{}) do
    %User{}
    |> User.changeset(attrs)
    |> Repo.insert()
  end

  @doc """
  Updates a user.
  """
  def update_user(%User{} = user, attrs) do
    user
    |> User.changeset(attrs)
    |> Repo.update()
  end

  @doc """
  Deletes a user.
  """
  def delete_user(%User{} = user) do
    Repo.delete(user)
  end

  @doc """
  Returns an `%Ecto.Changeset{}` for tracking user changes.
  """
  def change_user(%User{} = user, attrs \\ %{}) do
    User.changeset(user, attrs)
  end
end

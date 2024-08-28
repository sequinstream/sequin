defmodule Sequin.Accounts do
  @moduledoc false
  alias Sequin.Accounts.Account
  alias Sequin.Accounts.ApiKey
  alias Sequin.Accounts.User
  alias Sequin.Consumers
  alias Sequin.Databases
  alias Sequin.Error
  alias Sequin.Replication
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

  def list_expired_temp_accounts do
    cutoff_time = DateTime.add(DateTime.utc_now(), -48, :hour)

    Account.where_temp()
    |> Account.where_inserted_before(cutoff_time)
    |> Repo.all()
  end

  def deprovision_account(%Account{is_temp: true} = account) do
    # Delete associated users
    account.id
    |> list_users_for_account()
    |> Enum.each(&delete_user/1)

    # Delete associated API keys
    account.id
    |> list_api_keys_for_account()
    |> Enum.each(&delete_api_key/1)

    # Delete associated HTTP push and pull consumers
    account.id
    |> Consumers.list_consumers_for_account()
    |> Enum.each(&Consumers.delete_consumer_with_lifecycle/1)

    # Delete associated HTTP endpoints
    account.id
    |> Consumers.list_http_endpoints_for_account()
    |> Enum.each(&Consumers.delete_http_endpoint/1)

    # Delete associated PostgresDatabases
    account.id
    |> Databases.list_dbs_for_account(:replication_slot)
    |> Enum.each(&Databases.delete_db_with_replication_slot/1)

    # Delete associated PostgresReplicationSlots
    account.id
    |> Replication.list_pg_replications_for_account()
    |> Enum.each(&Replication.delete_pg_replication_with_lifecycle/1)

    # Finally, delete the account
    Repo.delete(account)
  end

  # User functions

  @doc """
  Returns the list of users for a given account.
  """
  def list_users_for_account(account_id) do
    Repo.all(User.where_account_id(account_id))
  end

  def list_users do
    Repo.all(User)
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

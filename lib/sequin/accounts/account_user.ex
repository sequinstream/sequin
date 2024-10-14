defmodule Sequin.Accounts.AccountUser do
  @moduledoc """
  Schema and changeset for the accounts_users join table.
  """
  use Sequin.ConfigSchema

  import Ecto.Changeset
  import Ecto.Query, only: [from: 2]

  alias Sequin.Accounts.Account
  alias Sequin.Accounts.AccountUser
  alias Sequin.Accounts.User

  @derive {Jason.Encoder, only: [:id, :user_id, :account_id, :current, :inserted_at, :updated_at]}

  typed_schema "accounts_users" do
    belongs_to :user, User
    belongs_to :account, Account
    field :current, :boolean, default: false

    timestamps()
  end

  @doc """
  Changeset function for the AccountUser schema.
  """
  def changeset(account_user, attrs) do
    cast(account_user, attrs, [:current])
  end

  def verify_account_ownership_query(account, user) do
    from au in AccountUser,
      where: au.account_id == ^account.id and au.user_id == ^user.id,
      select: 1
  end

  def verify_account_user_query(account, email) do
    from au in AccountUser,
      join: u in assoc(au, :user),
      where: au.account_id == ^account.id and u.email == ^email,
      select: 1
  end
end

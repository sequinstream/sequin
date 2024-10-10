defmodule Sequin.Accounts.AccountUser do
  @moduledoc """
  Schema and changeset for the accounts_users join table.
  """
  use Sequin.ConfigSchema

  import Ecto.Changeset

  alias Sequin.Accounts.Account
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
end

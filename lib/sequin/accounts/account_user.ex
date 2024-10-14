defmodule Sequin.Accounts.AccountUser do
  @moduledoc """
  Schema and changeset for the accounts_users join table.
  """
  use Sequin.ConfigSchema

  import Ecto.Changeset
  import Ecto.Query

  alias Sequin.Accounts.Account
  alias Sequin.Accounts.User

  @derive {Jason.Encoder, only: [:id, :user_id, :account_id, :current, :inserted_at, :updated_at]}

  typed_schema "accounts_users" do
    belongs_to :user, User
    belongs_to :account, Account
    field :current, :boolean, default: false

    timestamps()
  end

  def where_account_id(query \\ base_query(), account_id) do
    from([account_user: au] in query, where: au.account_id == ^account_id)
  end

  def where_email(query \\ base_query(), email) do
    from([account_user: au] in query, join: u in assoc(au, :user), where: u.email == ^email)
  end

  defp base_query(query \\ __MODULE__) do
    from(au in query, as: :account_user)
  end

  @doc """
  Changeset function for the AccountUser schema.
  """
  def changeset(account_user, attrs) do
    cast(account_user, attrs, [:current])
  end
end

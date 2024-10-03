defmodule Sequin.Accounts.Account do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset
  import Ecto.Query

  alias Sequin.Accounts.Account
  alias Sequin.Accounts.AccountUser

  @derive {Jason.Encoder, only: [:id, :name, :inserted_at, :updated_at]}

  @type id :: String.t()

  typed_schema "accounts" do
    field :name, :string

    has_many :accounts_users, AccountUser
    has_many :users, through: [:accounts_users, :user]

    timestamps()
  end

  def changeset(%Account{} = account, attrs) do
    account
    |> cast(attrs, [:name])
    |> maybe_put_name()
    |> validate_required([:name])
    |> validate_length(:name, max: 80, message: "Account name cannot be longer than 80 characters")
    |> validate_account_name()
  end

  defp maybe_put_name(changeset) do
    if fetch_field!(changeset, :name) do
      changeset
    else
      put_change(changeset, :name, "Personal")
    end
  end

  defp validate_account_name(changeset) do
    name = get_field(changeset, :name)

    if is_nil(name) || String.trim(name) == "" do
      add_error(changeset, :name, "Account name cannot be blank")
    else
      changeset
    end
  end

  def where_id(query \\ base_query(), id) do
    from(a in query, where: a.id == ^id)
  end

  def where_user_id(query \\ base_query(), user_id) do
    from(a in query,
      join: au in AccountUser,
      on: au.account_id == a.id,
      where: au.user_id == ^user_id
    )
  end

  defp base_query(query \\ __MODULE__) do
    from(a in query, as: :account)
  end
end

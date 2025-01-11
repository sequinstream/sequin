defmodule Sequin.Accounts.Account do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset
  import Ecto.Query

  alias Sequin.Accounts.Account
  alias Sequin.Accounts.AccountUser

  @derive {Jason.Encoder, only: [:id, :name, :inserted_at, :updated_at, :contact_email]}

  @type id :: String.t()

  typed_schema "accounts" do
    field :name, :string
    field :features, {:array, :string}
    field :annotations, :map, default: %{}
    field :contact_email, :string

    has_many :accounts_users, AccountUser
    has_many :users, through: [:accounts_users, :user]

    timestamps()
  end

  def changeset(%Account{} = account, attrs) do
    account
    |> cast(attrs, [:name, :features, :annotations, :contact_email])
    |> maybe_put_name()
    |> validate_required([:name])
    |> validate_length(:name, max: 80)
    |> validate_format(:contact_email, ~r/^[^\s@]+@[^\s@]+\.[^\s@]+$/)
  end

  defp maybe_put_name(changeset) do
    if fetch_field!(changeset, :name) do
      changeset
    else
      put_change(changeset, :name, "Personal")
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

  def show_contact_email_alert?(%__MODULE__{} = account) do
    is_nil(account.contact_email) and not Map.get(account.annotations, "show_contact_email_alert_dismissed", false)
  end

  defp base_query(query \\ __MODULE__) do
    from(a in query, as: :account)
  end
end

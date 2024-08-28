defmodule Sequin.Accounts.User do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset
  import Ecto.Query, only: [from: 2]

  schema "users" do
    field :name, :string
    field :email, :string
    field :auth_provider, Ecto.Enum, values: [:github]
    field :auth_provider_id, :string

    belongs_to :account, Sequin.Accounts.Account

    timestamps()
  end

  @doc false
  def changeset(user, attrs) do
    user
    |> cast(attrs, [:name, :email, :account_id, :auth_provider, :auth_provider_id])
    |> validate_required([:email, :account_id, :auth_provider])
    |> validate_format(:email, ~r/^[^\s]+@[^\s]+$/, message: "must have the @ sign and no spaces")
    |> unique_constraint(:email)
    |> unique_constraint([:auth_provider, :auth_provider_id])
  end

  def where_account_id(query \\ base_query(), account_id) do
    from([user: u] in query, where: u.account_id == ^account_id)
  end

  defp base_query(query \\ __MODULE__) do
    from(u in query, as: :user)
  end
end

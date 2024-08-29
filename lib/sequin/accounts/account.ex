defmodule Sequin.Accounts.Account do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset
  # import Ecto.Query, only: [from: 2]

  alias Sequin.Accounts.Account
  alias Sequin.Name

  typed_schema "accounts" do
    field :name, :string

    timestamps()
  end

  def changeset(%Account{} = account, attrs) do
    account
    |> cast(attrs, [:name])
    |> maybe_put_name()
  end

  defp maybe_put_name(changeset) do
    if fetch_field!(changeset, :name) do
      changeset
    else
      put_change(changeset, :name, Name.generate())
    end
  end

  # defp base_query(query \\ __MODULE__) do
  #   from(a in query, as: :account)
  # end
end

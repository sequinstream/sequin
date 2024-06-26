defmodule SequinStream.Accounts.Account do
  use SequinStream.Schema

  alias SequinStream.Accounts.Account

  import Ecto.Changeset

  typed_schema "accounts" do
    timestamps()
  end

  def changeset(%Account{} = account, attrs) do
    account
    |> cast(attrs, [])
    |> validate_required([])
  end
end

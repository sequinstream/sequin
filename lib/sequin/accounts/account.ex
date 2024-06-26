defmodule Sequin.Accounts.Account do
  @moduledoc false
  use Sequin.Schema

  import Ecto.Changeset

  alias Sequin.Accounts.Account

  typed_schema "accounts" do
    timestamps()
  end

  def changeset(%Account{} = account, attrs) do
    account
    |> cast(attrs, [])
    |> validate_required([])
  end
end

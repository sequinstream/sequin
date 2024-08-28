defmodule Sequin.Accounts.Account do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset
  # import Ecto.Query, only: [from: 2]

  alias Sequin.Accounts.Account

  typed_schema "accounts" do
    timestamps()
  end

  def changeset(%Account{} = account, attrs) do
    cast(account, attrs, [])
  end

  # defp base_query(query \\ __MODULE__) do
  #   from(a in query, as: :account)
  # end
end

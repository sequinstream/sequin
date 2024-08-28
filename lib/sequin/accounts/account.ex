defmodule Sequin.Accounts.Account do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset
  import Ecto.Query, only: [from: 2]

  alias Sequin.Accounts.Account

  typed_schema "accounts" do
    field :is_temp, :boolean, null: false, default: true

    timestamps()
  end

  def changeset(%Account{} = account, attrs) do
    account
    |> cast(attrs, [:is_temp])
    |> validate_required([:is_temp])
  end

  def where_temp(query \\ base_query()) do
    from([account: a] in query, where: a.is_temp == true)
  end

  def where_inserted_before(query \\ base_query(), cutoff_time) do
    from([account: a] in query, where: a.inserted_at < ^cutoff_time)
  end

  defp base_query(query \\ __MODULE__) do
    from(a in query, as: :account)
  end
end

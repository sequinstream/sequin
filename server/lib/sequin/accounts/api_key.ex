defmodule Sequin.Accounts.ApiKey do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset
  import Ecto.Query

  alias Sequin.Accounts.Account

  @derive {Jason.Encoder, only: [:id, :name, :account_id, :inserted_at, :updated_at]}
  typed_schema "api_keys" do
    field :name, :string
    field(:value, Sequin.Encrypted.Binary)

    belongs_to :account, Account

    timestamps()
  end

  def changeset(api_key, attrs) do
    api_key
    |> cast(attrs, [:name, :value])
    |> validate_required([:name, :value])
    |> unique_constraint(:value)
  end

  defp base_query(query \\ __MODULE__) do
    from(ak in query, as: :api_key)
  end

  def where_account(query \\ base_query(), account_id) do
    from(ak in query, where: ak.account_id == ^account_id)
  end
end

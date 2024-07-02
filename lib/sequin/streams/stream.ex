defmodule Sequin.Streams.Stream do
  @moduledoc false
  use Sequin.Schema

  import Ecto.Changeset
  import Ecto.Query

  alias Sequin.Accounts.Account
  alias Sequin.Streams.Stream

  @derive {Jason.Encoder, only: [:id, :slug, :account_id, :inserted_at, :updated_at]}
  typed_schema "streams" do
    field :slug, :string

    belongs_to :account, Account

    timestamps()
  end

  def changeset(%Stream{} = stream, attrs) do
    stream
    |> cast(attrs, [:slug])
    |> validate_required([:slug])
  end

  def where_id(query \\ base_query(), id) do
    from(s in query, where: s.id == ^id)
  end

  def where_account_id(query \\ __MODULE__, account_id) do
    from(s in query, where: s.account_id == ^account_id)
  end

  defp base_query(query \\ __MODULE__) do
    from(s in query, as: :stream)
  end
end

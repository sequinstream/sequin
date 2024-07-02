defmodule Sequin.Databases.PostgresDatabase do
  @moduledoc false
  use Sequin.Schema

  import Ecto.Changeset
  import Ecto.Query

  alias Ecto.Queryable

  require Logger

  typed_schema "postgres_databases" do
    field :dbname, :string
    field :host, :string
    field :pool_size, :integer, default: 3
    field :port, :integer
    field :queue_interval, :integer
    field :queue_target, :integer, default: 200
    field :slug, :string
    field :ssl, :boolean, default: false
    field :user, :string
    field(:password, Sequin.EncryptedBinary) :: String.t()

    belongs_to(:account, Sequin.Accounts.Account)

    timestamps()
  end

  def changeset(df, attrs) do
    df
    |> cast(attrs, [
      :dbname,
      :host,
      :pool_size,
      :port,
      :queue_interval,
      :queue_target,
      :slug,
      :ssl,
      :user,
      :password
    ])
    |> validate_number(:port, greater_than_or_equal_to: 0, less_than_or_equal_to: 65_535)
  end

  @spec where_account(Queryable.t(), String.t()) :: Queryable.t()
  def where_account(query \\ base_query(), account_id) do
    from([database: db] in query, where: db.account_id == ^account_id)
  end

  defp base_query(query \\ __MODULE__) do
    from(db in query, as: :database)
  end
end

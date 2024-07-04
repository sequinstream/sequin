defmodule Sequin.Databases.PostgresDatabase do
  @moduledoc false
  use Sequin.Schema

  import Ecto.Changeset
  import Ecto.Query

  alias __MODULE__
  alias Ecto.Queryable

  require Logger

  @derive {Inspect, except: [:password]}
  typed_schema "postgres_databases" do
    field :database, :string
    field :hostname, :string
    field :pool_size, :integer, default: 3
    field :port, :integer
    field :queue_interval, :integer, default: 1000
    field :queue_target, :integer, default: 50
    field :slug, :string
    field :ssl, :boolean, default: false
    field :username, :string
    field(:password, Sequin.Encrypted.Binary) :: String.t()

    belongs_to(:account, Sequin.Accounts.Account)

    timestamps()
  end

  def changeset(df, attrs) do
    df
    |> cast(attrs, [
      :database,
      :hostname,
      :pool_size,
      :port,
      :queue_interval,
      :queue_target,
      :slug,
      :ssl,
      :username,
      :password
    ])
    |> validate_number(:port, greater_than_or_equal_to: 0, less_than_or_equal_to: 65_535)
  end

  @spec where_account(Queryable.t(), String.t()) :: Queryable.t()
  def where_account(query \\ base_query(), account_id) do
    from([database: db] in query, where: db.account_id == ^account_id)
  end

  defp base_query(query \\ PostgresDatabase) do
    from(db in query, as: :database)
  end

  def to_postgrex_opts(%PostgresDatabase{} = db) do
    db
    |> Sequin.Map.from_ecto()
    |> Map.take([:database, :hostname, :pool_size, :port, :queue_interval, :queue_target, :password, :ssl, :username])
    |> Enum.to_list()
  end
end

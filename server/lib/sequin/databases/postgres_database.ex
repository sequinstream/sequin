defmodule Sequin.Databases.PostgresDatabase do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset
  import Ecto.Query

  alias __MODULE__
  alias Ecto.Queryable

  require Logger

  @derive {Inspect, except: [:password]}
  @derive {Jason.Encoder,
           only: [
             :id,
             :database,
             :hostname,
             :pool_size,
             :port,
             :queue_interval,
             :queue_target,
             :name,
             :ssl,
             :username,
             :password
           ]}
  typed_schema "postgres_databases" do
    field :database, :string
    field :hostname, :string
    field :pool_size, :integer, default: 3
    field :port, :integer
    field :queue_interval, :integer, default: 1000
    field :queue_target, :integer, default: 50
    field :name, :string
    field :ssl, :boolean, default: false
    field :username, :string
    field(:password, Sequin.Encrypted.Binary) :: String.t()

    belongs_to(:account, Sequin.Accounts.Account)

    timestamps()
  end

  def changeset(pd, attrs) do
    pd
    |> cast(attrs, [
      :database,
      :hostname,
      :pool_size,
      :port,
      :queue_interval,
      :queue_target,
      :name,
      :ssl,
      :username,
      :password
    ])
    |> validate_required([:database, :hostname, :port, :username, :password, :name])
    |> validate_number(:port, greater_than_or_equal_to: 0, less_than_or_equal_to: 65_535)
    |> Sequin.Changeset.validate_name()
    |> unique_constraint([:account_id, :name], name: :postgres_databases_account_id_name_index)
  end

  @spec where_account(Queryable.t(), String.t()) :: Queryable.t()
  def where_account(query \\ base_query(), account_id) do
    from([database: pd] in query, where: pd.account_id == ^account_id)
  end

  @spec where_id(Queryable.t(), String.t()) :: Queryable.t()
  def where_id(query \\ base_query(), id) do
    from([database: pd] in query, where: pd.id == ^id)
  end

  defp base_query(query \\ PostgresDatabase) do
    from(pd in query, as: :database)
  end

  def to_postgrex_opts(%PostgresDatabase{} = pd) do
    opts =
      pd
      |> Sequin.Map.from_ecto()
      |> Map.take([:database, :hostname, :pool_size, :port, :queue_interval, :queue_target, :password, :ssl, :username])
      |> Enum.to_list()

    if opts[:ssl] do
      # To change this to verify_full in the future, we'll need to add CA certs for the cloud vendors
      Keyword.put(opts, :ssl_opts, verify: :verify_none)
    else
      opts
    end
  end
end

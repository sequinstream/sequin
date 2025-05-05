defmodule Sequin.Databases.PostgresDatabasePrimary do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset

  # alias __MODULE__
  alias Sequin.Databases.PostgresDatabase

  @derive {Inspect, except: [:password]}
  @derive {Jason.Encoder,
           only: [
             :id,
             :database,
             :hostname,
             :port,
             :ssl,
             :username,
             :ipv6,
             :postgres_database_id
           ]}

  typed_schema "postgres_databases_primaries" do
    field :database, :string
    field :hostname, :string
    field :port, :integer
    field :ssl, :boolean, default: false
    field :username, :string
    field(:password, Sequin.Encrypted.Binary) :: String.t()
    field :ipv6, :boolean, default: false

    belongs_to(:postgres_database, PostgresDatabase, type: :binary_id)

    timestamps()
  end

  @doc false
  def changeset(primary, attrs) do
    primary
    |> cast(attrs, [
      :database,
      :hostname,
      :port,
      :ssl,
      :username,
      :password,
      :ipv6,
      :postgres_database_id
    ])
    |> validate_required([
      :database,
      :hostname,
      :port,
      :username,
      :password,
      :postgres_database_id
    ])
    |> validate_number(:port, greater_than_or_equal_to: 0, less_than_or_equal_to: 65_535)
    |> unique_constraint([:postgres_database_id, :hostname],
      name: :postgres_databases_primaries_postgres_database_id_hostname_index
    )
    |> foreign_key_constraint(:postgres_database_id)
  end
end

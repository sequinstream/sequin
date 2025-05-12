defmodule Sequin.Databases.PostgresDatabasePrimary do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset

  alias Sequin.Encrypted.Field, as: EncryptedField

  @derive {Inspect, except: [:password]}
  @derive {Jason.Encoder,
           only: [
             :database,
             :hostname,
             :port,
             :ssl,
             :username,
             :ipv6
           ]}

  @primary_key false
  typed_embedded_schema do
    field :database, :string
    field :hostname, :string
    field :port, :integer
    field :ssl, :boolean, default: false
    field :username, :string
    field :password, EncryptedField
    field :ipv6, :boolean, default: false
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
      :ipv6
    ])
    |> validate_required([
      :database,
      :hostname,
      :port,
      :username,
      :password
    ])
    |> validate_number(:port, greater_than_or_equal_to: 0, less_than_or_equal_to: 65_535)
  end

  def connection_url(%__MODULE__{} = pd) do
    "postgres://#{pd.username}:#{pd.password}@#{pd.hostname}:#{pd.port}/#{pd.database}"
  end
end

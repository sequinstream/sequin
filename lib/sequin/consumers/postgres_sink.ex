defmodule Sequin.Consumers.PostgresSink do
  @moduledoc false
  use Ecto.Schema
  use TypedEctoSchema

  import Ecto.Changeset

  alias __MODULE__
  alias Sequin.Encrypted.Field, as: EncryptedField

  @derive {Jason.Encoder, only: [:host, :port, :database, :table_name]}
  @derive {Inspect, except: [:password]}
  @primary_key false
  typed_embedded_schema do
    field :type, Ecto.Enum, values: [:postgres], default: :postgres
    field :host, :string
    field :port, :integer, default: 5432
    field :database, :string
    field :table_name, :string
    field :username, :string
    field :password, EncryptedField
    field :ssl, :boolean, default: false
    field :routing_mode, Ecto.Enum, values: [:dynamic, :static]
    field :connection_id, :string
  end

  @doc false
  def changeset(struct, params) do
    struct
    |> cast(params, [:host, :port, :database, :table_name, :username, :password, :ssl, :routing_mode])
    |> validate_required([:host, :port, :database, :table_name])
    |> validate_number(:port, greater_than: 0, less_than: 65_536)
    |> put_new_connection_id()
  end

  def conn_opts(%PostgresSink{} = sink) do
    [
      hostname: sink.host,
      port: sink.port,
      username: sink.username,
      password: sink.password,
      database: sink.database,
      ssl: if(sink.ssl, do: [verify: :verify_none], else: false)
    ]
  end

  defp put_new_connection_id(changeset) do
    case get_field(changeset, :connection_id) do
      nil -> put_change(changeset, :connection_id, Ecto.UUID.generate())
      _ -> changeset
    end
  end
end

defmodule Sequin.Consumers.KinesisSink do
  @moduledoc false
  use Ecto.Schema
  use TypedEctoSchema

  import Ecto.Changeset

  alias Sequin.Aws.HttpClient
  alias Sequin.Encrypted

  @derive {Jason.Encoder, only: [:stream_name, :region]}
  @derive {Inspect, except: [:secret_access_key]}
  @primary_key false
  typed_embedded_schema do
    field :type, Ecto.Enum, values: [:kinesis], default: :kinesis
    field :stream_name, :string
    field :region, :string
    field :access_key_id, :string
    field :secret_access_key, Encrypted.Field
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [:stream_name, :region, :access_key_id, :secret_access_key])
    |> validate_required([:stream_name, :region, :access_key_id, :secret_access_key])
  end

  def aws_client(%__MODULE__{} = sink) do
    sink.access_key_id
    |> AWS.Client.create(sink.secret_access_key, sink.region)
    |> HttpClient.put_client()
  end
end

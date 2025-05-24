defmodule Sequin.Consumers.KinesisSink do
  @moduledoc "Represents configuration for sinking events to an AWS Kinesis Data Stream."
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
    field :partition_key_field, :string, default: "id"
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [:stream_name, :region, :access_key_id, :secret_access_key, :partition_key_field])
    |> validate_required([:stream_name, :region, :access_key_id, :secret_access_key])
    |> validate_stream_name()
    |> validate_length(:partition_key_field, max: 100)
  end

  defp validate_stream_name(changeset) do
    changeset
    |> validate_format(:stream_name, ~r/^[a-zA-Z0-9_.-]+$/,
      message: "must contain only alphanumeric characters, hyphens, underscores, and periods"
    )
    |> validate_length(:stream_name, min: 1, max: 128)
  end

  def aws_client(%__MODULE__{} = sink) do
    sink.access_key_id
    |> AWS.Client.create(sink.secret_access_key, sink.region)
    |> HttpClient.put_client()
  end
end

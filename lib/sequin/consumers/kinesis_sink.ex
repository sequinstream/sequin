defmodule Sequin.Consumers.KinesisSink do
  @moduledoc false
  use Ecto.Schema
  use TypedEctoSchema

  import Ecto.Changeset

  alias Sequin.Aws.HttpClient
  alias Sequin.Encrypted

  @derive {Jason.Encoder, only: [:stream_arn]}
  @derive {Inspect, except: [:secret_access_key]}

  @primary_key false
  typed_embedded_schema do
    field :type, Ecto.Enum, values: [:kinesis], default: :kinesis
    field :stream_arn, :string
    field :access_key_id, :string
    field :secret_access_key, Encrypted.Field
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [:stream_arn, :access_key_id, :secret_access_key])
    |> validate_required([:stream_arn, :access_key_id, :secret_access_key])
  end

  def region(%__MODULE__{stream_arn: stream_arn}) when is_binary(stream_arn) do
    case Regex.run(~r/^arn:aws:kinesis:([^:]+):[^:]+:stream\/(.+)$/, stream_arn) do
      [_, region, _stream_name] -> region
      _ -> nil
    end
  end

  def region(%__MODULE__{}), do: nil

  def aws_client(%__MODULE__{} = sink) do
    sink.access_key_id
    |> AWS.Client.create(sink.secret_access_key, region(sink))
    |> HttpClient.put_client()
  end
end

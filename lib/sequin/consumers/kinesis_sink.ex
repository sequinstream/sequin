defmodule Sequin.Consumers.KinesisSink do
  @moduledoc false
  use Ecto.Schema
  use TypedEctoSchema

  import Ecto.Changeset

  alias Sequin.Aws.HttpClient
  alias Sequin.Encrypted

  @derive {Jason.Encoder, only: [:stream_arn, :region]}
  @derive {Inspect, except: [:secret_access_key]}

  @primary_key false
  typed_embedded_schema do
    field :type, Ecto.Enum, values: [:kinesis], default: :kinesis
    field :stream_arn, :string
    field :region, :string
    field :access_key_id, :string
    field :secret_access_key, Encrypted.Field
    field :routing_mode, Ecto.Enum, values: [:dynamic, :static]
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [:stream_arn, :region, :access_key_id, :secret_access_key, :routing_mode])
    |> maybe_put_region_from_arn()
    |> validate_required([:region, :access_key_id, :secret_access_key])
    |> validate_stream_arn()
    |> validate_routing()
  end

  defp validate_stream_arn(changeset) do
    changeset
    |> validate_format(:stream_arn, kinesis_arn_regex(),
      message: "must be a valid AWS Kinesis Stream ARN (arn:aws:kinesis:<region>:<account-id>:stream/<stream-name>)"
    )
    |> validate_length(:stream_arn, max: 2000)
  end

  defp maybe_put_region_from_arn(changeset) do
    case {get_field(changeset, :region), get_field(changeset, :stream_arn)} do
      {nil, nil} ->
        changeset

      {nil, stream_arn} ->
        case region_from_arn(stream_arn) do
          {:error, _} -> add_error(changeset, :region, "Could not infer region from stream_arn")
          region -> put_change(changeset, :region, region)
        end

      _ ->
        changeset
    end
  end

  defp validate_routing(changeset) do
    routing_mode = get_field(changeset, :routing_mode)

    cond do
      routing_mode == :dynamic ->
        put_change(changeset, :stream_arn, nil)

      routing_mode == :static ->
        validate_required(changeset, [:stream_arn])

      true ->
        add_error(changeset, :routing_mode, "is required")
    end
  end

  def region(%__MODULE__{region: region}) when is_binary(region), do: region

  def region(%__MODULE__{stream_arn: stream_arn}) when is_binary(stream_arn) do
    case region_from_arn(stream_arn) do
      {:error, _} -> nil
      region -> region
    end
  end

  def region(%__MODULE__{}), do: nil

  @doc """
  Extracts the AWS region from the given Kinesis Stream ARN.

  ## Examples

      iex> region_from_arn("arn:aws:kinesis:us-west-2:123456789012:stream/my-stream")
      "us-west-2"

      iex> region_from_arn("arn:aws:kinesis:eu-central-1:123456789012:stream/another-stream")
      "eu-central-1"

      iex> region_from_arn("invalid_arn")
      {:error, Sequin.Error.validation(summary: "Invalid Kinesis Stream ARN format")}

  """
  def region_from_arn(stream_arn) do
    case Regex.named_captures(kinesis_arn_regex(), stream_arn) do
      %{"region" => region} -> region
      _ -> {:error, Sequin.Error.validation(summary: "Invalid Kinesis Stream ARN format")}
    end
  end

  def kinesis_arn_regex do
    ~r/^arn:aws:kinesis:(?<region>[a-z0-9-]+):(?<account_id>\d+):stream\/(?<stream_name>.+)$/
  end

  def aws_client(%__MODULE__{} = sink) do
    sink.access_key_id
    |> AWS.Client.create(sink.secret_access_key, region(sink))
    |> HttpClient.put_client()
  end
end

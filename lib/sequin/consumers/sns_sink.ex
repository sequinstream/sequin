defmodule Sequin.Consumers.SnsSink do
  @moduledoc "Represents configuration for sinking events to an AWS SNS Topic."
  use Ecto.Schema
  use TypedEctoSchema

  import Ecto.Changeset

  alias Sequin.Aws.HttpClient
  alias Sequin.Encrypted

  @derive {Jason.Encoder, only: [:topic_arn, :region]}
  @primary_key false
  typed_embedded_schema do
    field :type, Ecto.Enum, values: [:sns], default: :sns
    field :topic_arn, :string
    field :region, :string
    field :access_key_id, :string
    field :secret_access_key, Encrypted.Field
    field :is_fifo, :boolean, default: false
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [:topic_arn, :region, :access_key_id, :secret_access_key])
    |> maybe_put_region_from_arn()
    |> validate_required([:topic_arn, :region, :access_key_id, :secret_access_key])
    |> validate_topic_arn()
    |> put_is_fifo()
  end

  defp validate_topic_arn(changeset) do
    changeset
    |> validate_format(:topic_arn, sns_arn_regex(),
      message: "must be a valid AWS SNS Topic ARN (arn:aws:sns:<region>:<account-id>:<topic-name>)"
    )
    |> validate_length(:topic_arn, max: 2000) # Keep length validation, ARNs can be long
  end

  defp maybe_put_region_from_arn(changeset) do
    case {get_field(changeset, :region), get_field(changeset, :topic_arn)} do
      {nil, nil} ->
        changeset

      {nil, topic_arn} ->
        case region_from_arn(topic_arn) do
          {:error, _} -> add_error(changeset, :region, "Could not infer region from topic_arn")
          region -> put_change(changeset, :region, region)
        end

      _ ->
        changeset
    end
  end

  defp put_is_fifo(changeset) do
    if changeset |> get_field(:topic_arn) |> ends_with_fifo?() do
      put_change(changeset, :is_fifo, true)
    else
      changeset
    end
  end

  defp ends_with_fifo?(nil), do: false
  defp ends_with_fifo?(arn), do: String.ends_with?(arn, ".fifo")

  def aws_client(%__MODULE__{} = sink) do
    sink.access_key_id
    |> AWS.Client.create(sink.secret_access_key, sink.region)
    |> HttpClient.put_client()
  end

  @doc """
  Extracts the AWS region from the given SNS Topic ARN.

  ## Examples

      iex> region_from_arn("arn:aws:sns:us-west-2:123456789012:MyTopic")
      "us-west-2"

      iex> region_from_arn("arn:aws:sns:eu-central-1:123456789012:AnotherTopicName")
      "eu-central-1"

      iex> region_from_arn("invalid_arn")
      {:error, Sequin.Error.validation(summary: "Invalid SNS Topic ARN format")}

  """
  def region_from_arn(topic_arn) do
    case Regex.named_captures(sns_arn_regex(), topic_arn) do
      %{"region" => region} -> region
      _ -> {:error, Sequin.Error.validation(summary: "Invalid SNS Topic ARN format")}
    end
  end

  defp sns_arn_regex do
    ~r/^arn:aws:sns:(?<region>[a-z0-9-]+):(?<account_id>\d{12}):(?<topic_name>[a-zA-Z0-9_-]+)$/
  end
end

defmodule Sequin.Consumers.SqsSink do
  @moduledoc false
  use Ecto.Schema
  use TypedEctoSchema

  import Ecto.Changeset

  alias Sequin.Aws.HttpClient
  alias Sequin.Encrypted

  @derive {Jason.Encoder, only: [:queue_url, :region]}
  @derive {Inspect, except: [:secret_access_key]}
  @primary_key false
  typed_embedded_schema do
    field :type, Ecto.Enum, values: [:sqs], default: :sqs
    field :queue_url, :string
    field :region, :string
    field :access_key_id, :string
    field :secret_access_key, Encrypted.Field
    field :is_fifo, :boolean, default: false
    field :endpoint, :string
    field :proto, :string
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [:queue_url, :region, :access_key_id, :secret_access_key, :is_fifo, :endpoint, :proto])
    |> maybe_put_region()
    |> validate_required([:queue_url, :region, :access_key_id, :secret_access_key])
    |> validate_queue_url()
    |> put_is_fifo()
    |> validate_inclusion(:proto, ["http", "https"])
  end

  defp validate_queue_url(changeset) do
    changeset
    |> validate_format(:queue_url, sqs_url_regex(),
      message: "must be a valid AWS SQS URL (https://sqs.<region>.amazonaws.com/<account-id>/<queue-name>)"
    )
    |> validate_length(:queue_url, max: 2000)
  end

  defp maybe_put_region(changeset) do
    case {get_field(changeset, :region), get_field(changeset, :queue_url)} do
      {nil, nil} ->
        changeset

      {nil, queue_url} ->
        case region_from_url(queue_url) do
          {:error, _} -> add_error(changeset, :region, "Could not infer region from queue_url")
          region -> put_change(changeset, :region, region)
        end

      _ ->
        changeset
    end
  end

  defp put_is_fifo(changeset) do
    is_fifo = changeset |> get_field(:queue_url) |> ends_with_fifo?()
    put_change(changeset, :is_fifo, is_fifo)
  end

  defp ends_with_fifo?(nil), do: false
  defp ends_with_fifo?(url), do: String.ends_with?(url, ".fifo")

  def aws_client(%__MODULE__{} = sink) do
    client = sink.access_key_id
    |> AWS.Client.create(sink.secret_access_key, sink.region)

    if sink.endpoint do
      client
      |> Map.put(:endpoint, sink.endpoint)
      |> Map.put(:proto, sink.proto)
    else
      client
    end
    |> HttpClient.put_client()
  end

  @doc """
  Extracts the AWS region from the given SQS queue URL.

  ## Examples

      iex> region_from_url("https://sqs.us-west-2.amazonaws.com/123456789012/MyQueue")
      "us-west-2"

      iex> region_from_url("https://sqs.eu-central-1.amazonaws.com/123456789012/MyQueue.fifo")
      "eu-central-1"

      iex> region_from_url("http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/demo-queue")
      "us-east-1"

      iex> region_from_url("invalid_url")
      {:error, "Invalid SQS queue URL format"}

  """
  def region_from_url(queue_url) do
    case Regex.named_captures(sqs_url_regex(), queue_url) do
      %{"region" => region} -> region
      _ -> {:error, Sequin.Error.validation(summary: "Invalid SQS queue URL format")}
    end
  end

  def sqs_url_regex do
    ~r/^https?:\/\/sqs\.(?<region>[a-z0-9-]+)\.(amazonaws\.com|localhost\.localstack\.cloud)(?::\d+)?\/\d{12}\/[a-zA-Z0-9_-]+(?:\.fifo)?$/
  end
end

defmodule Sequin.Consumers.SnsSink do
  @moduledoc "Represents configuration for sinking events to an AWS SNS Topic."
  use Ecto.Schema
  use TypedEctoSchema

  import Ecto.Changeset

  alias Sequin.Aws.HttpClient
  alias Sequin.Encrypted

  @derive {Jason.Encoder, only: [:topic_arn, :region, :use_task_role]}
  @derive {Inspect, except: [:secret_access_key]}
  @primary_key false
  typed_embedded_schema do
    field :type, Ecto.Enum, values: [:sns], default: :sns
    field :topic_arn, :string
    field :region, :string
    field :access_key_id, :string
    field :secret_access_key, Encrypted.Field
    field :use_task_role, :boolean, default: false
    field :is_fifo, :boolean, default: false
    field :use_emulator, :boolean, default: false
    field :emulator_base_url, :string
    field :routing_mode, Ecto.Enum, values: [:dynamic, :static]
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [
      :topic_arn,
      :region,
      :access_key_id,
      :secret_access_key,
      :use_task_role,
      :use_emulator,
      :emulator_base_url,
      :routing_mode
    ])
    |> maybe_put_region_from_arn()
    |> validate_credentials()
    |> validate_topic_arn()
    |> validate_routing()
    |> put_is_fifo()
    |> validate_emulator_base_url()
  end

  defp validate_credentials(changeset) do
    use_task_role = get_field(changeset, :use_task_role)

    if use_task_role do
      # When using task role, we only need region
      changeset
      |> validate_required([:region])
      |> put_change(:access_key_id, nil)
      |> put_change(:secret_access_key, nil)
    else
      # When using explicit credentials, we need all three
      validate_required(changeset, [:region, :access_key_id, :secret_access_key])
    end
  end

  defp validate_topic_arn(changeset) do
    changeset
    |> validate_format(:topic_arn, sns_arn_regex(),
      message: "must be a valid AWS SNS Topic ARN (arn:aws:sns:<region>:<account-id>:<topic-name>)"
    )
    # Keep length validation, ARNs can be long
    |> validate_length(:topic_arn, max: 2000)
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

  defp validate_routing(changeset) do
    routing_mode = get_field(changeset, :routing_mode)

    cond do
      routing_mode == :dynamic ->
        put_change(changeset, :topic_arn, nil)

      routing_mode == :static ->
        validate_required(changeset, [:topic_arn])

      true ->
        add_error(changeset, :routing_mode, "is required")
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

  defp validate_emulator_base_url(changeset) do
    use_emulator? = get_field(changeset, :use_emulator)

    if use_emulator? do
      changeset
      |> validate_required([:emulator_base_url])
      |> validate_format(:emulator_base_url, ~r/^https?:\/\//i, message: "must start with http:// or https://")
    else
      changeset
    end
  end

  def aws_client(%__MODULE__{} = sink) do
    with {:ok, client} <- Sequin.Aws.get_aws_client(sink),
         {:ok, client} <- configure_emulator(client, sink) do
      {:ok, HttpClient.put_client(client)}
    end
  end

  defp configure_emulator(client, sink) do
    if sink.use_emulator do
      %{scheme: scheme, authority: authority} = URI.parse(sink.emulator_base_url)

      client =
        client
        |> Map.put(:proto, scheme)
        |> Map.put(:endpoint, authority)

      {:ok, client}
    else
      {:ok, client}
    end
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

  def sns_arn_regex do
    ~r/^arn:aws:sns:(?<region>[a-z0-9-]+):(?<account_id>\d{12}):(?<topic_name>[a-zA-Z0-9_.-]+)$/
  end
end

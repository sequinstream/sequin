defmodule Sequin.Consumers.GcpPubsubSink do
  @moduledoc false
  use Ecto.Schema
  use TypedEctoSchema

  import Ecto.Changeset

  alias Sequin.Sinks.Gcp.Credentials
  alias Sequin.Sinks.Gcp.PubSub

  @derive {Jason.Encoder, only: [:project_id, :topic_id]}
  @primary_key false
  typed_embedded_schema do
    field :type, Ecto.Enum, values: [:gcp_pubsub], default: :gcp_pubsub
    field :project_id, :string
    field :topic_id, :string
    field :connection_id, :string
    field :use_emulator, :boolean, default: false
    field :emulator_base_url, :string
    field :routing_mode, Ecto.Enum, values: [:dynamic, :static]

    embeds_one :credentials, Credentials, on_replace: :delete
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [
      :project_id,
      :topic_id,
      :use_emulator,
      :emulator_base_url,
      :routing_mode
    ])
    |> validate_required([:project_id, :use_emulator])
    |> validate_credential_param(params)
    |> validate_emulator_base_url()
    |> cast_credentials()
    |> validate_length(:project_id, max: 255)
    |> validate_length(:topic_id, max: 255)
    |> validate_format(:project_id, ~r/^[a-z][-a-z0-9]{4,28}[a-z0-9]$/,
      message:
        "must be between 6 and 30 characters, start with a letter, and contain only lowercase letters, numbers, and hyphens"
    )
    |> validate_format(:topic_id, ~r/^[a-zA-Z][a-zA-Z0-9-_.~+%]{2,254}$/,
      message: "must be between 3 and 255 characters and match the pattern: [a-zA-Z][a-zA-Z0-9-_.~+%]*"
    )
    |> validate_routing()
    |> put_new_connection_id()
  end

  defp validate_routing(changeset) do
    routing_mode = get_field(changeset, :routing_mode)

    cond do
      routing_mode == :dynamic ->
        put_change(changeset, :topic_id, nil)

      routing_mode == :static ->
        validate_required(changeset, [:topic_id])

      true ->
        add_error(changeset, :routing_mode, "is required")
    end
  end

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

  defp cast_credentials(changeset) do
    use_emulator? = get_field(changeset, :use_emulator)

    if use_emulator? do
      put_change(changeset, :credentials, %{})
    else
      cast_embed(changeset, :credentials, required: true)
    end
  end

  @doc """
  Creates a new PubSub client for the given sink configuration.
  """
  def pubsub_client(%__MODULE__{} = sink) do
    opts =
      if sink.use_emulator do
        [req_opts: [base_url: sink.emulator_base_url], use_emulator: true]
      else
        []
      end

    PubSub.new(
      sink.project_id,
      sink.credentials,
      opts
    )
  end

  def topic_path(%__MODULE__{} = sink) do
    "projects/#{sink.project_id}/topics/#{sink.topic_id}"
  end

  defp put_new_connection_id(changeset) do
    case get_field(changeset, :connection_id) do
      nil -> put_change(changeset, :connection_id, Ecto.UUID.generate())
      _ -> changeset
    end
  end

  defp validate_credential_param(changeset, %{"credentials" => creds}) when is_binary(creds) do
    case Jason.decode(creds) do
      {:ok, _} -> changeset
      {:error, _} -> add_error(changeset, :credentials, "must be valid JSON service account credentials")
    end
  end

  defp validate_credential_param(changeset, _params), do: changeset
end

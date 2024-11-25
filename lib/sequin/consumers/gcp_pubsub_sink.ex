defmodule Sequin.Consumers.GcpPubsubSink do
  @moduledoc false
  use Ecto.Schema
  use TypedEctoSchema

  import Ecto.Changeset

  alias Sequin.Gcp.Credentials
  alias Sequin.Gcp.PubSub

  @derive {Jason.Encoder, only: [:project_id, :topic_id]}
  @primary_key false
  typed_embedded_schema do
    field :type, Ecto.Enum, values: [:gcp_pubsub], default: :gcp_pubsub
    field :project_id, :string
    field :topic_id, :string
    field :connection_id, :string

    embeds_one :credentials, Credentials
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [
      :project_id,
      :topic_id
    ])
    |> validate_required([:project_id, :topic_id])
    |> validate_credential_param(params)
    |> cast_embed(:credentials, required: true)
    |> validate_length(:project_id, max: 255)
    |> validate_length(:topic_id, max: 255)
    |> validate_format(:project_id, ~r/^[a-z][-a-z0-9]{4,28}[a-z0-9]$/,
      message:
        "must be between 6 and 30 characters, start with a letter, and contain only lowercase letters, numbers, and hyphens"
    )
    |> validate_format(:topic_id, ~r/^[a-zA-Z][a-zA-Z0-9-_.~+%]{2,254}$/,
      message: "must be between 3 and 255 characters and match the pattern: [a-zA-Z][a-zA-Z0-9-_.~+%]*"
    )
    |> put_new_connection_id()
  end

  @doc """
  Creates a new PubSub client for the given sink configuration.
  """
  def pubsub_client(%__MODULE__{} = sink) do
    PubSub.new(
      sink.project_id,
      sink.credentials
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

defmodule Sequin.Consumers.GooglePubsubSink do
  @moduledoc false
  use Ecto.Schema
  use TypedEctoSchema

  import Ecto.Changeset

  alias Sequin.Encrypted

  @derive {Jason.Encoder, only: [:project_id, :topic_id]}
  @primary_key false
  typed_embedded_schema do
    field :type, Ecto.Enum, values: [:pubsub], default: :pubsub
    field :project_id, :string
    field :topic_id, :string
    field :credentials, Encrypted.Field
    field :connection_id, :string
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [
      :project_id,
      :topic_id,
      :credentials
    ])
    |> validate_required([:project_id, :topic_id, :credentials])
    |> validate_length(:project_id, max: 255)
    |> validate_length(:topic_id, max: 255)
    |> validate_format(:project_id, ~r/^[a-z][-a-z0-9]{4,28}[a-z0-9]$/,
      message:
        "must be between 6 and 30 characters, start with a letter, and contain only lowercase letters, numbers, and hyphens"
    )
    |> validate_format(:topic_id, ~r/^[a-zA-Z][a-zA-Z0-9-_.~+%]{2,254}$/,
      message: "must be between 3 and 255 characters and match the pattern: [a-zA-Z][a-zA-Z0-9-_.~+%]*"
    )
    |> validate_credentials()
    |> put_new_connection_id()
  end

  defp validate_credentials(changeset) do
    case get_field(changeset, :credentials) do
      nil ->
        changeset

      credentials ->
        case Jason.decode(credentials) do
          {:ok, _} -> changeset
          {:error, _} -> add_error(changeset, :credentials, "must be valid JSON service account credentials")
        end
    end
  end

  defp put_new_connection_id(changeset) do
    case get_field(changeset, :connection_id) do
      nil -> put_change(changeset, :connection_id, Ecto.UUID.generate())
      _ -> changeset
    end
  end

  def topic_path(%__MODULE__{} = sink) do
    "projects/#{sink.project_id}/topics/#{sink.topic_id}"
  end
end

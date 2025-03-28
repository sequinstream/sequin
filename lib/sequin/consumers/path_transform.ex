defmodule Sequin.Consumers.PathTransform do
  @moduledoc false
  use Ecto.Schema
  use TypedEctoSchema

  import Ecto.Changeset

  @derive {Jason.Encoder, only: [:path]}
  @primary_key false
  typed_embedded_schema do
    field :type, Ecto.Enum, values: [:path], default: :path
    field :path, :string
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [:path])
    |> validate_required([:path])
    |> validate_path()
  end

  defp validate_path(changeset) do
    changeset
    |> validate_format(
      :path,
      ~r/^[a-zA-Z0-9\-._~!$&'()*+,;=:@%\/]+$/,
      message: "must be a valid URL path"
    )
    |> validate_format(
      :path,
      ~r/^(record|changes|action|metadata)$|^(record|changes|metadata)\.([a-zA-Z0-9_]+)$|^metadata\.(table_schema|table_name|commit_timestamp|commit_lsn|transaction_annotations|consumer)$|^metadata\.transaction_annotations\.[a-zA-Z0-9_]+$|^metadata\.consumer\.(id|name)$/,
      message: "must be a valid path into the record structure"
    )
    |> validate_length(:path, max: 2000)
  end
end

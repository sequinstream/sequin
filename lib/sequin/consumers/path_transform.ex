defmodule Sequin.Transforms.PathTransform do
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
    |> validate_length(:path, max: 2000)
  end
end

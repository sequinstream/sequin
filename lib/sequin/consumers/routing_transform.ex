defmodule Sequin.Consumers.RoutingTransform do
  @moduledoc false
  use Ecto.Schema
  use TypedEctoSchema

  import Ecto.Changeset

  @derive {Jason.Encoder, only: [:type, :code]}

  @primary_key false
  typed_embedded_schema do
    field :type, Ecto.Enum, values: [:routing], default: :routing
    field :code, :string

    field :sink_type, Ecto.Enum, values: [:http_push]
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [:code, :sink_type])
    |> validate_required([:code])
    |> validate_required([:sink_type])
    |> validate_path()
  end

  defp validate_path(changeset) do
    validate_length(changeset, :code, max: 20_000)
  end
end

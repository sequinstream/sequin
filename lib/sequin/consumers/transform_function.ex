defmodule Sequin.Consumers.TransformFunction do
  @moduledoc false
  use Ecto.Schema
  use TypedEctoSchema

  import Ecto.Changeset

  alias Sequin.Consumers

  @derive {Jason.Encoder, only: [:type, :code]}

  @primary_key false
  typed_embedded_schema do
    field :type, Ecto.Enum, values: [:transform], default: :transform
    field :code, :string
  end

  def changeset(struct, params, account_id) do
    changeset = cast(struct, params, [:code])

    if Sequin.feature_enabled?(account_id, :function_transforms) do
      changeset
      |> validate_required([:code])
      |> validate_change(:code, fn :code, code ->
        Consumers.validate_code(code)
      end)
    else
      add_error(changeset, :type, "Transform functions are not enabled. Talk to the Sequin team to enable them.")
    end
  end
end

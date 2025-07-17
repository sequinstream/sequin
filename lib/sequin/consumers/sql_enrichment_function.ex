defmodule Sequin.Consumers.SqlEnrichmentFunction do
  @moduledoc false
  use Ecto.Schema
  use TypedEctoSchema

  import Ecto.Changeset

  @derive {Jason.Encoder, only: [:type, :code]}

  @primary_key false
  typed_embedded_schema do
    field :type, Ecto.Enum, values: [:enrichment], default: :enrichment
    field :code, :string
  end

  def changeset(struct, params, account_id) do
    changeset = cast(struct, params, [:code])

    if Sequin.feature_enabled?(account_id, :function_transforms) do
      validate_required(changeset, [:code])
    else
      add_error(changeset, :type, "Enrichment functions are not enabled. Talk to the Sequin team to enable them.")
    end
  end
end

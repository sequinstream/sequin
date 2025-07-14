defmodule Sequin.Consumers.SqlEnrichmentFunction do
  @moduledoc false
  use Ecto.Schema
  use TypedEctoSchema

  import Ecto.Changeset

  @derive {Jason.Encoder, only: [:type, :code]}

  @primary_key false
  typed_embedded_schema do
    field :type, Ecto.Enum, values: [:sql_enrichment], default: :sql_enrichment
    field :code, :string
  end

  def changeset(struct, params, account_id) do
    changeset = cast(struct, params, [:code])

    if Sequin.feature_enabled?(account_id, :sql_enrichment_functions) do
      validate_required(changeset, [:code])
    else
      add_error(changeset, :type, "SQL enrichment functions are not enabled. Talk to the Sequin team to enable them.")
    end
  end
end

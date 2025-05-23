defmodule Sequin.Consumers.SchemaFilter do
  @moduledoc false
  use Ecto.Schema

  import Ecto.Changeset

  @derive {Jason.Encoder, only: [:schema]}

  @type t :: %__MODULE__{
          schema: String.t()
        }

  @primary_key false
  embedded_schema do
    field :schema, :string
  end

  def create_changeset(schema_filter, attrs) do
    schema_filter
    |> cast(attrs, [:schema])
    |> validate_required([:schema])
    |> validate_format(:schema, ~r/^[a-zA-Z_][a-zA-Z0-9_]*$/, message: "must be a valid PostgreSQL identifier")
  end
end

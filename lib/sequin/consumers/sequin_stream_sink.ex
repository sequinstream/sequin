defmodule Sequin.Consumers.SequinStreamSink do
  @moduledoc false
  use Ecto.Schema
  use TypedEctoSchema

  import Ecto.Changeset

  @derive {Jason.Encoder, only: [:type]}
  @primary_key false
  typed_embedded_schema do
    field :type, Ecto.Enum, values: [:sequin_stream], default: :sequin_stream
  end

  def changeset(struct, params) do
    cast(struct, params, [])
  end
end

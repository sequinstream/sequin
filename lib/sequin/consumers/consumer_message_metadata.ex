defmodule Sequin.Consumers.ConsumerMessageMetadata do
  @moduledoc false
  use Ecto.Schema

  import Ecto.Changeset

  @primary_key false
  @derive Jason.Encoder

  embedded_schema do
    field :table, :string
    field :schema, :string
    field :commit_timestamp, :utc_datetime_usec
  end

  def changeset(metadata, attrs) do
    metadata
    |> cast(attrs, [:table, :schema, :commit_timestamp])
    |> validate_required([:table, :schema, :commit_timestamp])
  end
end

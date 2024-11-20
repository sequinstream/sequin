defmodule Sequin.Consumers.RecordConsumerState do
  @moduledoc false
  use Ecto.Schema
  use TypedEctoSchema

  import Ecto.Changeset

  @derive {Jason.Encoder, only: [:producer, :initial_min_cursor]}
  @primary_key false
  typed_embedded_schema do
    field :producer, Ecto.Enum, values: [:table_and_wal, :wal]
    field :initial_min_cursor, Sequin.Ecto.IntegerKeyMap
  end

  def changeset(config, attrs) do
    cast(config, attrs, [:producer, :initial_min_cursor])
  end
end

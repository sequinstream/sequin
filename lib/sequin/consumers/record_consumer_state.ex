defmodule Sequin.Consumers.RecordConsumerState do
  @moduledoc false
  use Ecto.Schema

  import Ecto.Changeset

  @primary_key false
  embedded_schema do
    field :producer, Ecto.Enum, values: [:table_and_wal, :wal]
    field :initial_min_cursor, Sequin.Ecto.IntegerKeyMap
  end

  def changeset(config, attrs) do
    config
    |> cast(attrs, [:producer, :initial_min_cursor])
    |> validate_required([:producer])
  end
end

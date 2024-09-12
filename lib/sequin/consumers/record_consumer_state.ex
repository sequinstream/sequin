defmodule Sequin.Consumers.RecordConsumerState do
  @moduledoc false
  use Ecto.Schema

  import Ecto.Changeset

  @primary_key false
  embedded_schema do
    field :sort_column_attnum, :integer
  end

  def changeset(config, attrs) do
    config
    |> cast(attrs, [:sort_column_attnum])
    |> validate_required([:sort_column_attnum])
  end
end

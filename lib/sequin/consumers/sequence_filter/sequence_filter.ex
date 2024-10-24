defmodule Sequin.Consumers.SequenceFilter do
  @moduledoc false
  use Ecto.Schema

  import Ecto.Changeset

  alias Sequin.Consumers.SequenceFilter.ColumnFilter

  @derive {Jason.Encoder, only: [:actions, :column_filters, :group_column_attnums]}

  @type t :: %__MODULE__{
          actions: [atom()],
          column_filters: [ColumnFilter.t()],
          group_column_attnums: [integer()] | nil
        }

  @type filter_type :: :string | :number | :boolean | :datetime

  @primary_key false
  embedded_schema do
    field :actions, {:array, Ecto.Enum}, values: [:insert, :update, :delete]
    field :group_column_attnums, {:array, :integer}
    embeds_many :column_filters, ColumnFilter
  end

  def create_changeset(source_table, attrs) do
    source_table
    |> cast(attrs, [:actions, :group_column_attnums])
    |> validate_required([:actions, :group_column_attnums])
    |> cast_embed(:column_filters, with: &ColumnFilter.changeset/2)
    |> validate_length(:actions, min: 1)
    |> validate_length(:group_column_attnums, min: 1)
  end
end

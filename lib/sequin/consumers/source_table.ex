defmodule Sequin.Consumers.SourceTable do
  @moduledoc false
  use Ecto.Schema

  import Ecto.Changeset

  defmodule ColumnFilter do
    @moduledoc false
    use Ecto.Schema

    import Ecto.Changeset

    @operators [:==, :!=, :>, :<, :>=, :<=, :in, :not_in, :is_null, :not_null]
    def operator_values, do: @operators

    embedded_schema do
      field :column_attnum, :integer
      field :column_name, :string, virtual: true
      field :operator, Ecto.Enum, values: @operators
      field :value, :string
    end

    def changeset(column_filter, attrs) do
      attrs = stringify_value(attrs)

      column_filter
      |> cast(attrs, [:column_attnum, :column_name, :operator, :value])
      |> validate_required([:column_attnum, :operator, :value])
      |> validate_inclusion(:operator, @operators)
    end

    defp stringify_value(attrs) do
      Map.put(attrs, :value, to_string(attrs.value))
    end
  end

  embedded_schema do
    field :oid, :integer
    field :schema_name, :string, virtual: true
    field :table_name, :string, virtual: true
    field :actions, {:array, Ecto.Enum}, values: [:insert, :update, :delete]
    embeds_many :column_filters, ColumnFilter
  end

  def changeset(source_table, attrs) do
    source_table
    |> cast(attrs, [:oid, :schema_name, :table_name, :actions])
    |> validate_required([:oid, :actions])
    |> cast_embed(:column_filters, with: &ColumnFilter.changeset/2)
    |> validate_length(:actions, min: 1)
  end
end

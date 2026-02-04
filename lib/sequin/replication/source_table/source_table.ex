defmodule Sequin.WalPipeline.SourceTable do
  @moduledoc false
  use Ecto.Schema

  import Ecto.Changeset

  alias Sequin.WalPipeline.SourceTable.ColumnFilter

  @derive {Jason.Encoder,
           only: [
             :oid,
             :schema_name,
             :table_name,
             :actions,
             :column_filters,
             :sort_column_attnum,
             :group_column_attnums,
             :include_column_attnums,
             :exclude_column_attnums
           ]}

  @type t :: %__MODULE__{
          oid: integer,
          schema_name: String.t() | nil,
          table_name: String.t() | nil,
          actions: [atom()],
          column_filters: [ColumnFilter.t()],
          sort_column_attnum: integer() | nil,
          group_column_attnums: [integer()] | nil,
          include_column_attnums: [integer()] | nil,
          exclude_column_attnums: [integer()] | nil
        }

  @type filter_type :: :string | :number | :boolean | :datetime

  @primary_key false
  embedded_schema do
    field :oid, :integer
    field :schema_name, :string, virtual: true
    field :table_name, :string, virtual: true
    field :sort_column_attnum, :integer
    field :actions, {:array, Ecto.Enum}, values: [:insert, :update, :delete]
    field :group_column_attnums, {:array, :integer}
    field :include_column_attnums, {:array, :integer}
    field :exclude_column_attnums, {:array, :integer}
    embeds_many :column_filters, ColumnFilter
  end

  def changeset(source_table, attrs) do
    source_table
    |> cast(attrs, [
      :oid,
      :schema_name,
      :table_name,
      :actions,
      :sort_column_attnum,
      :group_column_attnums,
      :include_column_attnums,
      :exclude_column_attnums
    ])
    |> validate_required([:oid, :actions])
    |> cast_embed(:column_filters, with: &ColumnFilter.changeset/2)
    |> validate_length(:actions, min: 1)
    |> validate_mutually_exclusive([:include_column_attnums, :exclude_column_attnums])
  end

  defp validate_mutually_exclusive(changeset, fields) do
    present_fields =
      Enum.filter(fields, fn field ->
        value = get_field(changeset, field)
        is_list(value) and value != []
      end)

    case present_fields do
      [] -> changeset
      [_field] -> changeset
      [field1, field2 | _] -> add_error(changeset, field2, "cannot be set when #{field1} is set")
    end
  end

  def record_changeset(source_table, attrs) do
    changeset(source_table, attrs)
  end

  def event_changeset(source_table, attrs) do
    changeset(source_table, attrs)
  end
end

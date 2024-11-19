defmodule Sequin.Databases.PostgresDatabaseTable do
  @moduledoc false
  use Ecto.Schema

  import Ecto.Changeset

  alias Sequin.Postgres

  @primary_key false
  embedded_schema do
    field :oid, :integer, primary_key: true
    field :schema, :string
    field :name, :string
    field :sort_column_attnum, :integer, virtual: true

    embeds_many :columns, Column, on_replace: :delete, primary_key: false do
      field :attnum, :integer, primary_key: true
      field :is_pk?, :boolean
      field :name, :string
      field :type, :string
      field :pg_typtype, :string
    end
  end

  def changeset(table, attrs) do
    table
    |> cast(attrs, [:oid, :schema, :name])
    |> validate_required([:oid, :schema, :name])
    |> cast_embed(:columns, with: &columns_changeset/2, required: true)
  end

  def columns_changeset(column, attrs) do
    column
    |> cast(attrs, [:attnum, :name, :type, :is_pk?, :pg_typtype])
    |> validate_required([:attnum, :name, :type, :is_pk?, :pg_typtype])
  end

  def default_group_column_attnums(%__MODULE__{} = table) do
    if Postgres.is_event_table?(table) do
      Enum.map(["source_database_id", "source_table_oid", "record_pk"], fn col_name ->
        column = Sequin.Enum.find!(table.columns, &(&1.name == col_name))
        column.attnum
      end)
    else
      table
      |> Map.get(:columns, [])
      |> Enum.filter(& &1.is_pk?)
      |> Enum.map(& &1.attnum)
    end
  end

  def column_attnums_to_names(%__MODULE__{} = table, attnums) do
    attnum_to_name = Map.new(table.columns, &{&1.attnum, &1.name})
    Enum.map(attnums, &Map.get(attnum_to_name, &1))
  end
end

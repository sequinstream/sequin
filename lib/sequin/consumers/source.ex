defmodule Sequin.Consumers.Source do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset

  alias __MODULE__
  alias Sequin.Databases.PostgresDatabaseTable

  @derive Jason.Encoder

  @primary_key false
  typed_embedded_schema do
    field :include_schemas, {:array, :string}
    field :exclude_schemas, {:array, :string}
    field :include_table_oids, {:array, :integer}
    field :exclude_table_oids, {:array, :integer}
  end

  def changeset(source, attrs) do
    source
    |> cast(attrs, [:include_schemas, :exclude_schemas, :include_table_oids, :exclude_table_oids])
    |> validate_length(:include_schemas, min: 1)
    |> validate_length(:exclude_schemas, min: 1)
    |> validate_length(:include_table_oids, min: 1)
    |> validate_length(:exclude_table_oids, min: 1)
    |> validate_mutually_exclusive([:include_schemas, :exclude_schemas])
    |> validate_mutually_exclusive([:include_table_oids, :exclude_table_oids])
  end

  defp validate_mutually_exclusive(changeset, fields) do
    case Enum.filter(fields, &get_field(changeset, &1)) do
      [] -> changeset
      [_field] -> changeset
      [field1, field2 | _] -> add_error(changeset, field2, "cannot be set when #{field1} is set")
    end
  end

  def table_in_source?(%Source{} = source, %PostgresDatabaseTable{} = table) do
    schema_and_table_oid_in_source?(source, table.schema, table.oid)
  end

  # FIXME: make this O(1) ? could create a custom type for MapSet of strings and integers
  def schema_and_table_oid_in_source?(%Source{} = source, table_schema, table_oid) do
    cond do
      is_list(source.exclude_schemas) and table_schema in source.exclude_schemas -> false
      is_list(source.include_schemas) and table_schema not in source.include_schemas -> false
      is_list(source.exclude_table_oids) and table_oid in source.exclude_table_oids -> false
      is_list(source.include_table_oids) and table_oid not in source.include_table_oids -> false
      true -> true
    end
  end
end

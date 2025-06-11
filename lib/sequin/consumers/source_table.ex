defmodule Sequin.Consumers.SourceTable do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset

  typed_embedded_schema do
    field :table_oid, :integer
    field :schema_name, :string, virtual: true
    field :table_name, :string, virtual: true
    field :group_column_attnums, {:array, :integer}
  end

  def changeset(source_table, attrs) do
    source_table
    |> cast(attrs, [:table_oid, :schema_name, :table_name, :group_column_attnums])
    |> validate_required([:table_oid, :group_column_attnums])
    |> validate_length(:group_column_attnums, min: 1)
  end
end

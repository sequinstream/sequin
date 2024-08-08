defmodule Sequin.Streams.SourceTableStreamTableColumnMapping do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset

  alias Sequin.Streams.SourceTable
  alias Sequin.Streams.StreamTableColumn

  @derive {Jason.Encoder, only: [:id, :source_table_id, :stream_column_id, :mapping, :inserted_at, :updated_at]}
  typed_schema "source_table_stream_table_column_mappings" do
    belongs_to :source_table, SourceTable
    belongs_to :stream_column, StreamTableColumn

    embeds_one :mapping, Mapping, primary_key: false do
      field :type, Ecto.Enum, values: [:record_field, :metadata]
      field :field_name, :string
    end

    timestamps()
  end

  @spec changeset(
          Sequin.Streams.SourceTableStreamTableColumnMapping.t(),
          :invalid | %{optional(:__struct__) => none(), optional(atom() | binary()) => any()}
        ) :: Ecto.Changeset.t()
  def changeset(%__MODULE__{} = mapping, attrs) do
    mapping
    |> cast(attrs, [:source_table_id, :stream_column_id])
    |> cast_embed(:mapping, with: &mapping_changeset/2)
    |> validate_required([:source_table_id, :stream_column_id, :mapping])
    |> foreign_key_constraint(:source_table_id)
    |> foreign_key_constraint(:stream_column_id)
    |> unique_constraint([:source_table_id, :stream_column_id])
  end

  defp mapping_changeset(schema, params) do
    schema
    |> cast(params, [:type, :field_name])
    |> validate_required([:type, :field_name])
    |> validate_inclusion(:type, [:record_field, :metadata])
    |> validate_metadata_field_name()
  end

  defp validate_metadata_field_name(changeset) do
    if get_field(changeset, :type) == :metadata do
      validate_inclusion(changeset, :field_name, ["action", "table_name"])
    else
      changeset
    end
  end
end

defmodule Sequin.Streams.SourceTableStreamTableColumnMapping do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset

  alias Sequin.Streams.SourceTable
  alias Sequin.Streams.StreamTableColumn

  def objects, do: ~w(row transform_output metadata)

  @derive {Jason.Encoder,
           only: [:id, :source_table_id, :stream_column_id, :object, :object_field, :inserted_at, :updated_at]}
  typed_schema "source_table_stream_table_column_mappings" do
    belongs_to :source_table, SourceTable
    belongs_to :stream_column, StreamTableColumn
    field :object, :string
    field :object_field, :string

    timestamps()
  end

  def changeset(%__MODULE__{} = mapping, attrs) do
    mapping
    |> cast(attrs, [:source_table_id, :stream_column_id, :object, :object_field])
    |> validate_required([:source_table_id, :stream_column_id, :object])
    |> foreign_key_constraint(:source_table_id)
    |> foreign_key_constraint(:stream_column_id)
    |> unique_constraint([:source_table_id, :stream_column_id])
    |> validate_inclusion(:object, objects())
  end
end

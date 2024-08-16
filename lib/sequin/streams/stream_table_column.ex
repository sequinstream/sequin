defmodule Sequin.Streams.StreamTableColumn do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset

  alias Sequin.Streams.StreamTable

  @column_types [:text, :integer, :boolean, :timestamp, :uuid]

  def column_types do
    @column_types
  end

  @derive {Jason.Encoder, only: [:id, :name, :type, :is_conflict_key, :stream_table_id, :inserted_at, :updated_at]}
  typed_schema "stream_table_columns" do
    field :name, :string
    field :type, Ecto.Enum, values: @column_types
    field :is_conflict_key, :boolean, default: false

    belongs_to :stream_table, StreamTable

    timestamps()
  end

  def create_changeset(%__MODULE__{} = stream_table_column, attrs) do
    stream_table_column
    |> cast(attrs, [:name, :type, :is_conflict_key, :stream_table_id])
    |> validate_required([:name, :type, :is_conflict_key])
    |> foreign_key_constraint(:stream_table_id)
    |> validate_inclusion(:type, column_types())
    |> unique_constraint([:stream_table_id, :name], message: "has already been taken", error_key: :name)
  end

  def update_changeset(%__MODULE__{} = stream_table_column, attrs) do
    stream_table_column
    |> cast(attrs, [:name])
    |> validate_required([:name])
    |> foreign_key_constraint(:stream_table_id)
    |> unique_constraint([:stream_table_id, :name], message: "has already been taken", error_key: :name)
  end

  # defp base_query(query \\ __MODULE__) do
  #   from(stc in query, as: :stream_table_column)
  # end
end

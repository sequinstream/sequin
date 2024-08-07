defmodule Sequin.Streams.StreamTableColumn do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset

  alias Sequin.Streams.StreamTable

  def column_types do
    [:text, :integer, :boolean, :timestamp]
  end

  @derive {Jason.Encoder, only: [:id, :name, :type, :primary_key, :stream_table_id, :inserted_at, :updated_at]}
  typed_schema "stream_table_columns" do
    field :name, :string
    field :type, :string
    field :primary_key, :boolean, default: false

    belongs_to :stream_table, StreamTable

    timestamps()
  end

  def changeset(%__MODULE__{} = stream_table_column, attrs) do
    stream_table_column
    |> cast(attrs, [:name, :type, :primary_key, :stream_table_id])
    |> validate_required([:name, :type, :primary_key, :stream_table_id])
    |> foreign_key_constraint(:stream_table_id)
    |> validate_inclusion(:type, column_types())
    |> unique_constraint([:stream_table_id, :name])
  end
end

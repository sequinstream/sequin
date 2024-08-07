defmodule Sequin.Streams.StreamTableColumn do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset

  alias Sequin.Streams.StreamTable

  def column_types do
    ~w(text integer boolean timestamp)
  end

  @derive {Jason.Encoder, only: [:id, :name, :type, :is_pk, :stream_table_id, :inserted_at, :updated_at]}
  typed_schema "stream_table_columns" do
    field :name, :string
    field :type, :string
    field :is_pk, :boolean, default: false

    belongs_to :stream_table, StreamTable

    timestamps()
  end

  def changeset(%__MODULE__{} = stream_table_column, attrs) do
    stream_table_column
    |> cast(attrs, [:name, :type, :is_pk, :stream_table_id])
    |> validate_required([:name, :type, :is_pk, :stream_table_id])
    |> foreign_key_constraint(:stream_table_id)
    |> validate_inclusion(:type, column_types())
    |> unique_constraint([:stream_table_id, :name])
  end

  # defp base_query(query \\ __MODULE__) do
  #   from(stc in query, as: :stream_table_column)
  # end
end

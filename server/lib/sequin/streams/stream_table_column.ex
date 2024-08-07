defmodule Sequin.Streams.StreamTableColumn do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset
  import Ecto.Query

  alias Sequin.Streams.StreamTable
  alias Sequin.Streams.StreamTableColumn

  @derive {Jason.Encoder, only: [:id, :stream_table_id, :name, :type, :is_pk, :inserted_at, :updated_at]}
  typed_schema "stream_table_columns" do
    field :name, :string
    field :type, :string
    field :is_pk, :boolean, default: false

    belongs_to :stream_table, StreamTable

    timestamps()
  end

  def changeset(%StreamTableColumn{} = stream_table_column, attrs) do
    stream_table_column
    |> cast(attrs, [:stream_table_id, :name, :type, :is_pk])
    |> validate_required([:stream_table_id, :name, :type, :is_pk])
    |> foreign_key_constraint(:stream_table_id)
    |> unique_constraint([:stream_table_id, :name])
  end

  def where_stream_table_id(query \\ base_query(), stream_table_id) do
    from(stc in query, where: stc.stream_table_id == ^stream_table_id)
  end

  def where_name(query \\ base_query(), name) do
    from(stc in query, where: stc.name == ^name)
  end

  def where_is_pk(query \\ base_query(), is_pk) do
    from(stc in query, where: stc.is_pk == ^is_pk)
  end

  defp base_query(query \\ __MODULE__) do
    from(stc in query, as: :stream_table_column)
  end
end

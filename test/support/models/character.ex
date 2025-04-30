defmodule Sequin.TestSupport.Models.Character do
  @moduledoc false
  use Ecto.Schema

  import Ecto.Query

  alias Sequin.Postgres

  schema "Characters" do
    field :name, :string
    field :house, :string
    field :planet, :string
    field :is_active, :boolean
    field :tags, {:array, :string}
    field :metadata, :map

    timestamps(type: :naive_datetime_usec)
  end

  def quoted_table_name do
    ~s{"Characters"}
  end

  def where_id(query \\ base_query(), id) do
    from(c in query, where: c.id == ^id)
  end

  def column_attnums do
    from(pg in "pg_attribute",
      # Filter out system columns
      where: pg.attrelid == ^table_oid() and pg.attnum > 0,
      select: {pg.attname, pg.attnum}
    )
    |> Sequin.Repo.all()
    |> Map.new()
  end

  def pk_attnums do
    [column_attnum("id")]
  end

  def record_pks(%__MODULE__{} = character) do
    [character.id]
  end

  def column_attnum(column_name) do
    Map.fetch!(column_attnums(), column_name)
  end

  def table_oid do
    Postgres.ecto_model_oid(__MODULE__)
  end

  defp base_query(query \\ __MODULE__) do
    from(c in query, as: :character)
  end

  def changeset(character, attrs) do
    Ecto.Changeset.cast(character, attrs, [:name, :house, :planet, :is_active, :tags, :metadata])
  end
end

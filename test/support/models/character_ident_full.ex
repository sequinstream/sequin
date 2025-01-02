defmodule Sequin.Test.Support.Models.CharacterIdentFull do
  @moduledoc false
  use Ecto.Schema

  import Ecto.Query

  alias Sequin.Postgres

  schema "characters_ident_full" do
    field :name, :string
    field :house, :string
    field :planet, :string
    field :is_active, :boolean
    field :tags, {:array, :string}

    timestamps()
  end

  def table_oid do
    Postgres.ecto_model_oid(__MODULE__)
  end

  def record_pks(%__MODULE__{} = character_ident_full) do
    [character_ident_full.id]
  end

  def where_id(query \\ base_query(), id) do
    from(cf in query, where: cf.id == ^id)
  end

  defp base_query(query \\ __MODULE__) do
    from(cf in query, as: :character_ident_full)
  end

  def changeset(character, attrs) do
    Ecto.Changeset.cast(character, attrs, [:name, :house, :planet, :is_active, :tags])
  end

  def column_attnums do
    from(pg in "pg_attribute",
      where: pg.attrelid == ^table_oid() and pg.attnum > 0,
      select: {pg.attname, pg.attnum}
    )
    |> Sequin.Repo.all()
    |> Map.new()
  end

  def pk_attnums do
    [column_attnum("id")]
  end

  def column_attnum(column_name) do
    Map.fetch!(column_attnums(), column_name)
  end
end

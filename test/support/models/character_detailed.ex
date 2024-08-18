defmodule Sequin.Test.Support.Models.CharacterDetailed do
  @moduledoc false
  use Ecto.Schema

  import Ecto.Query

  schema "characters_detailed" do
    field :name, :string
    field :age, :integer
    field :height, :float
    field :is_hero, :boolean
    field :biography, :string
    field :birth_date, :date
    field :last_seen, :time
    field :created_at, :naive_datetime
    field :updated_at, :utc_datetime
    field :powers, {:array, :string}
    field :metadata, :map
    field :rating, :decimal
    field :avatar, :binary
  end

  def table_oid do
    Sequin.Repo.one(
      from(pg in "pg_class",
        where: pg.relname == "characters_detailed",
        select: pg.oid
      )
    )
  end

  def where_id(query \\ base_query(), id) do
    from(c in query, where: c.id == ^id)
  end

  defp base_query(query \\ __MODULE__) do
    from(c in query, as: :character_detailed)
  end

  def changeset(character, attrs) do
    Ecto.Changeset.cast(character, attrs, [
      :name,
      :age,
      :height,
      :is_hero,
      :biography,
      :birth_date,
      :last_seen,
      :created_at,
      :updated_at,
      :powers,
      :metadata,
      :rating,
      :avatar
    ])
  end
end

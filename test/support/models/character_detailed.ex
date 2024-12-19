defmodule Sequin.Test.Support.Models.CharacterDetailed do
  @moduledoc false
  use Ecto.Schema

  import Ecto.Query

  alias Sequin.Postgres

  schema "characters_detailed" do
    field :name, :string
    field :age, :integer
    field :height, :float
    field :is_hero, :boolean
    field :biography, :string
    field :birth_date, :date
    field :last_seen, :time
    field :powers, {:array, :string}
    field :metadata, :map
    field :rating, :decimal
    field :avatar, :binary
    field :house_id, Ecto.UUID
    field :email, :string
    field :binary_data, :binary
    field :related_houses, {:array, Ecto.UUID}

    timestamps()
  end

  def table_oid do
    Postgres.ecto_model_oid(__MODULE__)
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

  def record_pks(%__MODULE__{} = character_detailed) do
    [character_detailed.id]
  end

  def column_attnum(column_name) do
    Map.fetch!(column_attnums(), column_name)
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
      :powers,
      :metadata,
      :rating,
      :avatar,
      :house_id,
      :email,
      :binary_data,
      :related_houses
    ])
  end
end

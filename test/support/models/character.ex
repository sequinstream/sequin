defmodule Sequin.Test.Support.Models.Character do
  @moduledoc false
  use Ecto.Schema

  import Ecto.Query

  schema "characters" do
    field :name, :string
    field :house, :string
    field :planet, :string
    field :is_active, :boolean
    field :tags, {:array, :string}
  end

  def where_id(query \\ base_query(), id) do
    from(c in query, where: c.id == ^id)
  end

  def column_attnums do
    from(pg in "pg_attribute",
      where: pg.attrelid == fragment("'characters'::regclass") and pg.attnum > 0,
      select: {pg.attname, pg.attnum}
    )
    |> Sequin.Repo.all()
    |> Map.new()
  end

  def column_attnum(column_name) do
    Map.fetch!(column_attnums(), column_name)
  end

  def table_oid do
    Sequin.Repo.one(
      from(pg in "pg_class",
        where: pg.relname == "characters",
        select: pg.oid
      )
    )
  end

  defp base_query(query \\ __MODULE__) do
    from(c in query, as: :character)
  end
end

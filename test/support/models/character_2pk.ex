defmodule Sequin.Test.Support.Models.Character2PK do
  @moduledoc false
  use Ecto.Schema

  import Ecto.Query

  @primary_key false
  schema "characters_2pk" do
    field :id1, :integer, primary_key: true
    field :id2, :integer, primary_key: true
    field :name, :string
    field :house, :string
    field :planet, :string
  end

  def table_oid do
    Sequin.Repo.one(
      from(pg in "pg_class",
        where: pg.relname == "characters_2pk",
        select: pg.oid
      )
    )
  end

  def where_id(query \\ base_query(), id1, id2) do
    from(c2 in query, where: c2.id1 == ^id1 and c2.id2 == ^id2)
  end

  defp base_query(query \\ __MODULE__) do
    from(c2 in query, as: :character_2pk)
  end
end

defmodule Sequin.Test.Support.Models.CharacterMultiPK do
  @moduledoc false
  use Ecto.Schema

  import Ecto.Query

  @primary_key false
  schema "characters_multi_pk" do
    field :id_integer, :integer, primary_key: true
    field :id_string, :string, primary_key: true
    field :id_uuid, Ecto.UUID, primary_key: true
    field :name, :string
  end

  def table_oid do
    Sequin.Repo.one(
      from(pg in "pg_class",
        where: pg.relname == "characters_multi_pk",
        select: pg.oid
      )
    )
  end

  def where_id(query \\ base_query(), id_integer, id_string, id_uuid) do
    from(c in query, where: c.id_integer == ^id_integer and c.id_string == ^id_string and c.id_uuid == ^id_uuid)
  end

  defp base_query(query \\ __MODULE__) do
    from(c in query, as: :character_multi_pk)
  end
end
defmodule Sequin.TestSupport.Models.CharacterMultiPK do
  @moduledoc false
  use Ecto.Schema

  import Ecto.Query

  alias Sequin.Postgres

  @primary_key false
  schema "characters_multi_pk" do
    field :id_integer, :integer, primary_key: true, read_after_writes: true
    field :id_string, :string, primary_key: true
    field :id_uuid, Ecto.UUID, primary_key: true
    field :name, :string
    field :house, :string

    timestamps()
  end

  def table_oid do
    Postgres.ecto_model_oid(__MODULE__)
  end

  def record_pks(%__MODULE__{} = character_multi_pk) do
    [character_multi_pk.id_integer, character_multi_pk.id_string, character_multi_pk.id_uuid]
  end

  def where_id(query \\ base_query(), id_integer, id_string, id_uuid) do
    from(c in query, where: c.id_integer == ^id_integer and c.id_string == ^id_string and c.id_uuid == ^id_uuid)
  end

  defp base_query(query \\ __MODULE__) do
    from(c in query, as: :character_multi_pk)
  end

  def changeset(character, attrs) do
    Ecto.Changeset.cast(character, attrs, [:id_integer, :id_string, :id_uuid, :name, :house])
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
    Enum.map(["id_integer", "id_string", "id_uuid"], &column_attnum/1)
  end

  def column_attnum(column_name) do
    Map.fetch!(column_attnums(), column_name)
  end
end

defmodule Sequin.Test.Support.Models.CharacterMultiPK do
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

    timestamps()
  end

  def table_oid do
    Postgres.ecto_model_oid(__MODULE__)
  end

  def where_id(query \\ base_query(), id_integer, id_string, id_uuid) do
    from(c in query, where: c.id_integer == ^id_integer and c.id_string == ^id_string and c.id_uuid == ^id_uuid)
  end

  defp base_query(query \\ __MODULE__) do
    from(c in query, as: :character_multi_pk)
  end

  def changeset(character, attrs) do
    Ecto.Changeset.cast(character, attrs, [:id_integer, :id_string, :id_uuid, :name])
  end
end

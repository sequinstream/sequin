defmodule Sequin.Test.Support.Models.CharactersRestricted do
  @moduledoc false
  use Ecto.Schema

  import Ecto.Changeset

  @schema_prefix :restricted
  schema "characters_restricted" do
    field :name, :string

    timestamps()
  end

  def changeset(character, attrs) do
    character
    |> cast(attrs, [:name])
    |> validate_required([:name])
  end
end

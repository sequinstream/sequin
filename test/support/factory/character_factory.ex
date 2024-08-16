defmodule Sequin.Factory.CharacterFactory do
  @moduledoc false
  alias Sequin.Test.Support.Models.Character
  alias Sequin.Test.Support.Models.Character2PK
  alias Sequin.Test.Support.Models.CharacterIdentFull
  alias Sequin.Test.UnboxedRepo

  def insert_character!(attrs \\ []) do
    attrs = Map.new(attrs)

    attrs =
      Map.merge(
        %{
          name: Faker.Person.name(),
          house: Faker.Team.name(),
          planet: Faker.Airports.name(),
          is_active: Enum.random([true, false]),
          tags: Enum.take_random(["warrior", "seer", "royal", "compound"], Enum.random(1..3))
        },
        attrs
      )

    %Character{}
    |> Ecto.Changeset.cast(attrs, [:name, :house, :planet, :is_active, :tags])
    |> UnboxedRepo.insert!()
  end

  def insert_character_ident_full!(attrs \\ []) do
    attrs = Map.new(attrs)

    attrs =
      Map.merge(
        %{
          name: Faker.Person.name(),
          house: Faker.Team.name(),
          planet: Faker.Airports.name(),
          is_active: Enum.random([true, false]),
          tags: Enum.take_random(["warrior", "seer", "royal", "compound"], Enum.random(1..3))
        },
        attrs
      )

    %CharacterIdentFull{}
    |> Ecto.Changeset.cast(attrs, [:name, :house, :planet, :is_active, :tags])
    |> UnboxedRepo.insert!()
  end

  def insert_character_2pk!(attrs \\ []) do
    attrs = Map.new(attrs)

    attrs =
      Map.merge(
        %{
          id1: :rand.uniform(1000),
          id2: :rand.uniform(1000),
          name: Faker.Person.name(),
          house: Faker.Team.name(),
          planet: Faker.Airports.name()
        },
        attrs
      )

    %Character2PK{}
    |> Ecto.Changeset.cast(attrs, [:id1, :id2, :name, :house, :planet])
    |> UnboxedRepo.insert!()
  end
end

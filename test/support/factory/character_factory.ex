defmodule Sequin.Factory.CharacterFactory do
  @moduledoc false
  alias Sequin.Factory
  alias Sequin.Test.Support.Models.Character
  alias Sequin.Test.Support.Models.CharacterDetailed
  alias Sequin.Test.Support.Models.CharacterIdentFull
  alias Sequin.Test.Support.Models.CharacterMultiPK
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
    |> Character.changeset(attrs)
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
    |> CharacterIdentFull.changeset(attrs)
    |> UnboxedRepo.insert!()
  end

  def insert_character_multi_pk!(attrs \\ []) do
    attrs = Map.new(attrs)

    attrs =
      Map.merge(
        %{
          # Omit id_integer, allow it to be auto-generated, so it's auto-incremented
          id_string: Faker.UUID.v4(),
          id_uuid: Faker.UUID.v4(),
          name: Faker.Person.name()
        },
        attrs
      )

    %CharacterMultiPK{}
    |> CharacterMultiPK.changeset(attrs)
    |> UnboxedRepo.insert!()
  end

  def insert_character_detailed!(attrs \\ []) do
    attrs = Map.new(attrs)

    attrs =
      Map.merge(
        %{
          name: Factory.name(),
          age: Factory.integer(),
          height: Factory.float(min: 1, max: 3),
          is_hero: Factory.boolean(),
          biography: Faker.Lorem.paragraph(),
          birth_date: Factory.date(),
          last_seen: Factory.naive_timestamp(),
          created_at: Factory.naive_timestamp(),
          updated_at: Factory.utc_datetime(),
          powers: Enum.take_random(["flight", "strength", "invisibility", "telepathy"], Enum.random(1..3)),
          metadata: %{
            "origin" => Faker.Address.country(),
            "alignment" => Factory.one_of(["good", "neutral", "evil"])
          },
          rating: Decimal.from_float(Factory.float(max: 10)),
          avatar: :crypto.strong_rand_bytes(16)
        },
        attrs
      )

    %CharacterDetailed{}
    |> CharacterDetailed.changeset(attrs)
    |> UnboxedRepo.insert!()
  end
end

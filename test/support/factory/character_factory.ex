defmodule Sequin.Factory.CharacterFactory do
  @moduledoc false
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

  def insert_character_multi_pk!(attrs \\ []) do
    attrs = Map.new(attrs)

    attrs =
      Map.merge(
        %{
          id_integer: :rand.uniform(1000),
          id_string: Faker.UUID.v4(),
          id_uuid: Ecto.UUID.generate(),
          name: Faker.Person.name()
        },
        attrs
      )

    %CharacterMultiPK{}
    |> Ecto.Changeset.cast(attrs, [:id_integer, :id_string, :id_uuid, :name])
    |> UnboxedRepo.insert!()
  end

  def insert_character_detailed!(attrs \\ []) do
    attrs = Map.new(attrs)

    attrs =
      Map.merge(
        %{
          name: Faker.Person.name(),
          age: Enum.random(18..100),
          # 1-3 meters
          height: :rand.uniform() * 2 + 1,
          is_hero: Enum.random([true, false]),
          biography: Faker.Lorem.paragraph(),
          # Up to 100 years ago
          birth_date: Faker.Date.backward(36_500),
          # Random time in a day
          last_seen: Time.add(~T[00:00:00], :rand.uniform(86_400)),
          created_at: NaiveDateTime.truncate(NaiveDateTime.utc_now(), :second),
          updated_at: DateTime.truncate(DateTime.utc_now(), :second),
          powers: Enum.take_random(["flight", "strength", "invisibility", "telepathy"], Enum.random(1..3)),
          metadata: %{
            "origin" => Faker.Address.country(),
            "alignment" => Enum.random(["good", "neutral", "evil"])
          },
          rating: Decimal.from_float(:rand.uniform() * 10),
          # 16 random bytes
          avatar: :crypto.strong_rand_bytes(16)
        },
        attrs
      )

    %CharacterDetailed{}
    |> Ecto.Changeset.cast(attrs, [
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
    |> UnboxedRepo.insert!()
  end
end

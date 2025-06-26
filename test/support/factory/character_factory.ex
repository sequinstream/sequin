defmodule Sequin.Factory.CharacterFactory do
  @moduledoc false
  import Sequin.Factory.Support

  alias Sequin.Factory
  alias Sequin.Repo
  alias Sequin.TestSupport.Models.Character
  alias Sequin.TestSupport.Models.CharacterDetailed
  alias Sequin.TestSupport.Models.CharacterIdentFull
  alias Sequin.TestSupport.Models.CharacterMultiPK

  def character(attrs \\ []) do
    attrs = Map.new(attrs)

    merge_attributes(
      %Character{
        id: Factory.uuid(),
        name: Faker.Person.name(),
        house: Faker.Team.name(),
        planet: Faker.Airports.name(),
        is_active: Enum.random([true, false]),
        tags: Enum.take_random(["warrior", "seer", "royal", "compound"], Enum.random(1..3)),
        metadata: %{
          "origin" => Faker.Address.country(),
          "alignment" => Factory.one_of(["good", "neutral", "evil"])
        },
        inserted_at: Factory.naive_timestamp(),
        updated_at: Factory.naive_timestamp()
      },
      attrs
    )
  end

  def character_attrs(attrs \\ []) do
    attrs
    |> character()
    |> Sequin.Map.from_ecto()
    |> Map.new(fn {k, v} -> {to_string(k), v} end)
  end

  def insert_character!(attrs \\ [], opts \\ []) do
    repo = Keyword.get(opts, :repo, Repo)
    attrs = character_attrs(attrs)

    %Character{}
    |> Character.changeset(attrs)
    |> repo.insert!()
  end

  def character_ident_full(attrs \\ []) do
    attrs = Map.new(attrs)

    merge_attributes(
      %CharacterIdentFull{
        name: Faker.Person.name(),
        house: Faker.Team.name(),
        planet: Faker.Airports.name(),
        is_active: Enum.random([true, false]),
        tags: Enum.take_random(["warrior", "seer", "royal", "compound"], Enum.random(1..3))
      },
      attrs
    )
  end

  def character_ident_full_attrs(attrs \\ []) do
    attrs
    |> character_ident_full()
    |> Sequin.Map.from_ecto()
  end

  def insert_character_ident_full!(attrs \\ [], opts \\ []) do
    repo = Keyword.get(opts, :repo, Repo)
    attrs = character_ident_full_attrs(attrs)

    %CharacterIdentFull{}
    |> CharacterIdentFull.changeset(attrs)
    |> repo.insert!()
  end

  def character_multi_pk(attrs \\ []) do
    attrs = Map.new(attrs)

    merge_attributes(
      %CharacterMultiPK{
        id_string: "string-" <> Faker.UUID.v4(),
        id_uuid: Faker.UUID.v4(),
        name: Faker.Person.name(),
        house: Faker.Team.name()
      },
      attrs
    )
  end

  def character_multi_pk_attrs(attrs \\ []) do
    attrs
    |> character_multi_pk()
    |> Sequin.Map.from_ecto()
  end

  def insert_character_multi_pk!(attrs \\ [], opts \\ []) do
    repo = Keyword.get(opts, :repo, Repo)
    attrs = character_multi_pk_attrs(attrs)

    %CharacterMultiPK{}
    |> CharacterMultiPK.changeset(attrs)
    |> repo.insert!()
  end

  def character_detailed(attrs \\ []) do
    attrs = Map.new(attrs)

    merge_attributes(
      %CharacterDetailed{
        id: Factory.uuid(),
        name: Factory.name(),
        status: Factory.one_of([:active, :inactive, :retired]),
        power_level: Factory.non_neg_integer(),
        age: Factory.non_neg_integer(),
        height: Factory.float(min: 1, max: 3),
        is_hero: Factory.boolean(),
        biography: Faker.Lorem.paragraph(),
        birth_date: Factory.date(),
        last_seen: Time.truncate(Time.utc_now(), :second),
        powers: Enum.take_random(["flight", "strength", "invisibility", "telepathy"], Enum.random(1..3)),
        metadata: %{
          "origin" => Faker.Address.country(),
          "alignment" => Factory.one_of(["good", "neutral", "evil"])
        },
        rating: Decimal.from_float(Factory.float(max: 10)),
        avatar: :crypto.strong_rand_bytes(16),
        house_id: UUID.uuid4(),
        email: Faker.Internet.email(),
        binary_data: :crypto.strong_rand_bytes(32),
        related_houses: Enum.map(1..3, fn _ -> UUID.uuid4() end),
        active_period: [~D[2020-01-01], ~D[2023-12-31]],
        embedding: [Factory.float(), Factory.float(), Factory.float()]
      },
      attrs
    )
  end

  def character_detailed_attrs(attrs \\ []) do
    attrs
    |> character_detailed()
    |> Sequin.Map.from_ecto()
  end

  def insert_character_detailed!(attrs \\ [], opts \\ []) do
    repo = Keyword.get(opts, :repo, Repo)
    attrs = character_detailed_attrs(attrs)

    %CharacterDetailed{}
    |> CharacterDetailed.changeset(attrs)
    |> repo.insert!()
  end
end

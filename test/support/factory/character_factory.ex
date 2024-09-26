if Mix.env() == :test do
  defmodule Sequin.Factory.CharacterFactory do
    @moduledoc false
    import Sequin.Factory.Support

    alias Sequin.Factory
    alias Sequin.Repo
    alias Sequin.Test.Support.Models.Character
    alias Sequin.Test.Support.Models.CharacterDetailed
    alias Sequin.Test.Support.Models.CharacterIdentFull
    alias Sequin.Test.Support.Models.CharacterMultiPK
    alias Sequin.Test.Support.Models.CharactersRestricted

    def character(attrs \\ []) do
      attrs = Map.new(attrs)

      merge_attributes(
        %Character{
          name: Faker.Person.name(),
          house: Faker.Team.name(),
          planet: Faker.Airports.name(),
          is_active: Enum.random([true, false]),
          tags: Enum.take_random(["warrior", "seer", "royal", "compound"], Enum.random(1..3)),
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
          id_string: Faker.UUID.v4(),
          id_uuid: Faker.UUID.v4(),
          name: Faker.Person.name()
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
          name: Factory.name(),
          age: Factory.integer(),
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
          avatar: :crypto.strong_rand_bytes(16)
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

    def characters_restricted(attrs \\ []) do
      attrs = Map.new(attrs)

      merge_attributes(
        %CharactersRestricted{
          name: Faker.Person.name()
        },
        attrs
      )
    end

    def characters_restricted_attrs(attrs \\ []) do
      attrs
      |> characters_restricted()
      |> Sequin.Map.from_ecto()
    end

    def insert_characters_restricted!(attrs \\ [], opts \\ []) do
      repo = Keyword.get(opts, :repo, Repo)
      attrs = characters_restricted_attrs(attrs)

      %CharactersRestricted{}
      |> CharactersRestricted.changeset(attrs)
      |> repo.insert!()
    end
  end
end

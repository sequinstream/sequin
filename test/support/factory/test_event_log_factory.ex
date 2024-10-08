if Mix.env() == :test do
  defmodule Sequin.Factory.TestEventLogFactory do
    @moduledoc false
    import Sequin.Factory.Support

    alias Sequin.Factory
    alias Sequin.Repo
    alias Sequin.Test.Support.Models.TestEventLog

    def test_event_log(attrs \\ []) do
      attrs = Map.new(attrs)

      merge_attributes(
        %TestEventLog{
          seq: Factory.integer(),
          source_database_id: Faker.UUID.v4(),
          source_table_schema: Factory.postgres_object(),
          source_table_name: Factory.postgres_object(),
          source_table_oid: Factory.integer(),
          record_pk: Faker.UUID.v4(),
          record: %{
            "field1" => Faker.Lorem.word(),
            "field2" => Faker.Lorem.sentence()
          },
          changes: %{
            "field1" => Faker.Lorem.word(),
            "field2" => Faker.Lorem.sentence()
          },
          action: Factory.one_of(["insert", "update", "delete"]),
          committed_at: Factory.utc_datetime(),
          inserted_at: Factory.utc_datetime()
        },
        attrs
      )
    end

    def test_event_log_attrs(attrs \\ []) do
      attrs
      |> test_event_log()
      |> Sequin.Map.from_ecto()
    end

    def insert_test_event_log!(attrs \\ [], opts \\ []) do
      repo = Keyword.get(opts, :repo, Repo)
      attrs = test_event_log_attrs(attrs)

      %TestEventLog{}
      |> TestEventLog.changeset(attrs)
      |> repo.insert!()
    end
  end
end

defmodule Sequin.Factory.ReplicationFactory do
  @moduledoc false
  import Sequin.Factory.Support

  alias Sequin.Extensions.PostgresAdapter.Changes.DeletedRecord
  alias Sequin.Extensions.PostgresAdapter.Changes.NewRecord
  alias Sequin.Extensions.PostgresAdapter.Changes.UpdatedRecord
  alias Sequin.Factory
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.StreamsFactory
  alias Sequin.Replication.PostgresReplicationSlot
  alias Sequin.Repo

  def postgres_replication(attrs \\ []) do
    attrs = Map.new(attrs)

    merge_attributes(
      %PostgresReplicationSlot{
        postgres_database_id: Factory.uuid(),
        publication_name: "pub_#{Factory.name()}",
        slot_name: "slot_#{Factory.name()}"
      },
      attrs
    )
  end

  def postgres_replication_attrs(attrs \\ []) do
    attrs
    |> postgres_replication()
    |> Sequin.Map.from_ecto()
  end

  def insert_postgres_replication!(attrs \\ []) do
    attrs = Map.new(attrs)

    {account_id, attrs} = Map.pop_lazy(attrs, :account_id, fn -> AccountsFactory.insert_account!().id end)

    {postgres_database_id, attrs} =
      Map.pop_lazy(attrs, :postgres_database_id, fn ->
        DatabasesFactory.insert_postgres_database!(account_id: account_id).id
      end)

    {stream_id, attrs} =
      Map.pop_lazy(attrs, :stream_id, fn -> StreamsFactory.insert_stream!(account_id: account_id).id end)

    attrs
    |> Map.put(:account_id, account_id)
    |> Map.put(:postgres_database_id, postgres_database_id)
    |> Map.put(:stream_id, stream_id)
    |> postgres_replication()
    |> Repo.insert!()
  end

  def insert_stream!(attrs \\ []) do
    attrs = Map.new(attrs)
    {account_id, attrs} = Map.pop_lazy(attrs, :account_id, fn -> AccountsFactory.insert_account!().id end)

    attrs
    |> Map.put(:account_id, account_id)
    |> StreamsFactory.stream()
    |> Repo.insert!()
  end

  def postgres_insert(attrs \\ []) do
    attrs = Map.new(attrs)

    merge_attributes(
      %NewRecord{
        commit_timestamp: Factory.timestamp(),
        errors: nil,
        ids: [Factory.unique_integer()],
        schema: "__postgres_replication_test_schema__",
        table: "__postgres_replication_test_table__",
        record: %{
          "id" => Factory.unique_integer(),
          "name" => Factory.name(),
          "house" => Factory.name(),
          "planet" => Factory.name()
        },
        type: "insert"
      },
      attrs
    )
  end

  def postgres_update(attrs \\ []) do
    attrs = Map.new(attrs)

    merge_attributes(
      %UpdatedRecord{
        commit_timestamp: Factory.timestamp(),
        errors: nil,
        ids: [Factory.unique_integer()],
        schema: Factory.postgres_object(),
        table: Factory.postgres_object(),
        old_record: nil,
        record: %{
          "id" => Factory.unique_integer(),
          "name" => Factory.name(),
          "house" => Factory.name(),
          "planet" => Factory.name()
        },
        type: "update"
      },
      attrs
    )
  end

  def postgres_delete(attrs \\ []) do
    attrs = Map.new(attrs)

    merge_attributes(
      %DeletedRecord{
        commit_timestamp: Factory.timestamp(),
        errors: nil,
        ids: [Factory.unique_integer()],
        schema: Factory.postgres_object(),
        table: Factory.postgres_object(),
        old_record: %{
          "id" => Factory.unique_integer(),
          "name" => nil,
          "house" => nil,
          "planet" => nil
        },
        type: "delete"
      },
      attrs
    )
  end
end

defmodule Sequin.Factory.ReplicationFactory do
  @moduledoc false
  import Sequin.Factory.Support

  alias Sequin.Extensions.PostgresAdapter.Decoder.Messages.Relation
  alias Sequin.Factory
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.StreamsFactory
  alias Sequin.Replication.Message
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

    attrs
    |> Map.put(:account_id, account_id)
    |> Map.put(:postgres_database_id, postgres_database_id)
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

  def postgres_message(attrs \\ []) do
    case attrs[:action] || Enum.random([:insert, :update, :delete]) do
      :insert -> postgres_insert(attrs)
      :update -> postgres_update(attrs)
      :delete -> postgres_delete(attrs)
    end
  end

  def field(attrs \\ []) do
    attrs = Map.new(attrs)

    merge_attributes(
      %Message.Field{
        column_name: Factory.postgres_object(),
        column_attnum: Factory.unique_integer(),
        value: Factory.name()
      },
      attrs
    )
  end

  def postgres_insert(attrs \\ []) do
    attrs = Map.new(attrs)

    merge_attributes(
      %Message{
        action: :insert,
        commit_timestamp: Factory.timestamp(),
        errors: nil,
        ids: [Factory.unique_integer()],
        table_schema: "__postgres_replication_test_schema__",
        table_name: "__postgres_replication_test_table__",
        table_oid: Factory.unique_integer(),
        fields: [
          field(column_name: "id", value: Factory.unique_integer()),
          field(column_name: "name", value: Factory.name()),
          field(column_name: "house", value: Factory.name()),
          field(column_name: "planet", value: Factory.name())
        ]
      },
      attrs
    )
  end

  def postgres_update(attrs \\ []) do
    attrs = Map.new(attrs)

    merge_attributes(
      %Message{
        action: :update,
        commit_timestamp: Factory.timestamp(),
        errors: nil,
        ids: [Factory.unique_integer()],
        table_schema: Factory.postgres_object(),
        table_name: Factory.postgres_object(),
        table_oid: Factory.unique_integer(),
        old_fields: [
          field(column_name: "name", value: "old_name")
        ],
        fields: [
          field(column_name: "id", value: Factory.unique_integer()),
          field(column_name: "name", value: Factory.name()),
          field(column_name: "house", value: Factory.name()),
          field(column_name: "planet", value: Factory.name())
        ]
      },
      attrs
    )
  end

  def postgres_delete(attrs \\ []) do
    attrs = Map.new(attrs)

    merge_attributes(
      %Message{
        action: :delete,
        commit_timestamp: Factory.timestamp(),
        errors: nil,
        ids: [Factory.unique_integer()],
        table_schema: Factory.postgres_object(),
        table_name: Factory.postgres_object(),
        table_oid: Factory.unique_integer(),
        old_fields: [
          field(column_name: "id", value: Factory.unique_integer()),
          field(column_name: "name", value: nil),
          field(column_name: "house", value: nil),
          field(column_name: "planet", value: nil)
        ]
      },
      attrs
    )
  end

  def relation(attrs \\ []) do
    attrs = Map.new(attrs)

    merge_attributes(
      %Relation{
        id: Factory.unique_integer(),
        namespace: Factory.postgres_object(),
        name: Factory.postgres_object(),
        replica_identity: Enum.random([:default, :nothing, :all_columns, :index]),
        columns: [relation_column()]
      },
      attrs
    )
  end

  def relation_column(attrs \\ []) do
    attrs = Map.new(attrs)

    merge_attributes(
      %Relation.Column{
        flags: Enum.random([[:key], []]),
        name: Factory.postgres_object(),
        type: Enum.random(["int8", "text", "timestamp", "bool"]),
        pk?: Enum.random([true, false]),
        type_modifier: Factory.unique_integer(),
        attnum: Factory.unique_integer()
      },
      attrs
    )
  end
end

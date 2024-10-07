defmodule Sequin.Factory.ReplicationFactory do
  @moduledoc false
  import Sequin.Factory.Support

  alias Sequin.Extensions.PostgresAdapter.Decoder.Messages.Relation
  alias Sequin.Factory
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Replication.Message
  alias Sequin.Replication.PostgresReplicationSlot
  alias Sequin.Replication.WalEvent
  alias Sequin.Replication.WalPipeline
  alias Sequin.Repo

  def commit_lsn, do: Factory.unique_integer()

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
        trace_id: Factory.uuid(),
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
        trace_id: Factory.uuid(),
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
        trace_id: Factory.uuid(),
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

  def wal_pipeline(attrs \\ []) do
    attrs = Map.new(attrs)

    merge_attributes(
      %WalPipeline{
        name: "wal_pipeline_#{Factory.sequence()}",
        seq: Factory.sequence(),
        source_tables: [ConsumersFactory.source_table()],
        replication_slot_id: Factory.uuid(),
        destination_oid: Factory.unique_integer(),
        destination_database_id: Factory.uuid(),
        account_id: Factory.uuid(),
        status: Factory.one_of([:active, :disabled])
      },
      attrs
    )
  end

  def wal_pipeline_attrs(attrs \\ []) do
    attrs
    |> wal_pipeline()
    |> Sequin.Map.from_ecto()
    |> Map.update!(:source_tables, fn source_tables ->
      Enum.map(source_tables, fn source_table ->
        source_table
        |> Sequin.Map.from_ecto()
        |> Map.update!(:column_filters, fn column_filters ->
          Enum.map(column_filters, &Sequin.Map.from_ecto/1)
        end)
      end)
    end)
  end

  def insert_wal_pipeline!(attrs \\ []) do
    attrs = Map.new(attrs)

    {account_id, attrs} =
      Map.pop_lazy(attrs, :account_id, fn ->
        AccountsFactory.insert_account!().id
      end)

    {replication_slot_id, attrs} =
      Map.pop_lazy(attrs, :replication_slot_id, fn ->
        insert_postgres_replication!(account_id: account_id).id
      end)

    {destination_database_id, attrs} =
      Map.pop_lazy(attrs, :destination_database_id, fn ->
        DatabasesFactory.insert_postgres_database!(account_id: account_id).id
      end)

    attrs =
      attrs
      |> Map.put(:replication_slot_id, replication_slot_id)
      |> Map.put(:destination_database_id, destination_database_id)
      |> wal_pipeline_attrs()

    %WalPipeline{account_id: account_id}
    |> WalPipeline.create_changeset(attrs)
    |> Repo.insert!()
  end

  def wal_event(attrs \\ []) do
    attrs = Map.new(attrs)

    {action, attrs} = Map.pop_lazy(attrs, :action, fn -> Enum.random([:insert, :update, :delete]) end)

    {record_pks, attrs} = Map.pop_lazy(attrs, :record_pks, fn -> [Faker.UUID.v4()] end)
    record_pks = Enum.map(record_pks, &to_string/1)

    merge_attributes(
      %WalEvent{
        wal_pipeline_id: Factory.uuid(),
        commit_lsn: Factory.unique_integer(),
        record_pks: record_pks,
        record: %{"column" => Factory.word()},
        changes: if(action == :update, do: %{"column" => Factory.word()}),
        action: action,
        committed_at: Factory.timestamp(),
        replication_message_trace_id: Factory.uuid(),
        source_table_oid: Factory.unique_integer()
      },
      attrs
    )
  end

  def wal_event_attrs(attrs \\ []) do
    attrs
    |> Map.new()
    |> wal_event()
    |> Sequin.Map.from_ecto()
  end

  def insert_wal_event!(attrs \\ []) do
    attrs = Map.new(attrs)

    {wal_pipeline_id, attrs} =
      Map.pop_lazy(attrs, :wal_pipeline_id, fn -> insert_wal_pipeline!().id end)

    attrs
    |> Map.put(:wal_pipeline_id, wal_pipeline_id)
    |> wal_event_attrs()
    |> then(&WalEvent.create_changeset(%WalEvent{}, &1))
    |> Repo.insert!()
  end
end

defmodule Sequin.Factory.ReplicationFactory do
  @moduledoc false
  import Sequin.Factory.Support

  alias Sequin.Factory
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Replication.PostgresReplicationSlot
  alias Sequin.Replication.WalEvent
  alias Sequin.Replication.WalPipeline
  alias Sequin.Repo
  alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.LogicalMessage
  alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.Relation
  alias Sequin.Runtime.SlotProcessor
  alias Sequin.Runtime.SlotProducer
  alias Sequin.WalPipeline.SourceTable.ColumnFilter

  def commit_lsn, do: Factory.unique_integer()

  def postgres_replication(attrs \\ []) do
    attrs = Map.new(attrs)

    merge_attributes(
      %PostgresReplicationSlot{
        postgres_database_id: Factory.uuid(),
        publication_name: "pub_#{Factory.name()}",
        slot_name: "slot_#{Factory.name()}",
        status: Factory.one_of([:active, :disabled]),
        partition_count: Enum.random(1..10)
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

  @doc """
  Returns attributes for a pre-configured replication slot used in tests.

  This uses the slot name defined in `Sequin.TestSupport.ReplicationSlots` for the factory module
  and a known publication name.
  """
  def configured_postgres_replication_attrs(attrs \\ []) do
    attrs = Map.new(attrs)

    merge_attributes(
      %{
        slot_name: Sequin.TestSupport.ReplicationSlots.slot_name(Sequin.Factory.ReplicationFactory),
        # This is the name of the publication in the test database.
        # See the "CreateTestTables" migration.
        publication_name: "characters_publication"
      },
      attrs
    )
  end

  @doc """
  Inserts a `PostgresReplicationSlot` record pointing to a pre-configured replication slot.
  """
  def insert_configured_postgres_replication!(attrs \\ []) do
    attrs = Map.new(attrs)

    {account_id, attrs} = Map.pop_lazy(attrs, :account_id, fn -> AccountsFactory.insert_account!().id end)

    {postgres_database_id, attrs} =
      Map.pop_lazy(attrs, :postgres_database_id, fn ->
        DatabasesFactory.insert_postgres_database!(account_id: account_id).id
      end)

    # Get configured attrs first, then merge explicit attrs, allowing overrides
    configured_attrs = configured_postgres_replication_attrs(attrs)

    configured_attrs
    |> Map.put(:account_id, account_id)
    |> Map.put(:postgres_database_id, postgres_database_id)
    # Build the final struct with all attributes
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
      %SlotProcessor.Message.Field{
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
      %SlotProcessor.Message{
        action: :insert,
        commit_timestamp: Factory.timestamp(),
        commit_lsn: Factory.unique_integer(),
        commit_idx: Enum.random(0..100),
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
      %SlotProcessor.Message{
        action: :update,
        commit_timestamp: Factory.timestamp(),
        commit_lsn: Factory.unique_integer(),
        commit_idx: Enum.random(0..100),
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
      %SlotProcessor.Message{
        action: :delete,
        commit_timestamp: Factory.timestamp(),
        commit_lsn: Factory.unique_integer(),
        commit_idx: Enum.random(0..100),
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

  def postgres_logical_message(attrs \\ []) do
    attrs = Map.new(attrs)

    merge_attributes(
      %LogicalMessage{
        prefix: Factory.word(),
        content:
          Jason.encode!(%{
            "batch_id" => "batch_#{Factory.sequence()}",
            "table_oid" => Factory.unique_integer(),
            "backfill_id" => "backfill_#{Factory.sequence()}"
          })
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
        source_tables: [source_table()],
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
        commit_idx: Enum.random(0..100),
        record_pks: record_pks,
        record: %{"column" => Factory.word()},
        changes: if(action == :update, do: %{"column" => Factory.word()}),
        action: action,
        committed_at: Factory.timestamp(),
        replication_message_trace_id: Factory.uuid(),
        source_table_oid: Factory.unique_integer(),
        source_table_schema: Factory.postgres_object(),
        source_table_name: Factory.postgres_object()
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

  def source_table(attrs \\ []) do
    attrs = Map.new(attrs)

    merge_attributes(
      %Sequin.WalPipeline.SourceTable{
        oid: Factory.unique_integer(),
        actions: [:insert, :update, :delete],
        column_filters: [column_filter()],
        group_column_attnums: nil,
        sort_column_attnum: Factory.unique_integer()
      },
      attrs
    )
  end

  def source_table_attrs(attrs \\ []) do
    attrs
    |> source_table()
    |> Sequin.Map.from_ecto()
  end

  def column_filter(attrs \\ []) do
    attrs = Map.new(attrs)

    value_type =
      Map.get(
        attrs,
        :value_type,
        Enum.random([
          :string,
          :cistring,
          :number,
          :boolean,
          :null,
          :list
        ])
      )

    merge_attributes(
      %ColumnFilter{
        column_attnum: Factory.unique_integer(),
        operator: generate_operator(value_type),
        value: %{__type__: value_type, value: generate_value(value_type)}
      },
      Map.delete(attrs, :value_type)
    )
  end

  def column_filter_attrs(attrs \\ []) do
    attrs
    |> column_filter()
    |> Sequin.Map.from_ecto()
  end

  def message(attrs \\ []) do
    attrs = Map.new(attrs)
    action = Map.get(attrs, :action, Factory.one_of([:insert, :update, :delete]))

    slot_processor_message = %SlotProcessor.Message{
      action: action,
      columns: [],
      commit_timestamp: Factory.timestamp(),
      commit_lsn: Factory.unique_integer(),
      commit_idx: Faker.random_between(0, 100),
      transaction_annotations: Factory.word(),
      errors: nil,
      ids: [Factory.unique_integer()],
      table_schema: Factory.postgres_object(),
      table_name: Factory.postgres_object(),
      table_oid: Factory.unique_integer(),
      trace_id: Factory.uuid(),
      old_fields: [],
      fields: [],
      subscription_ids: [],
      byte_size: Faker.random_between(100, 10_000),
      batch_epoch: Faker.random_between(0, 10)
    }

    merge_attributes(
      %SlotProducer.Message{
        byte_size: slot_processor_message.byte_size,
        commit_idx: slot_processor_message.commit_idx,
        commit_lsn: slot_processor_message.commit_lsn,
        commit_ts: slot_processor_message.commit_timestamp,
        kind: action,
        payload: "",
        message: slot_processor_message,
        transaction_annotations: slot_processor_message.transaction_annotations,
        batch_epoch: slot_processor_message.batch_epoch
      },
      attrs
    )
  end

  def batch_marker(attrs \\ []) do
    attrs = Map.new(attrs)

    merge_attributes(
      %Sequin.Runtime.SlotProducer.BatchMarker{
        epoch: Faker.random_between(0, 10),
        high_watermark_wal_cursor: %{
          commit_lsn: Factory.unique_integer(),
          commit_idx: Faker.random_between(0, 100)
        }
      },
      attrs
    )
  end

  defp generate_value(:string), do: Faker.Lorem.sentence()
  defp generate_value(:cistring), do: Faker.Internet.email()
  defp generate_value(:number), do: Enum.random([Factory.integer(), Factory.float()])
  defp generate_value(:boolean), do: Factory.boolean()
  defp generate_value(:null), do: nil
  defp generate_value(:list), do: Enum.map(1..3, fn _ -> Factory.word() end)

  defp generate_operator(:null), do: Factory.one_of([:is_null, :not_null])
  defp generate_operator(:list), do: Factory.one_of([:in, :not_in])
  defp generate_operator(:boolean), do: Factory.one_of([:==, :!=])
  defp generate_operator(_), do: Factory.one_of([:==, :!=, :>, :<, :>=, :<=])
end

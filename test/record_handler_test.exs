defmodule Sequin.DatabasesRuntime.RecordHandlerTest do
  use Sequin.DataCase, async: true

  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Databases.PostgresDatabase.Table.Column
  alias Sequin.DatabasesRuntime.RecordHandler
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Repo

  describe "handle_records/2" do
    setup do
      database = DatabasesFactory.insert_postgres_database!(table_count: 1)
      table = hd(database.tables)
      pk_cols = Enum.filter(table.columns, & &1.is_pk?)

      slot =
        ReplicationFactory.insert_postgres_replication!(
          postgres_database_id: database.id,
          account_id: database.account_id
        )

      consumer =
        ConsumersFactory.insert_consumer!(
          message_kind: :record,
          account_id: database.account_id,
          replication_slot_id: slot.id,
          source_tables: [
            ConsumersFactory.source_table(
              oid: table.oid,
              column_filters: []
            )
          ]
        )

      consumer = Repo.preload(consumer, :postgres_database)
      ctx = RecordHandler.init(consumer: consumer, table: table)

      {:ok, consumer: consumer, table: table, pk_cols: pk_cols, ctx: ctx}
    end

    test "inserts new records", %{consumer: consumer, table: table, ctx: ctx, pk_cols: pk_cols} do
      records = for _ <- 1..3, do: build_record(table.columns)

      assert {:ok, 3} = RecordHandler.handle_records(ctx, records)

      inserted_records = Repo.all(ConsumerRecord)
      assert length(inserted_records) == 3

      assert_records(inserted_records, consumer, table, pk_cols)
    end
  end

  # Helper function to create a consumer record
  defp build_record(columns) do
    Enum.reduce(columns, %{}, fn %Column{name: name, type: type}, acc ->
      value =
        case type do
          "integer" -> Sequin.Factory.integer()
          "text" -> Sequin.Factory.word()
          "boolean" -> Sequin.Factory.boolean()
          "timestamp" -> Sequin.Factory.timestamp()
          "uuid" -> Sequin.Factory.uuid()
          _ -> to_string(:rand.uniform(1000))
        end

      Map.put(acc, name, value)
    end)
  end

  defp assert_records(records, consumer, table, pk_cols) do
    for record <- records do
      assert record.consumer_id == consumer.id
      assert record.table_oid == table.oid
      assert length(record.record_pks) == length(pk_cols)
    end
  end
end

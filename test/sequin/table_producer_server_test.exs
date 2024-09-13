defmodule Sequin.DatabasesRuntime.TableProducerServerTest do
  # Needs to be false until we figure out how to work with Ecto sandbox + characters
  use Sequin.DataCase, async: false
  use ExUnit.Case

  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Databases
  alias Sequin.DatabasesRuntime.TableProducer
  alias Sequin.DatabasesRuntime.TableProducerServer
  alias Sequin.Factory.CharacterFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Repo
  alias Sequin.Test.Support.Models.Character

  @moduletag :uses_characters

  setup do
    # Set up the database and consumer
    database =
      DatabasesFactory.insert_configured_postgres_database!(
        tables_sort_column_attnums: %{Character.table_oid() => Character.column_attnum("updated_at")}
      )

    replication =
      ReplicationFactory.insert_postgres_replication!(
        account_id: database.account_id,
        postgres_database_id: database.id
      )

    {:ok, database} = Databases.update_tables(database)

    table_oid = Character.table_oid()

    source_table =
      ConsumersFactory.source_table(
        oid: table_oid,
        column_filters: []
      )

    consumer =
      ConsumersFactory.insert_consumer!(
        replication_slot_id: replication.id,
        message_kind: :record,
        record_consumer_state: ConsumersFactory.record_consumer_state_attrs(),
        source_tables: [source_table],
        account_id: database.account_id
      )

    {:ok, consumer: consumer, table_oid: table_oid}
  end

  describe "TableProducerServer" do
    test "initializes, fetches, and paginates records correctly", %{
      consumer: consumer,
      table_oid: table_oid
    } do
      page_size = 3

      # Insert initial 8 records
      characters =
        1..8
        |> Enum.map(fn _ -> CharacterFactory.insert_character!() end)
        |> Enum.sort_by(& &1.updated_at, NaiveDateTime)

      pid =
        start_supervised!(
          {TableProducerServer,
           [
             consumer: consumer,
             page_size: page_size,
             table_oid: table_oid,
             test_pid: self()
           ]}
        )

      Process.monitor(pid)

      # Wait for the TableProducerServer to finish processing
      assert_receive {:DOWN, _ref, :process, ^pid, :normal}, 5000

      # Fetch ConsumerRecords from the database
      consumer_records =
        consumer.id
        |> ConsumerRecord.where_consumer_id()
        |> Repo.all()
        |> Enum.sort_by(& &1.id)

      assert length(consumer_records) == 8

      # Verify that the records match the inserted characters
      for {consumer_record, character} <- Enum.zip(consumer_records, characters) do
        assert consumer_record.table_oid == table_oid
        assert consumer_record.record_pks == [to_string(character.id)]
      end

      # Assert that the consumer's cursor has been updated
      cursor = TableProducer.fetch_cursors(consumer.id)
      # Cursor should be nil after completion
      assert cursor == :error

      # Verify that the consumer's producer state has been updated
      consumer = Repo.reload(consumer)
      assert consumer.record_consumer_state.producer == :wal
    end
  end
end

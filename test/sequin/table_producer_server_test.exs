defmodule Sequin.DatabasesRuntime.TableProducerServerTest do
  use Sequin.DataCase, async: true
  use ExUnit.Case

  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Databases
  # Needs to be false until we figure out how to work with Ecto sandbox + characters
  alias Sequin.Databases.ConnectionCache
  alias Sequin.DatabasesRuntime.TableProducer
  alias Sequin.DatabasesRuntime.TableProducerServer
  alias Sequin.Factory.CharacterFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Repo
  alias Sequin.Test.Support.Models.Character

  setup do
    # Set up the database and consumer
    database = DatabasesFactory.insert_configured_postgres_database!()

    replication =
      ReplicationFactory.insert_postgres_replication!(
        account_id: database.account_id,
        postgres_database_id: database.id
      )

    {:ok, database} = Databases.update_tables(database)

    table_oid = Character.table_oid()
    table = Sequin.Enum.find!(database.tables, &(&1.oid == table_oid))
    table = %{table | sort_column_attnum: Character.column_attnum("updated_at")}

    source_table =
      ConsumersFactory.source_table(
        oid: table_oid,
        sort_column_attnum: Character.column_attnum("updated_at"),
        column_filters: []
      )

    # Insert initial 8 records
    characters =
      1..8
      |> Enum.map(fn _ -> CharacterFactory.insert_character!() end)
      |> Enum.sort_by(& &1.updated_at, NaiveDateTime)

    ConnectionCache.cache_connection(database, Repo)

    consumer =
      ConsumersFactory.insert_consumer!(
        replication_slot_id: replication.id,
        message_kind: :record,
        record_consumer_state:
          ConsumersFactory.record_consumer_state_attrs(initial_min_cursor: nil, producer: :table_and_wal),
        source_tables: [source_table],
        account_id: database.account_id
      )

    {:ok, consumer: consumer, table: table, table_oid: table_oid, database: database, characters: characters}
  end

  describe "TableProducerServer" do
    test "processes only characters after initial_min_cursor", %{
      consumer: consumer,
      table_oid: table_oid,
      characters: characters
    } do
      page_size = 3

      # Use the 4th character as the initial_min_cursor
      initial_min_cursor = %{
        Character.column_attnum("updated_at") => Enum.at(characters, 3).updated_at,
        Character.column_attnum("id") => Enum.at(characters, 3).id
      }

      record_state = consumer.record_consumer_state
      consumer = %{consumer | record_consumer_state: %{record_state | initial_min_cursor: initial_min_cursor}}

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

      # We expect only 5 records (the last 5 characters)
      assert length(consumer_records) == 5

      # Verify that the records match the last 5 inserted characters
      for {consumer_record, character} <- Enum.zip(consumer_records, Enum.drop(characters, 3)) do
        assert consumer_record.table_oid == table_oid
        assert consumer_record.record_pks == [to_string(character.id)]
      end

      cursor = TableProducer.fetch_cursors(consumer.id)
      # Cursor should be nil after completion
      assert cursor == :error

      # Verify that the consumer's producer state has been updated
      consumer = Repo.reload(consumer)
      assert consumer.record_consumer_state.producer == :wal
    end
  end
end

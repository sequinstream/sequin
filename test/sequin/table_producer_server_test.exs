defmodule Sequin.DatabasesRuntime.TableProducerServerTest do
  use Sequin.DataCase, async: true
  use ExUnit.Case

  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.SequenceFilter
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

    sequence =
      DatabasesFactory.insert_sequence!(
        postgres_database_id: database.id,
        table_oid: table_oid,
        sort_column_attnum: Character.column_attnum("updated_at")
      )

    sequence_filter = ConsumersFactory.sequence_filter(column_filters: [])

    filtered_sequence_filter =
      ConsumersFactory.sequence_filter(
        column_filters: [
          Map.from_struct(
            ConsumersFactory.sequence_filter_column_filter(
              column_attnum: Character.column_attnum("house"),
              operator: :==,
              value: %{__type__: :string, value: "Stark"}
            )
          )
        ]
      )

    # Insert initial 8 records
    characters =
      1..8
      |> Enum.map(fn _ -> CharacterFactory.insert_character!() end)
      |> Enum.sort_by(& &1.updated_at, NaiveDateTime)

    ConnectionCache.cache_connection(database, Repo)

    initial_min_cursor = %{
      Character.column_attnum("updated_at") => ~U[1970-01-01 00:00:00Z],
      Character.column_attnum("id") => 0
    }

    consumer =
      ConsumersFactory.insert_consumer!(
        replication_slot_id: replication.id,
        message_kind: :record,
        record_consumer_state:
          ConsumersFactory.record_consumer_state_attrs(initial_min_cursor: initial_min_cursor, producer: :table_and_wal),
        account_id: database.account_id,
        sequence_id: sequence.id,
        sequence_filter: Map.from_struct(sequence_filter)
      )

    filtered_consumer =
      ConsumersFactory.insert_consumer!(
        replication_slot_id: replication.id,
        message_kind: :record,
        record_consumer_state:
          ConsumersFactory.record_consumer_state_attrs(initial_min_cursor: initial_min_cursor, producer: :table_and_wal),
        account_id: database.account_id,
        sequence_id: sequence.id,
        sequence_filter: Map.from_struct(filtered_sequence_filter)
      )

    {:ok,
     consumer: consumer,
     filtered_consumer: filtered_consumer,
     table: table,
     table_oid: table_oid,
     database: database,
     characters: characters,
     sequence_filter: sequence_filter}
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

    test "sets group_id based on PKs when group_column_attnums is nil", %{
      consumer: consumer,
      table_oid: table_oid,
      sequence_filter: sequence_filter
    } do
      page_size = 3

      sequence_filter = %SequenceFilter{sequence_filter | group_column_attnums: nil}

      consumer = %{consumer | sequence_filter: sequence_filter}

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

      assert_receive {:DOWN, _ref, :process, ^pid, :normal}, 5000

      consumer_records =
        consumer.id
        |> ConsumerRecord.where_consumer_id()
        |> Repo.all()

      assert Enum.all?(consumer_records, &(&1.group_id == Enum.join(&1.record_pks, ",")))
    end

    test "sets group_id based on group_column_attnums when it's set", %{
      consumer: consumer,
      table_oid: table_oid,
      characters: characters,
      sequence_filter: sequence_filter
    } do
      page_size = 3

      sequence_filter = %SequenceFilter{sequence_filter | group_column_attnums: [Character.column_attnum("name")]}

      consumer = %{consumer | sequence_filter: sequence_filter}

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

      assert_receive {:DOWN, _ref, :process, ^pid, :normal}, 5000

      consumer_records =
        consumer.id
        |> ConsumerRecord.where_consumer_id()
        |> Repo.all()

      assert_lists_equal(consumer_records, characters, fn record, character ->
        [to_string(character.id)] == record.record_pks and character.name == record.group_id
      end)
    end

    test "processes only characters matching the filter", %{
      filtered_consumer: filtered_consumer,
      table_oid: table_oid
    } do
      # Insert characters that match and don't match the filter
      matching_characters = [
        CharacterFactory.insert_character!(house: "Stark"),
        CharacterFactory.insert_character!(house: "Stark")
      ]

      non_matching_characters = [
        CharacterFactory.insert_character!(house: "Lannister"),
        CharacterFactory.insert_character!(house: "Targaryen")
      ]

      page_size = 10

      pid =
        start_supervised!(
          {TableProducerServer,
           [
             consumer: filtered_consumer,
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
        filtered_consumer.id
        |> ConsumerRecord.where_consumer_id()
        |> Repo.all()
        |> Enum.sort_by(& &1.id)

      # We expect only 2 records (the matching characters)
      assert length(consumer_records) == 2

      # Verify that the records match only the characters with house "Stark"
      for {consumer_record, character} <- Enum.zip(consumer_records, matching_characters) do
        assert consumer_record.table_oid == table_oid
        assert consumer_record.record_pks == [to_string(character.id)]
      end

      # Verify that non-matching characters were not processed
      non_matching_ids = Enum.map(non_matching_characters, & &1.id)
      processed_ids = Enum.flat_map(consumer_records, & &1.record_pks)
      assert Enum.all?(non_matching_ids, &(to_string(&1) not in processed_ids))

      cursor = TableProducer.fetch_cursors(filtered_consumer.id)
      # Cursor should be nil after completion
      assert cursor == :error

      # Verify that the consumer's producer state has been updated
      filtered_consumer = Repo.reload(filtered_consumer)
      assert filtered_consumer.record_consumer_state.producer == :wal
    end
  end
end

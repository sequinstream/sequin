defmodule Sequin.DatabasesRuntime.BackfillServerTest do
  use Sequin.DataCase, async: true
  use ExUnit.Case

  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.SequenceFilter
  alias Sequin.Databases
  # Needs to be false until we figure out how to work with Ecto sandbox + characters
  alias Sequin.Databases.ConnectionCache
  alias Sequin.DatabasesRuntime.BackfillProducer
  alias Sequin.DatabasesRuntime.BackfillServer
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
        account_id: database.account_id,
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
      ConsumersFactory.insert_sink_consumer!(
        replication_slot_id: replication.id,
        message_kind: :record,
        account_id: database.account_id,
        sequence_id: sequence.id,
        sequence_filter: Map.from_struct(sequence_filter)
      )

    ConsumersFactory.insert_active_backfill!(
      account_id: database.account_id,
      sink_consumer_id: consumer.id,
      initial_min_cursor: initial_min_cursor
    )

    filtered_consumer =
      ConsumersFactory.insert_sink_consumer!(
        replication_slot_id: replication.id,
        message_kind: :record,
        account_id: database.account_id,
        sequence_id: sequence.id,
        sequence_filter: Map.from_struct(filtered_sequence_filter)
      )

    ConsumersFactory.insert_active_backfill!(
      account_id: database.account_id,
      sink_consumer_id: filtered_consumer.id,
      initial_min_cursor: initial_min_cursor
    )

    event_consumer =
      ConsumersFactory.insert_sink_consumer!(
        replication_slot_id: replication.id,
        message_kind: :event,
        account_id: database.account_id,
        sequence_id: sequence.id,
        sequence_filter: Map.from_struct(sequence_filter)
      )

    ConsumersFactory.insert_active_backfill!(
      account_id: database.account_id,
      sink_consumer_id: event_consumer.id,
      initial_min_cursor: initial_min_cursor
    )

    {:ok,
     consumer: Repo.preload(consumer, :active_backfill),
     filtered_consumer: Repo.preload(filtered_consumer, :active_backfill),
     event_consumer: Repo.preload(event_consumer, :active_backfill),
     table: table,
     table_oid: table_oid,
     database: database,
     characters: characters,
     sequence_filter: sequence_filter}
  end

  describe "BackfillServer" do
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

      consumer.active_backfill
      |> Ecto.Changeset.change(%{initial_min_cursor: initial_min_cursor})
      |> Repo.update!()

      pid =
        start_supervised!(
          {BackfillServer,
           [
             consumer: Repo.reload(consumer),
             page_size: page_size,
             table_oid: table_oid,
             test_pid: self()
           ]}
        )

      Process.monitor(pid)

      # Wait for the BackfillServer to finish processing
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

      cursor = BackfillProducer.fetch_cursors(consumer.id)
      # Cursor should be nil after completion
      assert cursor == :error

      # Verify that the consumer's backfill has been updated
      consumer = Repo.preload(consumer, :active_backfill, force: true)
      refute consumer.active_backfill
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
          {BackfillServer,
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
          {BackfillServer,
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
          {BackfillServer,
           [
             consumer: filtered_consumer,
             page_size: page_size,
             table_oid: table_oid,
             test_pid: self()
           ]}
        )

      Process.monitor(pid)

      # Wait for the BackfillServer to finish processing
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

      cursor = BackfillProducer.fetch_cursors(filtered_consumer.id)
      # Cursor should be nil after completion
      assert cursor == :error

      # Verify that the consumer's backfill has been updated
      filtered_consumer = Repo.preload(filtered_consumer, :active_backfill, force: true)
      refute filtered_consumer.active_backfill
    end

    test "processes events for event consumers", %{
      event_consumer: event_consumer,
      table_oid: table_oid,
      characters: characters,
      database: database
    } do
      page_size = 3

      pid =
        start_supervised!(
          {BackfillServer,
           [
             consumer: event_consumer,
             page_size: page_size,
             table_oid: table_oid,
             test_pid: self()
           ]}
        )

      Process.monitor(pid)

      assert_receive {:DOWN, _ref, :process, ^pid, :normal}, 5000

      consumer_events =
        event_consumer.id
        |> ConsumerEvent.where_consumer_id()
        |> Repo.all()
        |> Enum.sort_by(& &1.id)

      # Verify all characters were processed
      assert length(consumer_events) == length(characters)

      # Verify each record has the correct event fields
      for consumer_event <- consumer_events do
        assert consumer_event.table_oid == table_oid
        assert consumer_event.data.action == :read
        assert consumer_event.data.metadata.commit_timestamp
        assert consumer_event.data.metadata.database_name == database.name
        assert is_map(consumer_event.data)
      end

      cursor = BackfillProducer.fetch_cursors(event_consumer.id)
      assert cursor == :error

      # Verify that the consumer's backfill has been updated
      event_consumer = Repo.preload(event_consumer, :active_backfill, force: true)
      refute event_consumer.active_backfill
    end

    test "pauses backfill when too many pending messages exist", %{
      consumer: consumer,
      table_oid: table_oid
    } do
      # Start with a lower max_pending_messages threshold
      # Set below 8 characters in table
      max_pending_messages = 6

      pid =
        start_supervised!(
          {BackfillServer,
           [
             consumer: consumer,
             page_size: 1,
             table_oid: table_oid,
             test_pid: self(),
             max_pending_messages: max_pending_messages,
             consumer_reload_timeout: 1
           ]}
        )

      Process.monitor(pid)

      assert_receive {BackfillServer, :paused}, 1000

      # Now clear the messages
      consumer.id
      |> ConsumerRecord.where_consumer_id()
      |> Repo.delete_all()

      # Wait for the BackfillServer to finish processing
      assert_receive {:DOWN, _ref, :process, ^pid, :normal}, 5000
    end
  end
end

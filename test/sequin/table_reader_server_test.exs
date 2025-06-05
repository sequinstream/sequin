defmodule Sequin.Runtime.TableReaderServerTest do
  use Sequin.DataCase, async: true
  use ExUnit.Case

  import Bitwise
  import ExUnit.CaptureLog

  alias Sequin.Consumers
  alias Sequin.Consumers.SequenceFilter
  alias Sequin.Databases
  alias Sequin.Databases.ConnectionCache
  alias Sequin.Error
  alias Sequin.Factory
  alias Sequin.Factory.CharacterFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Repo
  alias Sequin.Runtime.PageSizeOptimizer
  alias Sequin.Runtime.PageSizeOptimizerMock
  alias Sequin.Runtime.SlotMessageStore
  alias Sequin.Runtime.SlotMessageStoreSupervisor
  alias Sequin.Runtime.TableReader
  alias Sequin.Runtime.TableReaderServer
  alias Sequin.TestSupport.Models.CharacterDetailed

  @filter_name "Stilgar"
  @task_sup_name Module.concat(__MODULE__, TaskSupervisor)

  setup do
    start_supervised!({Task.Supervisor, name: @task_sup_name})

    # Set up the database and consumer
    database = DatabasesFactory.insert_configured_postgres_database!()

    ConnectionCache.cache_connection(database, Repo)

    replication =
      ReplicationFactory.insert_postgres_replication!(
        account_id: database.account_id,
        postgres_database_id: database.id
      )

    {:ok, database} = Databases.update_tables(database)

    table_oid = CharacterDetailed.table_oid()
    table = Sequin.Enum.find!(database.tables, &(&1.oid == table_oid))
    table = %{table | sort_column_attnum: CharacterDetailed.column_attnum("updated_at")}

    sequence =
      DatabasesFactory.insert_sequence!(
        account_id: database.account_id,
        postgres_database_id: database.id,
        table_oid: table_oid,
        sort_column_attnum: CharacterDetailed.column_attnum("updated_at")
      )

    sequence_filter =
      ConsumersFactory.sequence_filter(column_filters: [], group_column_attnums: CharacterDetailed.pk_attnums())

    filtered_sequence_filter =
      ConsumersFactory.sequence_filter(
        group_column_attnums: CharacterDetailed.pk_attnums(),
        column_filters: [
          Map.from_struct(
            ConsumersFactory.sequence_filter_column_filter(
              column_attnum: CharacterDetailed.column_attnum("name"),
              operator: :==,
              value: %{__type__: :string, value: @filter_name}
            )
          )
        ]
      )

    # Insert initial 8 records
    characters =
      1..8
      |> Enum.map(fn _ -> CharacterFactory.insert_character_detailed!() end)
      |> Enum.sort_by(& &1.updated_at, NaiveDateTime)

    ConnectionCache.cache_connection(database, Repo)

    initial_min_cursor = %{
      CharacterDetailed.column_attnum("updated_at") => ~U[1970-01-01 00:00:00Z],
      CharacterDetailed.column_attnum("id") => 0
    }

    consumer =
      ConsumersFactory.insert_sink_consumer!(
        replication_slot_id: replication.id,
        message_kind: :record,
        account_id: database.account_id,
        sequence_id: sequence.id,
        sequence_filter: Map.from_struct(sequence_filter)
      )

    backfill =
      ConsumersFactory.insert_active_backfill!(
        account_id: database.account_id,
        sink_consumer_id: consumer.id,
        initial_min_cursor: initial_min_cursor,
        table_oid: table_oid
      )

    filtered_consumer =
      ConsumersFactory.insert_sink_consumer!(
        replication_slot_id: replication.id,
        message_kind: :record,
        account_id: database.account_id,
        sequence_id: sequence.id,
        sequence_filter: Map.from_struct(filtered_sequence_filter)
      )

    filtered_consumer_backfill =
      ConsumersFactory.insert_active_backfill!(
        account_id: database.account_id,
        sink_consumer_id: filtered_consumer.id,
        initial_min_cursor: initial_min_cursor,
        table_oid: table_oid
      )

    event_consumer =
      ConsumersFactory.insert_sink_consumer!(
        replication_slot_id: replication.id,
        message_kind: :event,
        account_id: database.account_id,
        sequence_id: sequence.id,
        sequence_filter: Map.from_struct(sequence_filter)
      )

    event_consumer_backfill =
      ConsumersFactory.insert_active_backfill!(
        account_id: database.account_id,
        sink_consumer_id: event_consumer.id,
        initial_min_cursor: initial_min_cursor,
        table_oid: table_oid
      )

    # Default stubs for PageSizeOptimizer.
    # Allows each test to specify a static page size (initial_page_size) by default, bypassing
    # PageSizeOptimizer's dynamic sizing. This is helpful for determinism/testing pagination in tests.

    Mox.stub(PageSizeOptimizerMock, :new, fn opts ->
      PageSizeOptimizer.new(opts)
    end)

    Mox.stub(PageSizeOptimizerMock, :put_timing, fn state, _page_size, _time_ms -> state end)
    Mox.stub(PageSizeOptimizerMock, :put_timeout, fn state, _page_size -> state end)
    Mox.stub(PageSizeOptimizerMock, :size, fn state -> state.initial_page_size end)
    Mox.stub(PageSizeOptimizerMock, :history, fn state -> state.history end)

    {:ok,
     consumer: consumer,
     backfill: backfill,
     filtered_consumer: filtered_consumer,
     filtered_consumer_backfill: filtered_consumer_backfill,
     event_consumer: event_consumer,
     event_consumer_backfill: event_consumer_backfill,
     table: table,
     table_oid: table_oid,
     database: database,
     characters: characters,
     sequence_filter: sequence_filter}
  end

  describe "TableReaderServer" do
    test "processes records in batches", %{
      backfill: backfill,
      consumer: consumer,
      table_oid: table_oid,
      characters: characters
    } do
      page_size = 3

      # Use the 4th character as the initial_min_cursor
      initial_min_cursor = %{
        CharacterDetailed.column_attnum("updated_at") => Enum.at(characters, 3).updated_at,
        CharacterDetailed.column_attnum("id") => Enum.at(characters, 3).id
      }

      backfill
      |> Ecto.Changeset.change(%{initial_min_cursor: initial_min_cursor})
      |> Repo.update!()

      start_supervised({SlotMessageStoreSupervisor, consumer_id: consumer.id, test_pid: self()})
      pid = start_table_reader_server(backfill, table_oid, initial_page_size: page_size)

      Process.monitor(pid)

      messages =
        Enum.reduce(1..2, [], fn n, messages ->
          assert_receive {TableReaderServer, {:batch_fetched, batch_id}}, 1000

          assert :ok =
                   TableReaderServer.flush_batch(pid, %{batch_id: batch_id, commit_lsn: n, drop_pks: MapSet.new()})

          produce_and_ack_messages(consumer, page_size) ++ messages
        end)

      # We expect only 5 records (the last 5 characters)
      assert length(messages) == 5

      assert messages |> Enum.frequencies_by(& &1.commit_lsn) |> Map.values() == [3, 2]

      # Verify that the records match the last 5 inserted characters
      messages = Enum.sort_by(messages, & &1.record_pks)

      for {message, character} <- Enum.zip(messages, Enum.drop(characters, 3)) do
        assert message.table_oid == table_oid
        assert message.record_pks == [to_string(character.id)]
      end

      assert_receive {:DOWN, _ref, :process, ^pid, :normal}, 1000

      cursor = TableReader.cursor(backfill.id)
      # Cursor should be nil after completion
      assert cursor == nil

      # Verify that the consumer's backfill has been updated
      consumer = Repo.preload(consumer, :active_backfill, force: true)
      refute consumer.active_backfill
    end

    test "sets group_id based on PKs by default", %{
      backfill: backfill,
      consumer: consumer,
      table_oid: table_oid,
      characters: characters
    } do
      page_size = 3

      start_supervised({SlotMessageStoreSupervisor, consumer_id: consumer.id, test_pid: self()})
      pid = start_table_reader_server(backfill, table_oid, initial_page_size: page_size)

      {:ok, messages} = flush_batches(consumer, pid)

      assert length(messages) == length(characters)

      assert Enum.all?(messages, &(&1.group_id == Enum.join(&1.record_pks, ",")))
    end

    test "sets group_id based on group_column_attnums when it's set", %{
      backfill: backfill,
      consumer: consumer,
      table_oid: table_oid,
      characters: characters,
      sequence_filter: sequence_filter
    } do
      page_size = 3

      sequence_filter = %SequenceFilter{sequence_filter | group_column_attnums: [CharacterDetailed.column_attnum("name")]}
      {:ok, _} = Consumers.update_sink_consumer(consumer, %{sequence_filter: Map.from_struct(sequence_filter)})

      start_supervised({SlotMessageStoreSupervisor, consumer_id: consumer.id, test_pid: self()})
      pid = start_table_reader_server(backfill, table_oid, initial_page_size: page_size)

      {:ok, messages} = flush_batches(consumer, pid)

      assert_lists_equal(messages, characters, fn message, character ->
        [to_string(character.id)] == message.record_pks and character.name == message.group_id
      end)
    end

    @tag capture_log: true
    test "processes only characters matching the filter", %{
      filtered_consumer_backfill: filtered_consumer_backfill,
      filtered_consumer: filtered_consumer,
      table_oid: table_oid
    } do
      # Insert characters that match and don't match the filter
      matching_characters = [
        CharacterFactory.insert_character_detailed!(name: @filter_name),
        CharacterFactory.insert_character_detailed!(name: @filter_name)
      ]

      non_matching_characters = [
        CharacterFactory.insert_character_detailed!(name: "Not Stilgar"),
        CharacterFactory.insert_character_detailed!(name: "Not Stilgar")
      ]

      page_size = 10

      start_supervised({SlotMessageStoreSupervisor, consumer_id: filtered_consumer.id, test_pid: self()})
      pid = start_table_reader_server(filtered_consumer_backfill, table_oid, initial_page_size: page_size)

      {:ok, messages} = flush_batches(filtered_consumer, pid)
      record_pks = Enum.map(messages, & &1.record_pks)
      assert_lists_equal(record_pks, Enum.uniq(record_pks))

      assert length(messages) == 2

      # Verify that the records match only the characters with status "active"
      messages = Enum.sort_by(messages, & &1.record_pks)

      for {message, character} <- Enum.zip(messages, matching_characters) do
        assert message.table_oid == table_oid
        assert message.record_pks == [to_string(character.id)]
      end

      # Verify that non-matching characters were not processed
      non_matching_ids = Enum.map(non_matching_characters, & &1.id)
      processed_ids = Enum.flat_map(messages, & &1.record_pks)
      assert Enum.all?(non_matching_ids, &(to_string(&1) not in processed_ids))

      cursor = TableReader.fetch_cursors(filtered_consumer.id)
      # Cursor should be nil after completion
      assert cursor == :error

      # Verify that the consumer's backfill has been updated
      filtered_consumer = Repo.preload(filtered_consumer, :active_backfill, force: true)
      refute filtered_consumer.active_backfill
    end

    test "processes events for event consumers", %{
      event_consumer_backfill: event_consumer_backfill,
      event_consumer: event_consumer,
      table_oid: table_oid,
      characters: characters,
      database: database
    } do
      page_size = 3

      start_supervised({SlotMessageStoreSupervisor, consumer_id: event_consumer.id, test_pid: self()})
      pid = start_table_reader_server(event_consumer_backfill, table_oid, initial_page_size: page_size)

      {:ok, messages} = flush_batches(event_consumer, pid)

      # Verify all characters were processed
      assert length(messages) == length(characters)

      # Verify each record has the correct event fields
      for message <- messages do
        assert message.table_oid == table_oid
        assert message.data.action == :read
        assert message.data.metadata.commit_timestamp
        assert message.data.metadata.database_name == database.name
        assert is_map(message.data)
      end

      cursor = TableReader.fetch_cursors(event_consumer.id)
      assert cursor == :error

      # Verify that the consumer's backfill has been updated
      event_consumer = Repo.preload(event_consumer, :active_backfill, force: true)
      refute event_consumer.active_backfill
    end

    # TODO: Come back to this after we fix failed message path
    @tag skip: true
    test "pauses backfill when too many pending messages exist", %{
      backfill: backfill,
      consumer: consumer,
      table_oid: table_oid
    } do
      # Start with a lower max_pending_messages threshold
      # Set below 8 characters in table
      max_pending_messages = 1

      start_supervised(
        {SlotMessageStoreSupervisor, consumer_id: consumer.id, test_pid: self(), flush_interval: 1, flush_wait_ms: 1}
      )

      pid =
        start_table_reader_server(backfill, table_oid,
          initial_page_size: 2,
          max_pending_messages: max_pending_messages,
          check_state_timeout: 1
        )

      Process.monitor(pid)
      assert_receive {TableReaderServer, {:batch_fetched, batch_id}}, 1000
      assert :ok = TableReaderServer.flush_batch(pid, %{batch_id: batch_id, commit_lsn: 0, drop_pks: MapSet.new()})
      assert_receive {TableReaderServer, :paused}, 1000

      # Now clear the messages
      {:ok, messages} = SlotMessageStore.produce(consumer.id, 100, self())
      SlotMessageStore.messages_succeeded(consumer.id, Enum.map(messages, & &1.ack_id))

      # We can continue more, and then may get paused again
      flush_batches(consumer, pid)
    end

    test "stops when LSN indicates a batch is stale", %{
      backfill: backfill,
      table_oid: table_oid,
      consumer: consumer
    } do
      # Return a very high LSN to force retry
      max_lsn = (1 <<< 64) - 1
      fetch_slot_lsn = fn _db, _slot_name -> {:ok, max_lsn} end

      start_supervised({SlotMessageStoreSupervisor, consumer_id: consumer.id, test_pid: self()})

      pid =
        start_table_reader_server(backfill, table_oid,
          initial_page_size: 1000,
          check_state_timeout: 1,
          fetch_slot_lsn: fetch_slot_lsn
        )

      Process.monitor(pid)

      # We should see multiple fetches of the same batch as it keeps getting marked stale
      assert capture_log(fn ->
               assert_receive {TableReaderServer, {:batch_fetched, _batch_id}}, 1000
               assert_receive {:DOWN, _ref, :process, ^pid, :stale_batch}, 1000
             end) =~ "Detected stale batch"
    end

    @tag :capture_log
    test "reduces page size and retries on fetch_batch_pks query timeout when page size > 1000", %{
      backfill: backfill,
      table_oid: table_oid,
      consumer: consumer
    } do
      initial_page_size = 2000
      test_pid = self()
      call_count = :atomics.new(1, [])

      fetch_batch_pks = fn _conn, _table, _cursor, opts ->
        count = :atomics.add_get(call_count, 1, 1)

        case count do
          1 ->
            send(test_pid, {:fetch_batch, 1})
            # First call - return timeout error
            {:error,
             Error.service(
               message: "Query timed out",
               service: :postgres,
               details: %{timeout: 5000},
               code: :query_timeout
             )}

          2 ->
            # Second call - verify reduced page size and return success
            assert opts[:limit] < initial_page_size
            send(test_pid, {:fetch_batch, 2})
            {:ok, %{pks: [], next_cursor: nil}}

          _ ->
            raise "Unexpected call count #{count}"
        end
      end

      start_supervised({SlotMessageStoreSupervisor, consumer_id: consumer.id, test_pid: self()})

      # Use PageSizeOptimizer in this test
      pid =
        start_table_reader_server(backfill, table_oid,
          initial_page_size: initial_page_size,
          fetch_batch_pks: fetch_batch_pks,
          page_size_optimizer_mod: nil
        )

      Process.monitor(pid)

      assert_receive {:fetch_batch, 1}, 1000
      assert_receive {:fetch_batch, 2}, 1000

      assert_receive {:DOWN, _ref, :process, ^pid, :normal}, 1000
    end

    @tag :capture_log
    test "reduces page size and retries on fetch_batch query timeout when page size > 1000", %{
      backfill: backfill,
      table_oid: table_oid,
      consumer: consumer
    } do
      initial_page_size = 2000
      test_pid = self()
      call_count1 = :atomics.new(1, [])
      call_count2 = :atomics.new(1, [])

      # First, mock the primary keys fetch to return some keys
      fetch_batch_pks = fn _conn, _table, _cursor, _opts ->
        count = :atomics.add_get(call_count1, 1, 1)

        case count do
          1 ->
            # Return some dummy primary keys
            {:ok, %{pks: [["1"], ["2"], ["3"]], next_cursor: ["4"]}}

          2 ->
            # System isn't very smart - it will re-run the ID fetch!
            {:ok, %{pks: [["1"], ["2"], ["3"]], next_cursor: ["4"]}}

          3 ->
            {:ok, %{pks: [], next_cursor: nil}}
        end
      end

      # Then, mock the batch fetch to timeout on first call
      fetch_batch = fn _conn, _consumer, _table, _cursor, _opts ->
        count = :atomics.add_get(call_count2, 1, 1)

        case count do
          1 ->
            send(test_pid, {:fetch_batch, 1})
            # First call - return timeout error
            {:error,
             Error.service(
               message: "Query timed out",
               service: :postgres,
               details: %{timeout: 5000},
               code: :query_timeout
             )}

          2 ->
            # Second call - verify we retry and return success
            send(test_pid, {:fetch_batch, 2})
            {:ok, %{messages: [], next_cursor: nil}}

          _ ->
            raise "Unexpected call count #{count}"
        end
      end

      start_supervised({SlotMessageStoreSupervisor, consumer_id: consumer.id, test_pid: self()})

      # Use PageSizeOptimizer in this test
      pid =
        start_table_reader_server(backfill, table_oid,
          initial_page_size: initial_page_size,
          fetch_batch_pks: fetch_batch_pks,
          fetch_batch: fetch_batch,
          page_size_optimizer_mod: nil
        )

      Process.monitor(pid)

      assert_receive {:fetch_batch, 1}, 1000
      assert_receive {:fetch_batch, 2}, 1000

      assert_receive {:DOWN, _ref, :process, ^pid, :normal}, 1000
    end

    test "continues to next page when fetch_batch returns no results after filtering", %{
      filtered_consumer_backfill: backfill,
      filtered_consumer: consumer,
      table_oid: table_oid
    } do
      # Set a small page size to ensure we need multiple pages
      page_size = 2

      # Insert characters with older timestamps that don't match the filter
      non_matching_characters =
        Enum.map(1..3, fn _i ->
          updated_at = Factory.timestamp_past()
          CharacterFactory.insert_character_detailed!(name: "Not Stilgar", updated_at: updated_at)
        end)

      # Create with older timestamps to ensure they come first in pagination

      # Insert a character that matches the filter with a newer timestamp
      matching_character =
        CharacterFactory.insert_character_detailed!(
          name: @filter_name,
          updated_at: DateTime.utc_now()
        )

      start_supervised({SlotMessageStoreSupervisor, consumer_id: consumer.id, test_pid: self()})
      pid = start_table_reader_server(backfill, table_oid, initial_page_size: page_size)

      # Monitor the process to track when it completes
      Process.monitor(pid)

      # We should get exactly one batch with our matching character
      {:ok, messages} = flush_batches(consumer, pid)

      # We should only get the one matching character
      assert length(messages) == 1
      message = List.first(messages)
      assert message.record_pks == [to_string(matching_character.id)]

      # Verify that non-matching characters were not processed
      non_matching_ids = Enum.map(non_matching_characters, & &1.id)
      processed_ids = Enum.flat_map(messages, & &1.record_pks)
      assert Enum.all?(non_matching_ids, &(to_string(&1) not in processed_ids))

      # Verify the process completed successfully
      assert_receive {:DOWN, _ref, :process, ^pid, :normal}, 1000
    end

    test "correctly handles all column types in backfill", %{
      backfill: backfill,
      consumer: consumer,
      table_oid: table_oid
    } do
      # Insert a character with specific values for fringe column types
      character =
        CharacterFactory.insert_character_detailed!(
          status: :retired,
          embedding: [1.1, 2.2, 3.3],
          house_id: "550e8400-e29b-41d4-a716-446655440000",
          related_houses: [
            "550e8400-e29b-41d4-a716-446655440001",
            "550e8400-e29b-41d4-a716-446655440002"
          ],
          active_period: [~D[2020-01-01], ~D[2025-12-31]],
          power_level: 9001
        )

      start_supervised({SlotMessageStoreSupervisor, consumer_id: consumer.id, test_pid: self()})
      pid = start_table_reader_server(backfill, table_oid, initial_page_size: 10)

      {:ok, messages} = flush_batches(consumer, pid)

      # Find the message corresponding to our test character
      message = Enum.find(messages, &(List.first(&1.record_pks) == to_string(character.id)))
      assert message, "Character message not found in backfill"

      # Verify fringe column types are correctly preserved
      record = message.data.record
      assert record["status"] == "retired"

      assert Enum.zip_with(record["embedding"], [1.1, 2.2, 3.3], fn actual, expected ->
               assert_in_delta actual, expected, 0.0001
             end)

      assert record["house_id"] == "550e8400-e29b-41d4-a716-446655440000"

      assert record["related_houses"] == [
               "550e8400-e29b-41d4-a716-446655440001",
               "550e8400-e29b-41d4-a716-446655440002"
             ]

      assert record["active_period"] == "[2020-01-01,2025-12-31)"
      assert record["power_level"] == 9001
    end

    test "pks_seen removes primary keys from batches before flushing", %{
      backfill: backfill,
      consumer: consumer,
      table_oid: table_oid,
      characters: characters
    } do
      page_size = 100

      start_supervised({SlotMessageStoreSupervisor, consumer_id: consumer.id, test_pid: self()})
      pid = start_table_reader_server(backfill, table_oid, initial_page_size: page_size)

      # Wait for the first batch to be fetched
      assert_receive {TableReaderServer, {:batch_fetched, batch_id}}, 5000

      # Select a couple of characters to mark as seen
      [char1, char2 | _] = characters
      pks_to_mark_as_seen = [[to_string(char1.id)], [to_string(char2.id)]]

      # Call pks_seen to remove these PKs from all batches
      assert :ok = TableReaderServer.pks_seen(consumer.id, pks_to_mark_as_seen)

      # Flush the batch
      assert :ok =
               TableReaderServer.flush_batch(pid, %{
                 batch_id: batch_id,
                 commit_lsn: 1
               })

      # Get the messages that were produced
      messages = produce_and_ack_messages(consumer, 100)

      # Verify that the marked PKs are not in the output
      processed_pks = Enum.map(messages, & &1.record_pks)

      # The PKs we marked as seen should not be in the processed PKs
      refute [to_string(char1.id)] in processed_pks
      refute [to_string(char2.id)] in processed_pks

      # But other PKs should be processed
      other_characters = Enum.drop(characters, 2)
      assert length(messages) == length(other_characters)

      # Verify that the remaining characters were processed
      other_character_pks = Enum.map(other_characters, fn char -> [to_string(char.id)] end)
      assert Enum.all?(other_character_pks, fn pk -> pk in processed_pks end)
    end
  end

  defp flush_batches(consumer, pid, commit_lsn \\ 0, message_history \\ [], messages \\ []) do
    Process.monitor(pid)

    receive do
      {TableReaderServer, {:batch_fetched, batch_id}} = msg ->
        assert :ok =
                 TableReaderServer.flush_batch(pid, %{
                   batch_id: batch_id,
                   commit_lsn: commit_lsn,
                   drop_pks: MapSet.new()
                 })

        new_messages = produce_and_ack_messages(consumer, 100)

        flush_batches(consumer, pid, commit_lsn, [msg | message_history], messages ++ new_messages)

      {TableReaderServer, :paused} ->
        :paused

      {:DOWN, _ref, :process, ^pid, :normal} ->
        {:ok, messages}
    after
      1000 ->
        raise "Timeout waiting for batch_fetched. Message history: #{inspect(Enum.reverse(message_history))}"
    end
  end

  defp start_table_reader_server(backfill, table_oid, opts) do
    defaults = [
      backfill_id: backfill.id,
      table_oid: table_oid,
      initial_page_size: 3,
      test_pid: self(),
      max_pending_messages: 100,
      check_state_timeout: :timer.seconds(5),
      fetch_slot_lsn: fn _db, _slot_name -> {:ok, 0} end,
      page_size_optimizer_mod: PageSizeOptimizerMock,
      task_supervisor: GenServer.whereis(@task_sup_name)
    ]

    config =
      defaults
      |> Keyword.merge(opts)
      |> Sequin.Keyword.reject_nils()

    start_supervised!({TableReaderServer, config})
  end

  defp produce_and_ack_messages(consumer, page_size) do
    {:ok, messages} = SlotMessageStore.produce(consumer, page_size, self())
    SlotMessageStore.messages_succeeded(consumer, messages)
    messages
  end
end

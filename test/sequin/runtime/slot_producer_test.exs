defmodule Sequin.Runtime.SlotProducerTest do
  @moduledoc """
  Tests for SlotProducer GenStage producer that streams PostgreSQL replication messages.

  This test uses real end-to-end replication without mocks, establishing a dedicated
  replication slot and testing the GenStage producer/consumer pipeline.
  """
  use Sequin.DataCase, async: false

  alias Sequin.Databases.ConnectionCache
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.CharacterFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Runtime.SlotProducer
  alias Sequin.Runtime.SlotProducer.Message
  alias Sequin.Test.UnboxedRepo
  alias Sequin.TestSupport.Models.Character
  alias Sequin.TestSupport.ReplicationSlots

  @moduletag :unboxed
  @publication "characters_publication"

  def replication_slot, do: ReplicationSlots.slot_name(__MODULE__)

  setup do
    # Fast-forward the replication slot to the current WAL position
    :ok = ReplicationSlots.reset_slot(UnboxedRepo, replication_slot())
  end

  defmodule TestConsumer do
    @moduledoc """
    A simple GenStage consumer for testing SlotProducer.

    Collects messages and sends them to the test process.
    """
    use GenStage

    def start_link(opts) do
      GenStage.start_link(__MODULE__, opts)
    end

    def init(%{producer: producer, test_pid: test_pid, max_demand: max_demand, min_demand: min_demand}) do
      state = %{
        test_pid: test_pid,
        messages: []
      }

      {:consumer, state, subscribe_to: [{producer, max_demand: max_demand, min_demand: min_demand}]}
    end

    def handle_events(events, _from, state) do
      # Send received messages to test process
      send(state.test_pid, {:messages_received, events})

      new_messages = state.messages ++ events
      {:noreply, [], %{state | messages: new_messages}}
    end
  end

  describe "SlotProducer GenStage pipeline" do
    setup do
      # Create test database configuration
      account = AccountsFactory.insert_account!()

      postgres_database =
        DatabasesFactory.insert_configured_postgres_database!(
          account_id: account.id,
          tables: :character_tables,
          pg_major_version: 17
        )

      ConnectionCache.cache_connection(postgres_database, UnboxedRepo)

      # Create replication slot entity
      pg_replication =
        ReplicationFactory.insert_postgres_replication!(
          account_id: account.id,
          postgres_database_id: postgres_database.id,
          slot_name: replication_slot(),
          publication_name: @publication,
          status: :active
        )

      test_pid = self()

      # Start the SlotProducer
      producer_pid = start_slot_producer(postgres_database)

      # Start a test consumer that will collect messages
      start_test_consumer(producer_pid, test_pid)

      {:ok, %{postgres_database: postgres_database, pg_replication: pg_replication}}
    end

    test "produces messages when data is inserted" do
      # Insert a character record to generate WAL messages
      character_attrs = CharacterFactory.character_attrs()
      CharacterFactory.insert_character!(character_attrs, repo: UnboxedRepo)

      # Wait for and assert we receive messages
      assert_receive_message_kinds([:insert])
    end

    test "produces messages in correct order" do
      # Insert a character record to generate WAL messages
      CharacterFactory.insert_character!(%{}, repo: UnboxedRepo)
      UnboxedRepo.update_all(Character, set: [name: "Updated Name"])
      CharacterFactory.insert_character!(%{}, repo: UnboxedRepo)
      UnboxedRepo.update_all(Character, set: [name: "Updated Namez"])
      UnboxedRepo.delete_all(Character)

      # Wait for and assert we receive messages
      assert_receive_message_kinds([:insert, :update, :insert, :update, :update, :delete, :delete])
    end

    test "respects transaction boundaries" do
      char1 = CharacterFactory.insert_character!(%{}, repo: UnboxedRepo)

      UnboxedRepo.transaction(fn ->
        CharacterFactory.insert_character!(%{}, repo: UnboxedRepo)
        CharacterFactory.insert_character!(%{}, repo: UnboxedRepo)
      end)

      char1.id
      |> Character.where_id()
      |> UnboxedRepo.update_all(set: [name: "Updated Name"])

      messages = receive_messages(4)

      assert [
               %Message{commit_lsn: lsn1, commit_idx: 0, commit_ts: ts1, kind: :insert},
               %Message{commit_lsn: lsn2, commit_idx: 0, commit_ts: ts2, kind: :insert},
               %Message{commit_lsn: lsn2, commit_idx: 1, commit_ts: ts2, kind: :insert},
               %Message{commit_lsn: lsn3, commit_idx: 0, commit_ts: ts3, kind: :update}
             ] = messages

      assert Enum.all?([lsn1, lsn2, lsn3], &is_integer/1)
      assert Enum.all?([ts1, ts2, ts3], &is_struct(&1, DateTime))
    end

    # test "handles multiple inserts and produces correct number of messages", %{
    #   postgres_database: postgres_database
    # } do
    #   test_pid = self()

    #   connect_opts = db_connect_opts(postgres_database)
    #   start_query = replication_start_query()

    #   opts = [
    #     connect_opts: connect_opts,
    #     start_replication_query: start_query,
    #     safe_wal_cursor_fn: fn _state -> %{commit_lsn: 0} end
    #   ]

    #   # Start the SlotProducer
    #   {:ok, producer_pid} = GenStage.start_link(SlotProducer, opts)

    #   # Start a test consumer
    #   {:ok, consumer_pid} = start_test_consumer(producer_pid, test_pid)

    #   # Wait for connection
    #   Process.sleep(200)

    #   # Insert multiple character records
    #   characters =
    #     Enum.map(1..3, fn _i ->
    #       character_attrs = CharacterFactory.character_attrs()
    #       CharacterFactory.insert_character!(character_attrs, repo: UnboxedRepo)
    #     end)

    #   # Wait for messages
    #   assert_receive {:messages_received, messages}, 5_000

    #   # We should receive at least some messages for our inserts
    #   assert length(messages) > 0

    #   # Log the messages for debugging
    #   IO.inspect(length(messages), label: "Total messages received")

    #   # Clean up
    #   GenStage.stop(consumer_pid)
    #   GenStage.stop(producer_pid)
    # end

    # test "handles backpressure correctly with limited demand", %{postgres_database: postgres_database} do
    #   test_pid = self()

    #   connect_opts = db_connect_opts(postgres_database)
    #   start_query = replication_start_query()

    #   opts = [
    #     connect_opts: connect_opts,
    #     start_replication_query: start_query,
    #     safe_wal_cursor_fn: fn _state -> %{commit_lsn: 0} end
    #   ]

    #   # Start the SlotProducer
    #   {:ok, producer_pid} = GenStage.start_link(SlotProducer, opts)

    #   # Start a test consumer with limited demand (max_demand: 2)
    #   {:ok, consumer_pid} = start_test_consumer(producer_pid, test_pid, max_demand: 2)

    #   # Wait for connection
    #   Process.sleep(200)

    #   # Insert several records
    #   Enum.each(1..5, fn _i ->
    #     character_attrs = CharacterFactory.character_attrs()
    #     CharacterFactory.insert_character!(character_attrs, repo: UnboxedRepo)
    #   end)

    #   # We should receive messages in batches due to backpressure
    #   assert_receive {:messages_received, first_batch}, 5_000
    #   assert length(first_batch) <= 2

    #   # Consumer should request more and receive more messages
    #   assert_receive {:messages_received, second_batch}, 5_000
    #   assert length(second_batch) <= 2

    #   # Clean up
    #   GenStage.stop(consumer_pid)
    #   GenStage.stop(producer_pid)
    # end

    # @tag capture_log: true
    # test "handles connection failures gracefully", %{postgres_database: postgres_database} do
    #   # Use invalid connection options to trigger failure
    #   invalid_connect_opts =
    #     postgres_database
    #     |> db_connect_opts()
    #     # Invalid port
    #     |> Keyword.put(:port, 9999)

    #   start_query = replication_start_query()

    #   connection_failures = :counters.new(1, [])

    #   opts = [
    #     connect_opts: invalid_connect_opts,
    #     start_replication_query: start_query,
    #     safe_wal_cursor_fn: fn _state -> %{commit_lsn: 0} end,
    #     handle_connect_fail: fn _reason ->
    #       :counters.add(connection_failures, 1, 1)
    #     end,
    #     # Fast reconnection for testing
    #     reconnect_interval: 100
    #   ]

    #   # Start the SlotProducer
    #   {:ok, producer_pid} = GenStage.start_link(SlotProducer, opts)

    #   # Wait for connection attempts
    #   Process.sleep(500)

    #   # Check that connection failures were handled
    #   failures = :counters.get(connection_failures, 1)
    #   assert failures > 0

    #   # Producer should still be alive
    #   assert Process.alive?(producer_pid)

    #   # Clean up
    #   GenStage.stop(producer_pid)
    # end

    # test "accumulates messages correctly in internal buffer", %{postgres_database: postgres_database} do
    #   test_pid = self()

    #   connect_opts = db_connect_opts(postgres_database)
    #   start_query = replication_start_query()

    #   opts = [
    #     connect_opts: connect_opts,
    #     start_replication_query: start_query,
    #     safe_wal_cursor_fn: fn _state -> %{commit_lsn: 0} end
    #   ]

    #   # Start the SlotProducer
    #   {:ok, producer_pid} = GenStage.start_link(SlotProducer, opts)

    #   # Don't start consumer immediately - let messages accumulate
    #   Process.sleep(200)

    #   # Insert records while no consumer is demanding
    #   Enum.each(1..2, fn _i ->
    #     character_attrs = CharacterFactory.character_attrs()
    #     CharacterFactory.insert_character!(character_attrs, repo: UnboxedRepo)
    #   end)

    #   # Wait for messages to be processed and buffered
    #   Process.sleep(500)

    #   # Now start consumer and it should receive the accumulated messages
    #   {:ok, consumer_pid} = start_test_consumer(producer_pid, test_pid)

    #   assert_receive {:messages_received, messages}, 5_000
    #   assert length(messages) > 0

    #   # Clean up
    #   GenStage.stop(consumer_pid)
    #   GenStage.stop(producer_pid)
    # end
  end

  defp receive_messages(count, acc \\ []) do
    assert_receive {:messages_received, messages}, 1_000

    case count - length(messages) do
      0 ->
        acc ++ messages

      next_count when next_count < 0 ->
        flunk("Received more messages than expected #{inspect(message_kinds(acc))})")

      next_count ->
        receive_messages(next_count, acc ++ messages)
    end
  rescue
    err ->
      case err do
        %ExUnit.AssertionError{message: "Assertion failed, no matching message after" <> _rest} ->
          flunk("Did not receive remaining #{count} messages (got: #{inspect(message_kinds(acc))})")

        err ->
          reraise err, __STACKTRACE__
      end
  end

  defp assert_receive_message_kinds(expected_kinds) do
    messages = receive_messages(length(expected_kinds))

    assert_lists_equal(expected_kinds, message_kinds(messages))
  end

  defp message_kinds(msgs) do
    Enum.map(msgs, & &1.kind)
  end

  # Helper functions
  defp start_slot_producer(db, opts \\ []) do
    start_query =
      "START_REPLICATION SLOT #{replication_slot()} LOGICAL 0/0 (proto_version '1', publication_names '#{@publication}', messages 'true')"

    opts =
      Keyword.merge(
        [
          id: UUID.uuid4(),
          connect_opts: db_connect_opts(db),
          start_replication_query: start_query,
          safe_wal_cursor_fn: fn _state -> %{commit_lsn: 0, commit_idx: 0} end
        ],
        opts
      )

    start_supervised!({SlotProducer, opts})
  end

  defp start_test_consumer(producer_pid, test_pid, opts \\ []) do
    max_demand = Keyword.get(opts, :max_demand, 10)
    min_demand = Keyword.get(opts, :min_demand, 5)

    start_supervised!(
      {TestConsumer, %{producer: producer_pid, test_pid: test_pid, max_demand: max_demand, min_demand: min_demand}}
    )
  end

  defp db_connect_opts(postgres_database) do
    postgres_database
    |> PostgresDatabase.to_postgrex_opts()
    |> Keyword.drop([:socket, :socket_dir, :endpoints])
  end
end

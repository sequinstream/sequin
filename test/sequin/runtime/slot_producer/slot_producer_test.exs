defmodule Sequin.Runtime.SlotProducerTest do
  @moduledoc """
  Tests for SlotProducer GenStage producer that streams PostgreSQL replication messages.

  This test uses real end-to-end replication without mocks, establishing a dedicated
  replication slot and testing the GenStage producer/consumer pipeline.
  """
  use Sequin.DataCase, async: false
  use AssertEventually, interval: 1

  alias Sequin.Databases.ConnectionCache
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.CharacterFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Factory.TestEventLogFactory
  alias Sequin.Postgres
  alias Sequin.Replication
  alias Sequin.Runtime.SlotProducer
  alias Sequin.Runtime.SlotProducer.BatchMarker
  alias Sequin.Runtime.SlotProducer.Message
  alias Sequin.Runtime.SlotProducer.Relation
  alias Sequin.Test.UnboxedRepo
  alias Sequin.TestSupport.Models.Character
  alias Sequin.TestSupport.Models.TestEventLogPartitioned
  alias Sequin.TestSupport.ReplicationSlots

  @moduletag :unboxed
  @publication "characters_publication"

  def replication_slot, do: ReplicationSlots.slot_name(__MODULE__)

  setup do
    # Fast-forward the replication slot to the current WAL position
    :ok = ReplicationSlots.reset_slot(UnboxedRepo, replication_slot())
  end

  defmodule TestProcessor do
    @moduledoc """
    A simple GenStage consumer for testing SlotProducer.

    Collects messages and sends them to the test process.
    """
    @behaviour Sequin.Runtime.SlotProducer.ProcessorBehaviour

    use GenStage

    def start_link(opts) do
      GenStage.start_link(__MODULE__, opts)
    end

    def ask(server, n) do
      GenStage.call(server, {:ask, n})
    end

    def handle_relation(server, relation) do
      GenStage.call(server, {:handle_relation, relation})
    end

    def handle_batch_marker(server, batch_marker) do
      GenStage.sync_info(server, {:handle_batch_marker, batch_marker})
    end

    def init(opts) do
      %{producer: producer, test_pid: test_pid, max_demand: max_demand, min_demand: min_demand} = opts

      state = %{
        test_pid: test_pid,
        messages: [],
        consumer_demand: Map.get(opts, :consumer_demand, :automatic),
        producer: nil
      }

      {:consumer, state, subscribe_to: [{producer, max_demand: max_demand, min_demand: min_demand}]}
    end

    def handle_events(events, _from, state) do
      # Send received messages to test process
      send(state.test_pid, {:messages_received, events})

      new_messages = state.messages ++ events
      {:noreply, [], %{state | messages: new_messages}}
    end

    def handle_call({:ask, n}, _from, state) do
      GenStage.ask(state.producer, n)
      {:reply, :ok, [], state}
    end

    def handle_call({:handle_relation, relation}, _from, state) do
      send(state.test_pid, {:relation_received, relation})

      {:reply, :ok, [], state}
    end

    def handle_subscribe(:producer, _opts, producer, state) do
      {state.consumer_demand, %{state | producer: producer}}
    end

    def handle_info({:handle_batch_marker, batch_marker}, state) do
      send(state.test_pid, {:batch_marker_received, batch_marker})

      {:noreply, [], state}
    end
  end

  describe "SlotProducer GenStage pipeline" do
    setup ctx do
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

      pg_replication = %{pg_replication | postgres_database: postgres_database}

      {producer_pid, consumer_pid} =
        if Map.get(ctx, :skip_start) do
          {nil, nil}
        else
          start_opts = Map.get(ctx, :start_opts, [])
          start_slot_producer(pg_replication, start_opts)
        end

      {:ok, %{slot: pg_replication, db: postgres_database, consumer_pid: consumer_pid, producer_pid: producer_pid}}
    end

    test "produces messages when data is inserted" do
      # Insert a character record to generate WAL messages
      CharacterFactory.insert_character!([], repo: UnboxedRepo)

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
      assert lsn3 > lsn2
      assert lsn2 > lsn1
    end

    test "add and clears transaction annotations", %{db: db} do
      annotation = ~s|{"my": "annotations"}|

      UnboxedRepo.transaction(fn ->
        CharacterFactory.insert_character!(%{}, repo: UnboxedRepo)
        write_transaction_annotation(db, annotation)
        CharacterFactory.insert_character!(%{}, repo: UnboxedRepo)
        CharacterFactory.insert_character!(%{}, repo: UnboxedRepo)
      end)

      messages = receive_messages(3)

      assert [
               %Message{transaction_annotations: nil, commit_idx: 0},
               %Message{transaction_annotations: ^annotation, commit_idx: 2},
               %Message{transaction_annotations: ^annotation, commit_idx: 3}
             ] = messages
    end

    test "clears transaction annotation when directed", %{db: db} do
      annotation = ~s|{"my": "annotations"}|

      UnboxedRepo.transaction(fn ->
        write_transaction_annotation(db, annotation)
        CharacterFactory.insert_character!(%{}, repo: UnboxedRepo)
        clear_transaction_annotation(db)
        CharacterFactory.insert_character!(%{}, repo: UnboxedRepo)
      end)

      messages = receive_messages(2)

      assert [
               %Message{transaction_annotations: ^annotation, commit_idx: 1},
               %Message{transaction_annotations: nil, commit_idx: 3}
             ] = messages
    end

    @tag skip_start: true
    test "sends acks to the replication slot on an interval", %{db: db, slot: slot} do
      {:ok, init_lsn} = Postgres.confirmed_flush_lsn(db, replication_slot())
      {:ok, agent} = Agent.start_link(fn -> %{commit_lsn: init_lsn, commit_idx: 0} end)

      start_slot_producer(slot,
        ack_interval: 1,
        restart_wal_cursor_update_interval: 1,
        restart_wal_cursor_fn: fn _, _ -> Agent.get(agent, & &1) end
      )

      CharacterFactory.insert_character!(%{}, repo: UnboxedRepo)
      [msg] = receive_messages(1)
      next_commit_lsn = msg.commit_lsn
      Agent.update(agent, fn _ -> %{commit_lsn: next_commit_lsn, commit_idx: 1} end)

      assert next_commit_lsn > init_lsn
      assert_eventually {:ok, ^next_commit_lsn} = Postgres.confirmed_flush_lsn(db, replication_slot()), 1000
    end

    test "logical messages flow through", %{db: db} do
      Postgres.query!(db, "select pg_logical_emit_message(true, 'my-msg', 'my-data')")

      assert_receive_message_kinds([:logical])
    end

    @tag start_opts: [batch_flush_interval: 1]
    test "non-transactional logical message flush is skipped", %{db: db} do
      Postgres.query!(db, "select pg_logical_emit_message(false, 'skip-me', 'body')")
      Postgres.query!(db, "select pg_logical_emit_message(true, 'see-me', 'body')")

      [msg] = receive_messages(1)
      assert msg.kind == :logical
      assert msg.payload =~ "see-me"

      # The batch marker should have a valid LSN from the logical message
      commit_lsn = msg.commit_lsn
      assert commit_lsn
      assert_receive {:batch_marker_received, %BatchMarker{high_watermark_wal_cursor: %{commit_lsn: ^commit_lsn}}}
    end

    test "receives relation messages" do
      CharacterFactory.insert_character!(%{}, repo: UnboxedRepo)

      assert_receive {:relation_received, %Relation{} = relation}, 1000

      assert relation.schema == "public"
      assert relation.table == Character.table_name()
      assert relation.parent_table_id == Character.table_oid()

      # Assert pk column is valid
      assert %Relation.Column{} = id_col = Enum.find(relation.columns, &(&1.name == "id"))
      assert id_col.type == "int8"
      assert id_col.pk?
      assert [id_col.attnum] == Character.pk_attnums()

      # Assert another column
      assert %Relation.Column{} = name_col = Enum.find(relation.columns, &(&1.name == "name"))
      refute name_col.pk?
    end

    test "receives relation messages for partitioned table" do
      TestEventLogFactory.insert_test_event_log_partitioned!(%{}, repo: UnboxedRepo)

      assert_receive {:relation_received, %Relation{} = relation}, 1000

      assert relation.schema == "public"
      assert relation.table == TestEventLogPartitioned.table_name()
      # For partitioned tables, parent_table_id should be the parent table's OID
      assert relation.parent_table_id == TestEventLogPartitioned.table_oid()

      # Assert pk columns are valid - partitioned table has composite PK (id, committed_at)
      assert %Relation.Column{} = id_col = Enum.find(relation.columns, &(&1.name == "id"))
      assert id_col.type == "int8"
      assert id_col.pk?

      assert %Relation.Column{} = committed_at_col = Enum.find(relation.columns, &(&1.name == "committed_at"))
      assert committed_at_col.type == "timestamp"
      assert committed_at_col.pk?

      # Verify the pk_attnums match
      pk_columns = Enum.filter(relation.columns, & &1.pk?)
      pk_attnums = pk_columns |> Enum.map(& &1.attnum) |> Enum.sort()
      expected_pk_attnums = Enum.sort(TestEventLogPartitioned.pk_attnums())
      assert pk_attnums == expected_pk_attnums

      # Assert a non-pk column
      assert %Relation.Column{} = seq_col = Enum.find(relation.columns, &(&1.name == "seq"))
      refute seq_col.pk?
    end

    @tag start_opts: [batch_flush_interval: 1]
    test "receives a batch flush marker after batch timer expires" do
      CharacterFactory.insert_character!(%{}, repo: UnboxedRepo)

      [msg] = receive_messages(1)
      commit_lsn = msg.commit_lsn
      assert_receive {:batch_marker_received, %BatchMarker{high_watermark_wal_cursor: %{commit_lsn: ^commit_lsn}}}

      CharacterFactory.insert_character!(%{}, repo: UnboxedRepo)

      [msg] = receive_messages(1)
      commit_lsn = msg.commit_lsn
      assert_receive {:batch_marker_received, %BatchMarker{high_watermark_wal_cursor: %{commit_lsn: ^commit_lsn}}}
    end

    @tag skip_start: true
    test "skips messages below restart WAL cursor", %{slot: slot, db: db} do
      # Insert first character
      CharacterFactory.insert_character!(%{}, repo: UnboxedRepo)

      # Get current WAL position after first insert
      {:ok, current_lsn} = Postgres.current_wal_lsn(db)

      # Set restart cursor to current position (this will skip the first message)
      Replication.put_restart_wal_cursor!(slot.id, %{commit_lsn: current_lsn, commit_idx: 0})

      # Insert second character (this will be after the restart cursor)
      CharacterFactory.insert_character!(%{name: "Character 2"}, repo: UnboxedRepo)

      start_slot_producer(slot)

      [msg] = receive_messages(1)

      assert msg.kind == :insert
      assert msg.payload =~ "Character 2"
    end

    @tag start_opts: [processor_opts: [consumer_demand: :manual]]
    test "buffers messages when no demand, then delivers all when demand is restored", %{
      slot: slot,
      consumer_pid: consumer_pid
    } do
      # Insert a lot of data to generate many WAL messages
      for i <- 1..20 do
        CharacterFactory.insert_character!(%{name: "Character #{i}"}, repo: UnboxedRepo)
      end

      # Verify the producer switches to buffering status
      assert_eventually SlotProducer.status(slot.id) == :buffering, 1000

      # Now ask for demand to trigger message delivery
      TestProcessor.ask(consumer_pid, 21)

      # Verify we receive all the expected messages
      messages = receive_messages(20)

      # Verify all messages are insert messages and we got the right count
      assert length(messages) == 20
      assert Enum.all?(messages, &(&1.kind == :insert))

      # Verify the producer is back to active status
      assert SlotProducer.status(slot.id) == :active
    end

    @tag start_opts: [processor_opts: [consumer_demand: :manual]]
    test "batch marker only reflects dispatched messages, not buffered messages", %{
      consumer_pid: consumer_pid
    } do
      # Insert data to generate messages
      # Do this in a transaction so they (likely) flood in at once
      UnboxedRepo.transaction(fn ->
        CharacterFactory.insert_character!(%{name: "Character 1"}, repo: UnboxedRepo)
        CharacterFactory.insert_character!(%{name: "Character 2"}, repo: UnboxedRepo)
        CharacterFactory.insert_character!(%{name: "Character 3"}, repo: UnboxedRepo)
      end)

      CharacterFactory.insert_character!(%{name: "Character 4"}, repo: UnboxedRepo)

      # Ask for only 1 message (leaving 3 buffered)
      TestProcessor.ask(consumer_pid, 1)

      # Receive the first message
      [%{commit_lsn: commit_lsn, commit_idx: commit_idx}] = receive_messages(1)

      # The batch marker should only reflect the dispatched message (msg1), not the buffered ones
      assert_receive {:batch_marker_received,
                      %BatchMarker{high_watermark_wal_cursor: %{commit_lsn: ^commit_lsn, commit_idx: ^commit_idx}}}

      # Now ask for two more messages
      # We can only ask for one at a time because our first ask set max_demand to 1.
      TestProcessor.ask(consumer_pid, 1)
      receive_messages(1)

      TestProcessor.ask(consumer_pid, 1)
      [%{commit_lsn: commit_lsn, commit_idx: commit_idx} = msg] = receive_messages(1)
      assert msg.payload =~ "Character 3"

      # Now the batch marker should reflect the highest dispatched message (msg3)
      assert_receive {:batch_marker_received,
                      %BatchMarker{high_watermark_wal_cursor: %{commit_lsn: ^commit_lsn, commit_idx: ^commit_idx}}}

      # Finally ask for last message
      TestProcessor.ask(consumer_pid, 1)
      [%{commit_lsn: commit_lsn, commit_idx: commit_idx}] = receive_messages(1)

      # Now the batch marker should reflect the highest dispatched message (msg3)
      assert_receive {:batch_marker_received,
                      %BatchMarker{high_watermark_wal_cursor: %{commit_lsn: ^commit_lsn, commit_idx: ^commit_idx}}}
    end

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
    #     restart_wal_cursor_fn: fn _state -> %{commit_lsn: 0} end,
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
  end

  defp receive_messages(count, acc \\ []) do
    assert_receive {:messages_received, messages}, 1_000

    case count - length(messages) do
      0 ->
        acc ++ messages

      next_count when next_count < 0 ->
        flunk("Received more messages than expected #{inspect(message_kinds(acc ++ messages))}")

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

  defp write_transaction_annotation(db, content) do
    Postgres.query(db, "select pg_logical_emit_message(true, 'sequin:transaction_annotations.set', $1);", [content])
  end

  defp clear_transaction_annotation(db) do
    Postgres.query(db, "select pg_logical_emit_message(true, 'sequin:transaction_annotations.clear', '');")
  end

  defp message_kinds(msgs) do
    Enum.map(msgs, & &1.kind)
  end

  # Helper functions
  defp start_slot_producer(pg_replication, opts \\ []) do
    db = pg_replication.postgres_database
    {processor_opts, opts} = Keyword.pop(opts, :processor_opts, [])

    opts =
      Keyword.merge(
        [
          id: pg_replication.id,
          database_id: db.id,
          account_id: db.account_id,
          slot_name: replication_slot(),
          publication_name: @publication,
          pg_major_version: 17,
          postgres_database: db,
          connect_opts: db_connect_opts(db),
          restart_wal_cursor_fn: fn _id, _last -> %{commit_lsn: 0, commit_idx: 0} end,
          test_pid: self(),
          conn: fn -> db end,
          consumer_mod: TestProcessor
        ],
        opts
      )

    producer_pid = start_supervised!({SlotProducer, opts})
    consumer_pid = start_test_consumer(producer_pid, self(), processor_opts)

    {producer_pid, consumer_pid}
  end

  defp start_test_consumer(producer_pid, test_pid, opts) do
    max_demand = Keyword.get(opts, :max_demand, 10)
    min_demand = Keyword.get(opts, :min_demand, 5)
    consumer_demand = Keyword.get(opts, :consumer_demand, :automatic)

    start_supervised!(
      {TestProcessor,
       %{
         producer: producer_pid,
         test_pid: test_pid,
         max_demand: max_demand,
         min_demand: min_demand,
         consumer_demand: consumer_demand
       }}
    )
  end

  defp db_connect_opts(postgres_database) do
    PostgresDatabase.to_protocol_opts(postgres_database)
  end
end

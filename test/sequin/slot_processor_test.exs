defmodule Sequin.Runtime.SlotProcessorTest do
  use Sequin.DataCase, async: true

  import ExUnit.CaptureLog

  alias Sequin.Databases.ConnectionCache
  alias Sequin.Error
  alias Sequin.Factory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Postgres
  alias Sequin.Replication
  alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.Relation
  alias Sequin.Runtime.SlotProcessor
  alias Sequin.Runtime.SlotProcessor.State
  alias Sequin.TestSupport.Models.Character

  setup do
    %{state: build_test_state()}
  end

  describe "handle_data/2 with relation messages" do
    test "properly enriches and stores relation schema information", %{state: state} do
      # Create a relation message for the Characters table from our test schema
      relation = %Relation{
        id: Character.table_oid(),
        namespace: "public",
        name: "Characters",
        replica_identity: :default,
        columns: [
          %Relation.Column{name: "id", flags: [:key], type: "int8", type_modifier: 0},
          %Relation.Column{name: "name", flags: [], type: "text", type_modifier: 0},
          %Relation.Column{name: "house", flags: [], type: "text", type_modifier: 0},
          %Relation.Column{name: "planet", flags: [], type: "text", type_modifier: 0}
        ]
      }

      # Process the relation message
      {:ok, [], updated_state} = SlotProcessor.handle_data(relation, state)

      # Verify that the schema was stored correctly
      assert Map.has_key?(updated_state.schemas, relation.id)
      stored_schema = updated_state.schemas[relation.id]

      # Check schema and table names
      assert stored_schema.schema == "public"
      assert stored_schema.table == "Characters"

      # Check that columns were enriched with primary key info and attnums
      assert length(stored_schema.columns) == length(relation.columns)

      # Find the id column and verify it's marked as a primary key
      id_column = Enum.find(stored_schema.columns, fn col -> col.name == "id" end)
      assert id_column.pk?
      assert is_integer(id_column.attnum)
    end

    test "handles partition tables by using parent table info", %{state: state} do
      {:ok, table_oid} = Postgres.fetch_table_oid(Sequin.Repo, "public", "test_event_logs_partitioned_default")
      # Create a relation message for a partition table
      relation = %Relation{
        id: table_oid,
        namespace: "public",
        name: "test_event_logs_partitioned_default",
        replica_identity: :default,
        columns: [
          %Relation.Column{name: "id", flags: [:key], type: "int8", type_modifier: 0},
          %Relation.Column{name: "seq", flags: [], type: "int8", type_modifier: 0},
          %Relation.Column{name: "committed_at", flags: [:key], type: "timestamptz", type_modifier: 0}
        ]
      }

      # Process the relation message
      {:ok, [], updated_state} = SlotProcessor.handle_data(relation, state)

      # Verify that the schema was stored with parent table info
      assert Map.has_key?(updated_state.schemas, relation.id)
      stored_schema = updated_state.schemas[relation.id]

      # The schema should have the parent table name, not the partition name
      assert stored_schema.table == "test_event_logs_partitioned"
      assert is_integer(stored_schema.parent_table_id)
    end
  end

  describe "handle_data/2 with binary messages" do
    test "accumulates binary messages correctly", %{state: state} do
      # Create a binary message (WAL data)
      # Format: <<?w, _header::192, msg_kind::8, msg::binary>>
      # Use ?I (Insert) as the message kind
      msg_kind = <<?I>>
      msg_content = "test message content"
      binary_msg = <<?w, 0::192, ?I, msg_content::binary>>

      # Process the binary message
      {:ok, [], updated_state} = SlotProcessor.handle_data(binary_msg, state)

      # Verify that the message was accumulated correctly
      assert updated_state.accumulated_msg_binaries.count == 1
      assert updated_state.accumulated_msg_binaries.bytes == byte_size(msg_kind <> msg_content)
      assert length(updated_state.accumulated_msg_binaries.binaries) == 1
      assert hd(updated_state.accumulated_msg_binaries.binaries).bin == msg_kind <> msg_content
    end

    test "accumulates multiple binary messages", %{state: state} do
      # Use ?I (Insert) and ?U (Update) as message kinds
      msg_kind1 = <<?I>>
      msg_kind2 = <<?U>>
      msg_content1 = "test message content 1"
      msg_content2 = "test message content 2"
      binary_msg1 = <<?w, 0::192>> <> msg_kind1 <> msg_content1
      binary_msg2 = <<?w, 0::192>> <> msg_kind2 <> msg_content2

      # Process the first binary message
      {:ok, [], state_after_first} = SlotProcessor.handle_data(binary_msg1, state)

      # Process the second binary message
      {:ok, [], state_after_second} = SlotProcessor.handle_data(binary_msg2, state_after_first)

      # Verify that both messages were accumulated correctly
      assert state_after_second.accumulated_msg_binaries.count == 2

      assert state_after_second.accumulated_msg_binaries.bytes ==
               byte_size(msg_kind1 <> msg_content1) + byte_size(msg_kind2 <> msg_content2)

      assert length(state_after_second.accumulated_msg_binaries.binaries) == 2

      # The binaries are stored in reverse order (newest first)
      [second, first] = state_after_second.accumulated_msg_binaries.binaries
      assert first.bin == msg_kind1 <> msg_content1
      assert second.bin == msg_kind2 <> msg_content2
    end

    test "increments commit_idx for each message and stores it in the envelope", %{state: state} do
      # Set a specific initial commit_idx for clarity
      initial_commit_idx = 100
      state = %{state | current_commit_idx: initial_commit_idx}

      # Create three different messages
      msg_kinds = [<<?I>>, <<?U>>, <<?D>>]
      msg_contents = ["insert content", "update content", "delete content"]

      binary_msgs =
        Enum.map(0..2, fn i ->
          <<?w, 0::192>> <> Enum.at(msg_kinds, i) <> Enum.at(msg_contents, i)
        end)

      # Process each message and collect the resulting states
      {final_state, states} =
        Enum.reduce(binary_msgs, {state, []}, fn msg, {current_state, acc_states} ->
          {:ok, [], updated_state} = SlotProcessor.handle_data(msg, current_state)
          {updated_state, [updated_state | acc_states]}
        end)

      # Reverse states to get them in processing order
      states = Enum.reverse(states)

      # Verify that commit_idx was incremented correctly for each message
      Enum.each(0..2, fn i ->
        state = Enum.at(states, i)
        assert state.current_commit_idx == initial_commit_idx + i + 1
      end)

      # Verify that the final state has all messages with correct commit_idx values
      assert final_state.accumulated_msg_binaries.count == 3

      # Messages are stored in reverse order (newest first)
      [msg3, msg2, msg1] = final_state.accumulated_msg_binaries.binaries

      assert msg1.commit_idx == initial_commit_idx
      assert msg2.commit_idx == initial_commit_idx + 1
      assert msg3.commit_idx == initial_commit_idx + 2

      # Verify the message contents
      assert msg1.bin == Enum.at(msg_kinds, 0) <> Enum.at(msg_contents, 0)
      assert msg2.bin == Enum.at(msg_kinds, 1) <> Enum.at(msg_contents, 1)
      assert msg3.bin == Enum.at(msg_kinds, 2) <> Enum.at(msg_contents, 2)
    end

    test "resets commit_idx to 0 after Commit followed by Begin", %{state: state} do
      alias Sequin.Postgres
      alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.Begin
      alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.Commit

      # Set initial state with a specific commit_idx
      initial_commit_idx = 100
      initial_lsn = 1000
      initial_timestamp = 1_234_567_890

      state = %{
        state
        | current_commit_idx: initial_commit_idx,
          current_xaction_lsn: initial_lsn,
          current_commit_ts: initial_timestamp
      }

      # Create some change messages (Insert, Update)
      msg_kind1 = <<?I>>
      msg_kind2 = <<?U>>
      msg_content1 = "first transaction message"
      msg_content2 = "second transaction message"
      binary_msg1 = <<?w, 0::192>> <> msg_kind1 <> msg_content1
      binary_msg2 = <<?w, 0::192>> <> msg_kind2 <> msg_content2

      # Process the first two messages in the first transaction
      {:ok, [], state_after_first} = SlotProcessor.handle_data(binary_msg1, state)
      {:ok, [], state_after_second} = SlotProcessor.handle_data(binary_msg2, state_after_first)

      # Verify that commit_idx was incremented correctly
      assert state_after_first.current_commit_idx == initial_commit_idx + 1
      assert state_after_second.current_commit_idx == initial_commit_idx + 2

      {:ok, lsn} = Postgres.ReplicationConnection.encode_lsn(initial_lsn)
      # Create a Commit message for the first transaction
      commit_msg = %Commit{
        lsn: lsn,
        commit_timestamp: initial_timestamp
      }

      # Process the Commit message
      {:ok, [], state_after_commit} = SlotProcessor.handle_data(commit_msg, state_after_second)

      # Verify that commit_idx was reset to 0 after Commit
      assert state_after_commit.current_commit_idx == 0
      assert state_after_commit.current_xaction_lsn == nil
      assert state_after_commit.current_commit_ts == nil
      assert state_after_commit.last_commit_lsn == initial_lsn

      # Create a Begin message for a new transaction
      new_lsn = 2000
      new_timestamp = 1_234_568_000
      new_xid = 12_345

      {:ok, final_lsn} = Postgres.ReplicationConnection.encode_lsn(new_lsn)

      begin_msg = %Begin{
        final_lsn: final_lsn,
        commit_timestamp: new_timestamp,
        xid: new_xid
      }

      # Process the Begin message
      {:ok, [], state_after_begin} = SlotProcessor.handle_data(begin_msg, state_after_commit)

      # Verify that commit_idx is still 0 after Begin
      assert state_after_begin.current_commit_idx == 0
      assert state_after_begin.current_xaction_lsn == new_lsn
      assert state_after_begin.current_commit_ts == new_timestamp
      assert state_after_begin.current_xid == new_xid

      # Create a new message for the second transaction
      msg_kind3 = <<?I>>
      msg_content3 = "new transaction message"
      binary_msg3 = <<?w, 0::192>> <> msg_kind3 <> msg_content3

      # Process the message in the second transaction
      {:ok, [], state_after_new_msg} = SlotProcessor.handle_data(binary_msg3, state_after_begin)

      # Verify that commit_idx starts from 0 and increments correctly in the new transaction
      assert state_after_new_msg.current_commit_idx == 1

      # Verify that the message envelope contains the correct transaction info
      [envelope | _rest] = state_after_new_msg.accumulated_msg_binaries.binaries
      assert envelope.commit_idx == 0
      assert envelope.commit_lsn == new_lsn
      assert envelope.commit_timestamp == new_timestamp
      assert envelope.bin == msg_kind3 <> msg_content3
    end

    test "checks memory limit on an interval, returns error when memory limit is exceeded" do
      # Create a state with a memory check function that always fails
      # but with bytes_between_limit_checks set high enough to require multiple messages
      state =
        build_test_state(%{
          max_memory_bytes: 100,
          # Set higher than our first message
          bytes_between_limit_checks: 20,
          # Always returns over the limit
          check_memory_fn: fn -> 1000 end
        })

      # 5 bytes
      small_msg = "small"
      # 24 bytes
      large_msg = "this is a larger message"
      binary_msg1 = <<?w, 0::192, ?I, small_msg::binary>>
      binary_msg2 = <<?w, 0::192, ?I, large_msg::binary>>

      # First message should succeed because we don't check the limit yet
      # (bytes received < bytes_between_limit_checks)
      {:ok, [], updated_state} = SlotProcessor.handle_data(binary_msg1, state)

      # Second message should trigger the check and fail
      # (bytes received > bytes_between_limit_checks)
      result = SlotProcessor.handle_data(binary_msg2, updated_state)
      assert {:error, %Error.InvariantError{code: :over_system_memory_limit}} = result
    end
  end

  describe "handle_data/2 with keepalive messages" do
    @describetag capture_log: true
    setup %{state: state} do
      # Set up the low watermark in Redis
      Replication.put_low_watermark_wal_cursor!(state.id, %{commit_lsn: 789, commit_idx: 101})

      :ok
    end

    test "handles keepalive with reply=0 correctly", %{state: state} do
      # Create a keepalive message with reply=0
      wal_end = 1000
      clock = 1_234_567_890
      keepalive_msg = <<?k, wal_end::64, clock::64, 0>>

      # Process the keepalive message
      {:ok, [], updated_state} = SlotProcessor.handle_data(keepalive_msg, state)

      # Verify that no reply was generated and state is unchanged
      assert updated_state == state
    end

    test "handles keepalive with reply=1 and nil last_commit_lsn", %{state: state} do
      # Create a keepalive message with reply=1
      wal_end = 1000
      clock = 1_234_567_890
      keepalive_msg = <<?k, wal_end::64, clock::64, 1>>

      # Process the keepalive message
      {:ok, [reply], _updated_state} = SlotProcessor.handle_data(keepalive_msg, state)

      # Extract the LSN values from the reply
      <<114, lsn1::64, lsn2::64, lsn3::64, _timestamp::64, 0>> = reply

      # Verify that all three LSN values in the reply are the same
      assert lsn1 == lsn2
      assert lsn2 == lsn3

      # Get the low watermark value that was set in the setup
      {:ok, low_watermark} = Replication.low_watermark_wal_cursor(state.id)

      # Verify that the LSN in the reply matches the low watermark
      assert lsn1 == low_watermark.commit_lsn
    end

    test "handles keepalive with reply=1 and existing last_commit_lsn", %{state: state} do
      # Set last_commit_lsn in the state
      state = %{state | last_commit_lsn: 456}

      # Create a keepalive message with reply=1
      wal_end = 1000
      clock = 1_234_567_890
      keepalive_msg = <<?k, wal_end::64, clock::64, 1>>

      # Process the keepalive message
      {:ok, [reply], _updated_state} = SlotProcessor.handle_data(keepalive_msg, state)

      # Verify that a reply was generated with the low_watermark value
      assert <<?r, 789::64, 789::64, 789::64, _timestamp::64, 0>> = reply
    end
  end

  describe "handle_data/2 with logical messages" do
    test "handles unknown binary data gracefully", %{state: state} do
      # Create an unknown binary message
      unknown_msg = <<1, 2, 3, 4, 5>>

      assert capture_log(fn ->
               {:ok, [], ^state} = SlotProcessor.handle_data(unknown_msg, state)
             end) =~ "Unknown data"
    end
  end

  # Private helper functions

  defp build_test_state(overrides \\ []) do
    overrides = Map.new(overrides)
    db = Map.get_lazy(overrides, :db, fn -> DatabasesFactory.insert_configured_postgres_database!() end)
    ConnectionCache.cache_connection(db, Sequin.Repo)

    base_state = %State{
      postgres_database: db,
      schemas: %{},
      id: "test-slot-id-#{Factory.uuid()}",
      check_memory_fn: fn -> 0 end,
      bytes_between_limit_checks: Factory.integer(),
      max_memory_bytes: Factory.integer(),
      safe_wal_cursor_fn: fn _state -> %{commit_lsn: Factory.integer(), commit_idx: Factory.integer()} end,
      accumulated_msg_binaries: %{count: 0, bytes: 0, binaries: []},
      low_watermark_wal_cursor: %{commit_lsn: Factory.integer(), commit_idx: Factory.integer()},
      current_xaction_lsn: Factory.integer(),
      current_commit_idx: Factory.integer()
    }

    Map.merge(base_state, overrides)
  end
end

defmodule Sequin.Runtime.SlotProcessorTest do
  use Sequin.DataCase, async: true

  import ExUnit.CaptureLog

  alias Sequin.Databases.ConnectionCache
  alias Sequin.Error
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
      # Format: <<?w, _header::192, msg::binary>>
      header = <<0::192>>
      msg_content = "test message content"
      binary_msg = <<?w>> <> header <> msg_content

      # Process the binary message
      {:ok, [], updated_state} = SlotProcessor.handle_data(binary_msg, state)

      # Verify that the message was accumulated correctly
      assert updated_state.accumulated_msg_binaries.count == 1
      assert updated_state.accumulated_msg_binaries.bytes == byte_size(msg_content)
      assert length(updated_state.accumulated_msg_binaries.binaries) == 1
      assert hd(updated_state.accumulated_msg_binaries.binaries) == msg_content
    end

    test "accumulates multiple binary messages", %{state: state} do
      # Create multiple binary messages
      header = <<0::192>>
      msg_content1 = "test message content 1"
      msg_content2 = "test message content 2"
      binary_msg1 = <<?w>> <> header <> msg_content1
      binary_msg2 = <<?w>> <> header <> msg_content2

      # Process the first binary message
      {:ok, [], state_after_first} = SlotProcessor.handle_data(binary_msg1, state)

      # Process the second binary message
      {:ok, [], state_after_second} = SlotProcessor.handle_data(binary_msg2, state_after_first)

      # Verify that both messages were accumulated correctly
      assert state_after_second.accumulated_msg_binaries.count == 2
      assert state_after_second.accumulated_msg_binaries.bytes == byte_size(msg_content1) + byte_size(msg_content2)
      assert length(state_after_second.accumulated_msg_binaries.binaries) == 2

      # The binaries are stored in reverse order (newest first)
      [second, first] = state_after_second.accumulated_msg_binaries.binaries
      assert first == msg_content1
      assert second == msg_content2
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

      # Create two binary messages
      header = <<0::192>>
      # 5 bytes
      small_msg = "small"
      # 24 bytes
      large_msg = "this is a larger message"
      binary_msg1 = <<?w>> <> header <> small_msg
      binary_msg2 = <<?w>> <> header <> large_msg

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

      # Verify that a reply was generated with the safe_wal_cursor value
      assert <<?r, 123::64, 123::64, 123::64, _timestamp::64, 0>> = reply
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
      id: "test-slot-id",
      check_memory_fn: fn -> 0 end,
      bytes_between_limit_checks: 1_000_000,
      max_memory_bytes: 100_000_000,
      safe_wal_cursor_fn: fn _state -> %{commit_lsn: 123, commit_idx: 456} end,
      accumulated_msg_binaries: %{count: 0, bytes: 0, binaries: []}
    }

    Map.merge(base_state, overrides)
  end
end

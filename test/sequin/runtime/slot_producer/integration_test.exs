defmodule Sequin.Runtime.SlotProducer.IntegrationTest do
  @moduledoc """
  Integration test for the complete SlotProducer → Processor → ReorderBuffer pipeline.

  This test verifies that PostgreSQL replication messages flow seamlessly through
  all three GenStage components in the correct order.
  """
  use Sequin.DataCase, async: false
  use AssertEventually, interval: 1

  alias Sequin.Databases.ConnectionCache
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.CharacterFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Postgres
  alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.LogicalMessage
  alias Sequin.Runtime.SlotProcessor
  alias Sequin.Runtime.SlotProducer
  alias Sequin.Runtime.SlotProducer.Processor
  alias Sequin.Runtime.SlotProducer.Supervisor
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

  describe "complete SlotProducer → Processor → ReorderBuffer pipeline" do
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

      if !ctx[:skip_start] do
        start_pipeline(pg_replication, Map.get(ctx, :start_opts, []))
      end

      {:ok, %{postgres_replication: pg_replication}}
    end

    test "messages flow through complete pipeline" do
      # Insert test data to generate replication messages
      CharacterFactory.insert_character!(%{name: "Alice"}, repo: UnboxedRepo)
      CharacterFactory.insert_character!(%{name: "Bob"}, repo: UnboxedRepo)
      CharacterFactory.insert_character!(%{name: "Zed"}, repo: UnboxedRepo)

      # Wait for messages to flow through the complete pipeline
      [batch1] = receive_messages_batched(3)
      messages = batch1.messages

      # Messages should be properly wrapped SlotProcessor.Messages
      assert Enum.all?(messages, fn %SlotProducer.Message{} = msg ->
               inner_msg = %SlotProcessor.Message{} = msg.message

               inner_msg.action == :insert and inner_msg.table_name == Character.table_name() and
                 msg.commit_lsn == inner_msg.commit_lsn and
                 msg.commit_idx == inner_msg.commit_idx
             end)

      # Verify messages are ordered by commit_lsn/commit_idx
      [{lsn1, idx1}, {lsn2, idx2}, {lsn3, idx3}] =
        Enum.map(messages, fn msg -> {msg.commit_lsn, msg.commit_idx} end)

      assert {lsn1, idx1} <= {lsn2, idx2}
      assert {lsn2, idx2} <= {lsn3, idx3}

      # Verify batch marker has correct epoch and high watermark
      assert batch1.idx == 0
      assert is_integer(batch1.high_watermark_wal_cursor.commit_lsn)
      assert batch1.high_watermark_wal_cursor.commit_lsn == lsn3
      assert batch1.high_watermark_wal_cursor.commit_idx == idx3

      # Insert more data to test second batch
      UnboxedRepo.update_all(Character, set: [name: "Updated"])

      # Wait for messages to flow through the complete pipeline
      [batch2] = receive_messages_batched(3)
      messages = batch2.messages

      assert batch2.idx == 1
      # Two update messages
      assert Enum.all?(messages, fn msg -> msg.message.action == :update end)

      # Second batch LSN should be higher than first
      assert batch2.high_watermark_wal_cursor.commit_lsn > batch1.high_watermark_wal_cursor.commit_lsn
    end

    @tag skip_start: true
    test "relations are properly synchronized to processors that subscribe after relations are received", %{
      postgres_replication: slot
    } do
      # A message will be waiting
      CharacterFactory.insert_character!(%{name: "Super Early Bird"}, repo: UnboxedRepo)

      opts = [processor_opts: [subscribe_to: []]]

      # Start pipeline but don't subscribe processors yet
      start_pipeline(slot, opts)

      producer = SlotProducer.via_tuple(slot.id)
      first_processor = Processor.via_tuple(slot.id, 0)

      # Subscribe first processor to SlotProducer and ReorderBuffer
      {:ok, _subscription_tag} = GenStage.sync_subscribe(first_processor, to: producer)

      # Insert a character to generate a relation message
      CharacterFactory.insert_character!(%{name: "Early Bird"}, repo: UnboxedRepo)

      # No batch markers could have flowed all the way through
      refute_receive {:batch, _, _}, 50

      # Now subscribe the remaining processors (late joiners)
      for partition_idx <- 1..(Processor.partition_count() - 1) do
        {:ok, _subscription_tag} = GenStage.sync_subscribe(Processor.via_tuple(slot.id, partition_idx), to: producer)
      end

      # Insert many more characters to ensure messages flow through all partitions
      for i <- 1..10 do
        CharacterFactory.insert_character!(%{name: "Character #{i}"}, repo: UnboxedRepo)
      end

      # Wait for all messages - should work fine because late joiners got relations
      messages = receive_messages(12)

      # All messages should be properly processed (proving relations were sent to late joiners)
      assert Enum.all?(messages, fn %SlotProducer.Message{} = msg ->
               inner_msg = msg.message

               is_struct(inner_msg, SlotProcessor.Message) and
                 inner_msg.action == :insert and inner_msg.table_name == Character.table_name()
             end)
    end

    test "transaction boundaries are preserved through the complete pipeline" do
      # First, insert a character outside any transaction to establish baseline
      CharacterFactory.insert_character!(%{name: "Solo"}, repo: UnboxedRepo)

      # Wait for first message
      [solo_msg] = receive_messages(1)
      assert solo_msg.message.action == :insert
      # First message in its transaction
      assert solo_msg.commit_idx == 0

      # Now insert multiple characters in a single transaction
      # This should generate multiple messages with same commit_lsn but different commit_idx
      UnboxedRepo.transaction(fn ->
        char1 = CharacterFactory.insert_character!(%{name: "Txn Char 1"}, repo: UnboxedRepo)
        char2 = CharacterFactory.insert_character!(%{name: "Txn Char 2"}, repo: UnboxedRepo)
        {char1, char2}
      end)

      # Wait for transaction messages - should come in separate batches due to low batch size
      [txn_msg1, txn_msg2] = receive_messages(2)

      # Sort by commit_idx to ensure proper order
      [first_txn_msg, second_txn_msg] = Enum.sort_by([txn_msg1, txn_msg2], & &1.commit_idx)

      # Verify transaction messages have same commit_lsn (same transaction)
      assert first_txn_msg.commit_lsn == second_txn_msg.commit_lsn

      # Verify different commit_idx within transaction
      assert first_txn_msg.commit_idx == 0
      assert second_txn_msg.commit_idx == 1

      # Verify transaction LSN is higher than solo message
      assert first_txn_msg.commit_lsn > solo_msg.commit_lsn

      # Insert another character after the transaction
      CharacterFactory.insert_character!(%{name: "Post Txn"}, repo: UnboxedRepo)

      # Wait for post-transaction message
      [post_txn_msg] = receive_messages(1)

      # Post-transaction message should have different (higher) LSN
      assert post_txn_msg.commit_lsn > first_txn_msg.commit_lsn
      # First message in its transaction
      assert post_txn_msg.commit_idx == 0
    end

    test "logical messages flow through complete pipeline", %{postgres_replication: slot} do
      # Emit a logical message using pg_logical_emit_message
      subject = "test-subject"
      content = ~s|{"event": "test", "data": "logical message content"}|

      Postgres.query!(slot.postgres_database, "SELECT pg_logical_emit_message(true, $1, $2)", [subject, content])

      # Wait for the logical message to flow through the pipeline
      [msg] = receive_messages(1)

      # Verify it's a LogicalMessage struct
      assert %LogicalMessage{} = logical_msg = msg.message

      # Verify the content matches what we sent
      assert logical_msg.prefix == subject
      assert logical_msg.content == content
    end
  end

  defp receive_messages(count) do
    batches = receive_messages_batched(count)
    Enum.flat_map(batches, & &1.messages)
  end

  defp receive_messages_batched(count, acc_messages \\ [], acc_batches \\ []) do
    assert_receive {:batch, batch}, 1_000
    new_messages = acc_messages ++ batch.messages
    new_batches = [batch | acc_batches]

    cond do
      length(new_messages) == count ->
        Enum.reverse(new_batches)

      length(new_messages) > count ->
        flunk("Received more messages than expected #{inspect(message_actions(new_messages))})")

      true ->
        receive_messages_batched(count, new_messages, new_batches)
    end
  rescue
    err ->
      remaining = count - length(acc_messages)

      case err do
        %ExUnit.AssertionError{message: "Assertion failed, no matching message after" <> _rest} ->
          flunk("Did not receive remaining #{remaining} messages (got: #{inspect(message_actions(acc_messages))})")

        err ->
          reraise err, __STACKTRACE__
      end
  end

  defp message_actions(msgs) do
    Enum.map(msgs, & &1.message.action)
  end

  defp start_pipeline(postgres_replication, opts) do
    test_pid = self()

    # Start ReorderBuffer first
    reorder_buffer_opts = [
      flush_batch_fn: fn _id, batch ->
        send(test_pid, {:batch, batch})
        :ok
      end
    ]

    processor_opts = Keyword.get(opts, :processor_opts, [])

    slot_producer_opts =
      Keyword.merge(
        [
          restart_wal_cursor_fn: fn _id, _last -> %{commit_lsn: 0, commit_idx: 0} end,
          batch_flush_interval: 10,
          test_pid: self()
        ],
        Keyword.get(opts, :slot_producer_opts, [])
      )

    opts = [
      replication_slot: postgres_replication,
      reorder_buffer_opts: reorder_buffer_opts,
      processor_opts: processor_opts,
      slot_producer_opts: slot_producer_opts,
      slot_processor_opts: [skip_start?: true]
    ]

    start_supervised!({Supervisor, opts})
  end
end

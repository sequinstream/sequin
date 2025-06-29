defmodule Sequin.Runtime.SlotProducer.IntegrationTest do
  @moduledoc """
  Integration test for the complete SlotProducer → Processor → ReorderBuffer pipeline.

  This test verifies that PostgreSQL replication messages flow seamlessly through
  all three GenStage components in the correct order.
  """
  use Sequin.DataCase, async: false
  use AssertEventually, interval: 1

  alias Sequin.Databases.ConnectionCache
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.CharacterFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Runtime.SlotProcessor.Message
  alias Sequin.Runtime.SlotProducer
  alias Sequin.Runtime.SlotProducer.BatchMarker
  alias Sequin.Runtime.SlotProducer.Processor
  alias Sequin.Runtime.SlotProducer.ReorderBuffer
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

      unless ctx[:skip_start] do
        start_pipeline(postgres_database, Map.get(ctx, :start_opts, []))
      end

      {:ok, %{postgres_database: postgres_database, pg_replication: pg_replication}}
    end

    @tag start_opts: [
           slot_producer: [batch_flush_interval: [max_messages: 3, max_bytes: 1024 * 1024 * 10, max_age: 5_000]]
         ]
    test "messages flow through complete pipeline" do
      # Insert test data to generate replication messages
      CharacterFactory.insert_character!(%{name: "Alice"}, repo: UnboxedRepo)
      CharacterFactory.insert_character!(%{name: "Bob"}, repo: UnboxedRepo)
      CharacterFactory.insert_character!(%{name: "Zed"}, repo: UnboxedRepo)

      # Wait for messages to flow through the complete pipeline
      {[%BatchMarker{} = marker], messages} = receive_messages(3)

      # Messages should be properly processed SlotProcessor.Messages
      assert Enum.all?(messages, fn msg ->
               is_struct(msg, Message) and
                 msg.action == :insert and msg.table_name == Character.table_name()
             end)

      # Verify messages are ordered by commit_lsn/commit_idx
      [{lsn1, idx1}, {lsn2, idx2}, {lsn3, idx3}] =
        Enum.map(messages, fn msg -> {msg.commit_lsn, msg.commit_idx} end)

      assert {lsn1, idx1} <= {lsn2, idx2}
      assert {lsn2, idx2} <= {lsn3, idx3}

      # Verify batch marker has correct epoch and high watermark
      assert marker.epoch == 0
      assert is_integer(marker.high_watermark_wal_cursor.commit_lsn)
      assert marker.high_watermark_wal_cursor.commit_lsn == lsn3
      assert marker.high_watermark_wal_cursor.commit_idx == idx3

      # Insert more data to test second batch
      UnboxedRepo.update_all(Character, set: [name: "Updated"])

      # Wait for messages to flow through the complete pipeline
      {[%BatchMarker{} = marker2], messages} = receive_messages(3)

      assert marker2.epoch == 1
      # Two update messages
      assert Enum.all?(messages, fn msg -> msg.action == :update end)

      # Second batch LSN should be higher than first
      assert marker2.high_watermark_wal_cursor.commit_lsn > marker.high_watermark_wal_cursor.commit_lsn
    end

    @tag skip_start: true
    test "relations are properly synchronized to processors that subscribe after relations are received", %{
      postgres_database: postgres_database
    } do
      # A message will be waiting
      CharacterFactory.insert_character!(%{name: "Super Early Bird"}, repo: UnboxedRepo)

      opts = [slot_producer: [batch_flush_interval: [max_messages: 2, max_bytes: 1024 * 1024 * 10, max_age: 5_000]]]

      # Start pipeline but don't subscribe processors yet
      {slot_producer_pid, processor_pids, _reorder_buffer_pid} =
        start_pipeline(postgres_database, opts, subscribe_processors?: false)

      # Subscribe only the first processor partition
      [{_first_partition_idx, first_processor_pid} | remaining_processors] = processor_pids

      # Subscribe first processor to SlotProducer
      {:ok, _subscription_tag} = GenStage.sync_subscribe(first_processor_pid, to: slot_producer_pid)

      # Insert a character to generate a relation message
      CharacterFactory.insert_character!(%{name: "Early Bird"}, repo: UnboxedRepo)

      # No batch markers could have flowed all the way through
      refute_receive {:batch, _, _}, 50

      # Now subscribe the remaining processors (late joiners)
      for {_partition_idx, processor_pid} <- remaining_processors do
        {:ok, _subscription_tag} = GenStage.sync_subscribe(processor_pid, to: slot_producer_pid)
      end

      # Insert many more characters to ensure messages flow through all partitions
      for i <- 1..10 do
        CharacterFactory.insert_character!(%{name: "Character #{i}"}, repo: UnboxedRepo)
      end

      # Wait for all messages - should work fine because late joiners got relations
      {_markers, messages} = receive_messages(12)

      # All messages should be properly processed (proving relations were sent to late joiners)
      assert Enum.all?(messages, fn msg ->
               is_struct(msg, Message) and
                 msg.action == :insert and msg.table_name == Character.table_name()
             end)
    end

    @tag start_opts: [
           slot_producer: [batch_flush_interval: [max_messages: 1, max_bytes: 1024, max_age: 50]]
         ]
    test "transaction boundaries are preserved through the complete pipeline" do
      # First, insert a character outside any transaction to establish baseline
      CharacterFactory.insert_character!(%{name: "Solo"}, repo: UnboxedRepo)

      # Wait for first message
      {_markers, [solo_msg]} = receive_messages(1)
      assert solo_msg.action == :insert
      # First message in its transaction
      assert solo_msg.commit_idx == 1

      # Now insert multiple characters in a single transaction
      # This should generate multiple messages with same commit_lsn but different commit_idx
      UnboxedRepo.transaction(fn ->
        char1 = CharacterFactory.insert_character!(%{name: "Txn Char 1"}, repo: UnboxedRepo)
        char2 = CharacterFactory.insert_character!(%{name: "Txn Char 2"}, repo: UnboxedRepo)
        {char1, char2}
      end)

      # Wait for transaction messages - should come in separate batches due to low batch size
      {_markers, [txn_msg1, txn_msg2]} = receive_messages(2)

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
      {_markers, [post_txn_msg]} = receive_messages(1)

      # Post-transaction message should have different (higher) LSN
      assert post_txn_msg.commit_lsn > first_txn_msg.commit_lsn
      # First message in its transaction
      assert post_txn_msg.commit_idx == 0
    end
  end

  defp receive_messages(count, {acc_markers, acc_messages} \\ {[], []}) do
    assert_receive {:batch, batch_marker, messages}, 1_000
    acc_messages = acc_messages ++ messages
    acc_markers = [batch_marker | acc_markers]
    acc = {acc_markers, acc_messages}

    cond do
      length(acc_messages) == count ->
        acc

      length(acc_messages) > count ->
        flunk("Received more messages than expected #{inspect(message_actions(acc))})")

      true ->
        receive_messages(count, acc)
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
    Enum.map(msgs, & &1.action)
  end

  defp start_pipeline(postgres_database, opts, pipeline_opts \\ []) do
    test_pid = self()
    pipeline_id = UUID.uuid4()
    subscribe_processors? = Keyword.get(pipeline_opts, :subscribe_processors?, true)

    # Start ReorderBuffer first
    reorder_buffer_opts = [
      id: pipeline_id,
      producer_partitions: Processor.partition_count(),
      on_batch_ready: fn batch_marker, messages ->
        send(test_pid, {:batch, batch_marker, messages})
        :ok
      end
    ]

    {:ok, reorder_buffer_pid} = start_supervised({ReorderBuffer, reorder_buffer_opts})

    # Start Processor partitions
    processor_pids =
      for partition_idx <- Processor.partitions() do
        processor_opts = [
          id: pipeline_id,
          partition_idx: partition_idx
        ]

        {:ok, processor_pid} =
          start_supervised({Processor, processor_opts}, id: {:processor, partition_idx})

        # Subscribe ReorderBuffer to this Processor partition
        {:ok, _subscription_tag} = GenStage.sync_subscribe(reorder_buffer_pid, to: processor_pid)

        {partition_idx, processor_pid}
      end

    # Start SlotProducer
    slot_producer_opts =
      Keyword.merge(
        [
          id: pipeline_id,
          database_id: postgres_database.id,
          slot_name: replication_slot(),
          publication_name: @publication,
          pg_major_version: 17,
          postgres_database: postgres_database,
          connect_opts: db_connect_opts(postgres_database),
          restart_wal_cursor_fn: fn _state -> %{commit_lsn: 0, commit_idx: 0} end,
          conn: fn -> postgres_database end,
          processor_mod: Processor,
          # Fast batch flush for testing
          batch_flush_interval: [max_messages: 2, max_bytes: 1024 * 1024 * 1024, max_age: 100]
        ],
        opts
      )

    {:ok, slot_producer_pid} = start_supervised({SlotProducer, slot_producer_opts})

    # Conditionally subscribe Processor partitions to SlotProducer
    if subscribe_processors? do
      for {_partition_idx, processor_pid} <- processor_pids do
        {:ok, _subscription_tag} = GenStage.sync_subscribe(processor_pid, to: slot_producer_pid)
      end
    end

    {slot_producer_pid, processor_pids, reorder_buffer_pid}
  end

  defp db_connect_opts(postgres_database) do
    postgres_database
    |> PostgresDatabase.to_postgrex_opts()
    |> Keyword.drop([:socket, :socket_dir, :endpoints])
  end
end

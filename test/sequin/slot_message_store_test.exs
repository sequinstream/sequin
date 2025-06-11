defmodule Sequin.SlotMessageStoreTest do
  use Sequin.DataCase, async: true

  import ExUnit.CaptureLog

  alias Sequin.Consumers
  alias Sequin.Consumers.AcknowledgedMessages
  alias Sequin.Error
  alias Sequin.Error.InvariantError
  alias Sequin.Factory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Runtime.SlotMessageStore
  alias Sequin.Runtime.SlotMessageStore.State
  alias Sequin.Runtime.SlotMessageStoreSupervisor

  @moduletag :capture_log

  describe "SlotMessageStore with persisted messages" do
    setup do
      consumer = ConsumersFactory.insert_sink_consumer!()

      msg1 = ConsumersFactory.insert_consumer_message!(message_kind: consumer.message_kind, consumer_id: consumer.id)
      msg2 = ConsumersFactory.insert_consumer_message!(message_kind: consumer.message_kind, consumer_id: consumer.id)

      start_supervised!({SlotMessageStoreSupervisor, consumer_id: consumer.id, test_pid: self()})

      %{consumer: consumer, msg1: msg1, msg2: msg2}
    end

    test "messages from disk are loaded on init and then produce_messages returns them", %{
      consumer: consumer,
      msg1: msg1,
      msg2: msg2
    } do
      assert {:ok, delivered} = SlotMessageStore.produce(consumer, 2, self())
      assert length(delivered) == 2
      assert Enum.all?(delivered, fn msg -> msg.id in [msg1.id, msg2.id] end)

      assert Enum.any?(SlotMessageStore.peek(consumer), fn state ->
               state.payload_size_bytes > 0
             end)
    end

    test "producing and acking persisted messages deletes them from disk", %{consumer: consumer} do
      refute [] == Consumers.list_consumer_messages_for_consumer(consumer)

      assert {:ok, delivered} = SlotMessageStore.produce(consumer, 10, self())
      assert length(delivered) == 2

      {:ok, 2} = SlotMessageStore.messages_succeeded(consumer, delivered)

      assert [] == Consumers.list_consumer_messages_for_consumer(consumer)
    end

    test "putting messages with persisted group_ids will upsert to postgres", %{
      consumer: consumer,
      msg1: msg1
    } do
      new_message =
        ConsumersFactory.consumer_message(
          table_oid: msg1.table_oid,
          group_id: msg1.group_id,
          message_kind: consumer.message_kind,
          consumer_id: consumer.id
        )

      :ok = SlotMessageStore.put_messages(consumer, [new_message])

      consumer_id = consumer.id
      assert_receive {:put_messages_done, ^consumer_id}, 1000

      persisted_messages = Consumers.list_consumer_messages_for_consumer(consumer)
      assert length(persisted_messages) == 3

      assert new_message.record_pks in Enum.map(persisted_messages, & &1.record_pks)
    end
  end

  describe "SlotMessageStore with persisted messages that exceed max_memory_bytes" do
    test "raises an error" do
      consumer = ConsumersFactory.insert_sink_consumer!(partition_count: 1)

      data = ConsumersFactory.consumer_message_data(message_kind: consumer.message_kind)
      data_size_bytes = :erlang.external_size(data)

      for _ <- 1..20 do
        ConsumersFactory.insert_consumer_message!(
          message_kind: consumer.message_kind,
          consumer_id: consumer.id,
          data: data
        )
      end

      max_memory_bytes = data_size_bytes * 10 / 2

      log =
        capture_log(fn ->
          pid =
            start_supervised!(
              {SlotMessageStoreSupervisor,
               consumer_id: consumer.id, max_memory_bytes: max_memory_bytes, test_pid: self(), restart: :temporary}
            )

          Process.monitor(pid)

          assert_receive {:DOWN, _, :process, ^pid, _}
        end)

      assert log =~ "Max memory limit exceeded"
    end
  end

  describe "SlotMessageStore with persisted messages that exceed max_memory_bytes and multiple partitions" do
    setup do
      consumer = ConsumersFactory.insert_sink_consumer!(partition_count: 3)

      data = ConsumersFactory.consumer_message_data(message_kind: consumer.message_kind)
      data_size_bytes = :erlang.external_size(data)

      for _ <- 1..100 do
        ConsumersFactory.insert_consumer_message!(
          message_kind: consumer.message_kind,
          consumer_id: consumer.id,
          data: data
        )
      end

      max_memory_bytes = data_size_bytes * 3

      log =
        capture_log(fn ->
          pid =
            start_supervised!(
              {SlotMessageStoreSupervisor,
               consumer_id: consumer.id, max_memory_bytes: max_memory_bytes, test_pid: self(), restart: :temporary}
            )

          Process.monitor(pid)

          assert_receive {:DOWN, _, :process, ^pid, _}
        end)

      assert log =~ "Max memory limit exceeded"

      %{consumer: consumer}
    end
  end

  describe "SlotMessageStore message handling" do
    setup do
      consumer = ConsumersFactory.insert_sink_consumer!()
      event_consumer = ConsumersFactory.insert_sink_consumer!(message_kind: :event)
      start_supervised!({SlotMessageStoreSupervisor, consumer_id: consumer.id, test_pid: self()})
      start_supervised!({SlotMessageStoreSupervisor, consumer_id: event_consumer.id, test_pid: self()})
      %{consumer: consumer, event_consumer: event_consumer}
    end

    test "puts, delivers, and acks in-memory messages", %{consumer: consumer} do
      # Create test events
      messages = [
        ConsumersFactory.consumer_message(),
        ConsumersFactory.consumer_message()
      ]

      # Put messages in store
      :ok = SlotMessageStore.put_messages(consumer, messages)

      # Retrieve messages
      {:ok, delivered} = SlotMessageStore.produce(consumer, 2, self())
      assert length(delivered) == 2

      # For acks
      {:ok, 2} = SlotMessageStore.messages_succeeded(consumer, delivered)

      # Produce messages, none should be delivered
      {:ok, []} = SlotMessageStore.produce(consumer, 2, self())
    end

    test "deletes failed then succeeded messages from postgres", %{consumer: consumer} do
      # Create message with a group_id
      group_id = "test-group"

      message =
        ConsumersFactory.consumer_message(
          group_id: group_id,
          message_kind: consumer.message_kind,
          consumer_id: consumer.id
        )

      # Put message in store (starts unpersisted)
      :ok = SlotMessageStore.put_messages(consumer, [message])

      # Deliver message
      {:ok, [delivered]} = SlotMessageStore.produce(consumer, 1, self())

      # Fail the message (this will persist it)
      meta = %{
        ack_id: delivered.ack_id,
        deliver_count: 1,
        last_delivered_at: DateTime.utc_now(),
        not_visible_until: DateTime.utc_now(),
        group_id: delivered.group_id
      }

      :ok = SlotMessageStore.messages_failed(consumer, [meta])

      # Verify message is persisted
      assert [_persisted] = Consumers.list_consumer_messages_for_consumer(consumer)

      # Redeliver and succeed the message
      {:ok, [redelivered]} = SlotMessageStore.produce(consumer, 1, self())
      {:ok, 1} = SlotMessageStore.messages_succeeded(consumer, [redelivered])

      # Verify message was deleted from postgres
      assert [] = Consumers.list_consumer_messages_for_consumer(consumer)
    end

    test "deletes failed then already_succeeded messages from postgres", %{consumer: consumer} do
      # Create message with a group_id
      group_id = "test-group"

      message =
        ConsumersFactory.consumer_message(
          group_id: group_id,
          message_kind: consumer.message_kind,
          consumer_id: consumer.id
        )

      # Put message in store (starts unpersisted)
      :ok = SlotMessageStore.put_messages(consumer, [message])

      # Deliver message
      {:ok, [delivered]} = SlotMessageStore.produce(consumer, 1, self())

      # Fail the message (this will persist it)
      meta = %{
        ack_id: delivered.ack_id,
        deliver_count: 1,
        last_delivered_at: DateTime.utc_now(),
        not_visible_until: DateTime.utc_now(),
        group_id: delivered.group_id
      }

      :ok = SlotMessageStore.messages_failed(consumer, [meta])

      # Verify message is persisted
      assert [_persisted] = Consumers.list_consumer_messages_for_consumer(consumer)

      # Redeliver and succeed the message
      {:ok, [redelivered]} = SlotMessageStore.produce(consumer, 1, self())
      {:ok, 1} = SlotMessageStore.messages_already_succeeded(consumer, [redelivered])

      # Verify message was deleted from postgres
      assert [] = Consumers.list_consumer_messages_for_consumer(consumer)
    end

    test "delivers and fails messages to disk", %{consumer: consumer} do
      messages = [
        ConsumersFactory.consumer_message(message_kind: consumer.message_kind, consumer_id: consumer.id),
        ConsumersFactory.consumer_message(message_kind: consumer.message_kind, consumer_id: consumer.id)
      ]

      # Put messages in store
      :ok = SlotMessageStore.put_messages(consumer, messages)

      # Produce messages
      {:ok, delivered} = SlotMessageStore.produce(consumer, 2, self())
      assert length(delivered) == 2

      # Fail the messages with updated metadata
      now = DateTime.utc_now()
      not_visible_until = DateTime.add(now, 60)

      metas =
        Enum.map(delivered, fn msg ->
          %{
            ack_id: msg.ack_id,
            deliver_count: 1,
            last_delivered_at: now,
            not_visible_until: not_visible_until,
            group_id: msg.group_id
          }
        end)

      :ok = SlotMessageStore.messages_failed(consumer, metas)

      # Verify messages were persisted with updated metadata
      messages = SlotMessageStore.peek_messages(consumer, 2)
      assert length(messages) == 2
      assert Enum.all?(messages, fn msg -> msg.deliver_count == 1 end)
      assert Enum.all?(messages, fn msg -> msg.last_delivered_at == now end)
      assert Enum.all?(messages, fn msg -> msg.not_visible_until == not_visible_until end)
    end

    test "if the pid changes between calls of produce_messages, produced_messages are available for deliver", %{
      consumer: consumer
    } do
      messages = [
        ConsumersFactory.consumer_message(),
        ConsumersFactory.consumer_message()
      ]

      # Put messages in store
      :ok = SlotMessageStore.put_messages(consumer, messages)

      # Produce messages, none should be delivered
      {:ok, delivered} = SlotMessageStore.produce(consumer, 2, self())
      assert length(delivered) == 2
      {:ok, []} = SlotMessageStore.produce(consumer, 2, self())

      {:ok, delivered} = SlotMessageStore.produce(consumer, 2, Factory.pid())
      assert length(delivered) == 2
    end

    test "messages with nil group_ids are not blocked when other nil group_id messages are persisted", %{
      # Only event_consumer has nil group_ids
      event_consumer: consumer
    } do
      # Create a message with nil group_id that will be persisted
      message1 =
        ConsumersFactory.consumer_message(
          group_id: nil,
          message_kind: consumer.message_kind,
          consumer_id: consumer.id,
          commit_lsn: 1
        )

      # Create another message with nil group_id that should not be blocked
      message2 =
        ConsumersFactory.consumer_message(
          group_id: nil,
          message_kind: consumer.message_kind,
          consumer_id: consumer.id,
          commit_lsn: 2
        )

      # Put first message and fail it to persist it
      :ok = SlotMessageStore.put_messages(consumer, [message1])
      {:ok, [delivered]} = SlotMessageStore.produce(consumer, 1, self())

      meta = %{
        ack_id: delivered.ack_id,
        deliver_count: 1,
        group_id: nil,
        last_delivered_at: DateTime.utc_now(),
        not_visible_until: DateTime.add(DateTime.utc_now(), 60)
      }

      :ok = SlotMessageStore.messages_failed(consumer, [meta])

      # Put second message - it should not be blocked or persisted
      :ok = SlotMessageStore.put_messages(consumer, [message2])
      [persisted_msg] = Consumers.list_consumer_messages_for_consumer(consumer)
      assert persisted_msg.commit_lsn == message1.commit_lsn
      {:ok, [message]} = SlotMessageStore.produce(consumer, 1, self())
      assert message.commit_lsn == message2.commit_lsn
    end

    test "message_group_persisted? returns false for nil group_ids", %{event_consumer: consumer} do
      # Create and persist a message with nil group_id
      message =
        ConsumersFactory.consumer_message(
          group_id: nil,
          message_kind: consumer.message_kind,
          consumer_id: consumer.id
        )

      :ok = SlotMessageStore.put_messages(consumer, [message])
      {:ok, [delivered]} = SlotMessageStore.produce(consumer, 1, self())

      meta = %{
        ack_id: delivered.ack_id,
        deliver_count: 1,
        group_id: nil,
        last_delivered_at: DateTime.utc_now(),
        not_visible_until: DateTime.add(DateTime.utc_now(), 60)
      }

      :ok = SlotMessageStore.messages_failed(consumer, [meta])

      # Peek at state to verify message_group_persisted? returns false
      Enum.each(SlotMessageStore.peek(consumer), fn state ->
        refute State.message_group_persisted?(state, message.table_oid, nil)
      end)
    end

    test "pop_blocked_messages does not block messages with nil group_ids", %{event_consumer: consumer} do
      # Create messages - one with group_id and one without
      message1 =
        ConsumersFactory.consumer_message(
          group_id: "test-group",
          message_kind: consumer.message_kind,
          consumer_id: consumer.id
        )

      message2 =
        ConsumersFactory.consumer_message(
          group_id: nil,
          message_kind: consumer.message_kind,
          consumer_id: consumer.id
        )

      # Put and fail first message to persist it
      :ok = SlotMessageStore.put_messages(consumer, [message1])
      {:ok, [delivered]} = SlotMessageStore.produce(consumer, 1, self())

      meta = %{
        ack_id: delivered.ack_id,
        deliver_count: 1,
        group_id: delivered.group_id,
        last_delivered_at: DateTime.utc_now(),
        not_visible_until: DateTime.add(DateTime.utc_now(), 60)
      }

      :ok = SlotMessageStore.messages_failed(consumer, [meta])

      # Put second message (nil group_id)
      :ok = SlotMessageStore.put_messages(consumer, [message2])
    end

    test "duplicate messages don't accumulate payload size", %{consumer: consumer} do
      # Create a message
      message =
        ConsumersFactory.consumer_message(
          message_kind: consumer.message_kind,
          consumer_id: consumer.id,
          commit_lsn: 1,
          commit_idx: 1
        )

      # Put the same message multiple times
      :ok = SlotMessageStore.put_messages(consumer, [message])
      :ok = SlotMessageStore.put_messages(consumer, [message])
      :ok = SlotMessageStore.put_messages(consumer, [message])

      # Verify we can only produce it once
      {:ok, [delivered]} = SlotMessageStore.produce(consumer, 2, self())
      assert length([delivered]) == 1
      assert delivered.commit_lsn == 1
      assert delivered.commit_idx == 1

      # Ack the message
      {:ok, 1} = SlotMessageStore.messages_succeeded(consumer, [delivered])

      # Verify no bytes are accumulated
      Enum.each(SlotMessageStore.peek(consumer), fn state ->
        assert state.payload_size_bytes == 0
      end)
    end

    test "messages_succeeded_returning_messages returns the acked messages", %{consumer: consumer} do
      # Create test messages
      messages = [
        ConsumersFactory.consumer_message(),
        ConsumersFactory.consumer_message()
      ]

      # Put messages in store
      :ok = SlotMessageStore.put_messages(consumer, messages)

      # Retrieve messages
      {:ok, delivered} = SlotMessageStore.produce(consumer, 2, self())
      assert length(delivered) == 2

      # Get ack_ids and call messages_succeeded_returning_messages
      ack_ids = Enum.map(delivered, & &1.ack_id)
      {:ok, returned_messages} = SlotMessageStore.messages_succeeded_returning_messages(consumer, ack_ids)

      # Verify returned messages match what was delivered
      assert length(returned_messages) == 2
      assert_lists_equal(Enum.map(returned_messages, & &1.ack_id), ack_ids)

      # Verify messages were removed from store
      {:ok, []} = SlotMessageStore.produce(consumer, 2, self())
    end

    test "produces messages with same group_id from different tables simultaneously", %{} do
      consumer =
        ConsumersFactory.insert_sink_consumer!(message_kind: :event)

      start_supervised!({SlotMessageStoreSupervisor, consumer_id: consumer.id, test_pid: self()})

      # Create two messages from different tables with the same group_id
      shared_group_id = "shared-group-123"
      table_oid_1 = 100
      table_oid_2 = 200

      message1 =
        ConsumersFactory.consumer_message(
          message_kind: :event,
          consumer_id: consumer.id,
          group_id: shared_group_id,
          table_oid: table_oid_1,
          commit_lsn: 1,
          commit_idx: 1
        )

      message2 =
        ConsumersFactory.consumer_message(
          message_kind: :event,
          consumer_id: consumer.id,
          group_id: shared_group_id,
          table_oid: table_oid_2,
          commit_lsn: 2,
          commit_idx: 1
        )

      # Put both messages in the store
      :ok = SlotMessageStore.put_messages(consumer, [message1, message2])

      # Both messages should be available for delivery simultaneously
      # This tests that messages with the same group_id from different tables don't block each other
      {:ok, delivered} = SlotMessageStore.produce(consumer, 2, self())

      assert length(delivered) == 2

      delivered_table_oids = delivered |> Enum.map(& &1.table_oid) |> Enum.sort()
      assert delivered_table_oids == [table_oid_1, table_oid_2]

      # Both should have the same group_id
      assert Enum.all?(delivered, fn msg -> msg.group_id == shared_group_id end)

      # Verify they can be acknowledged
      {:ok, 2} = SlotMessageStore.messages_succeeded(consumer, delivered)

      # No more messages should be available
      {:ok, []} = SlotMessageStore.produce(consumer, 2, self())
    end
  end

  describe "SlotMessageStore table reader batch handling" do
    setup do
      consumer = ConsumersFactory.insert_sink_consumer!()

      start_supervised!({SlotMessageStoreSupervisor, consumer_id: consumer.id, test_pid: self()})

      %{consumer: consumer}
    end

    test "puts batch and reports on batch progress", %{consumer: consumer} do
      consumer_id = consumer.id
      :syn.join(:consumers, {:table_reader_batches_changed, consumer_id}, self())

      # Create test events
      messages = [
        ConsumersFactory.consumer_message(),
        ConsumersFactory.consumer_message()
      ]

      # Put messages in store
      :ok = SlotMessageStore.put_table_reader_batch(consumer, messages, "test-batch-id")

      # Retrieve messages
      {:ok, [delivered]} = SlotMessageStore.produce(consumer, 1, self())

      assert {:ok, ["test-batch-id"]} == SlotMessageStore.unpersisted_table_reader_batch_ids(consumer)

      # For acks
      {:ok, 1} = SlotMessageStore.messages_succeeded(consumer, [delivered])

      # Produce messages, none should be delivered
      {:ok, [delivered]} = SlotMessageStore.produce(consumer, 1, self())

      # Delivered messages don't "complete" a batch
      assert {:ok, ["test-batch-id"]} == SlotMessageStore.unpersisted_table_reader_batch_ids(consumer)

      {:ok, 1} = SlotMessageStore.messages_succeeded(consumer, [delivered])

      assert_received :table_reader_batches_changed

      assert {:ok, []} == SlotMessageStore.unpersisted_table_reader_batch_ids(consumer)
    end
  end

  describe "SlotMessageStore wal cursors" do
    setup do
      consumer = ConsumersFactory.insert_sink_consumer!()

      start_supervised!({SlotMessageStoreSupervisor, consumer_id: consumer.id, test_pid: self()})

      %{consumer: consumer}
    end

    test "min_wal_cursor raises on ref mismatch", %{consumer: consumer} do
      ref = make_ref()
      :ok = SlotMessageStore.set_monitor_ref(consumer, ref)

      assert SlotMessageStore.min_unpersisted_wal_cursors(consumer, ref) == []

      assert_raise(InvariantError, fn ->
        SlotMessageStore.min_unpersisted_wal_cursors(consumer, make_ref())
      end)
    end
  end

  describe "SlotMessageStore flush behavior" do
    setup do
      consumer = ConsumersFactory.insert_sink_consumer!()

      start_supervised!(
        {SlotMessageStoreSupervisor,
         consumer_id: consumer.id, test_pid: self(), flush_interval: 100, message_age_before_flush_ms: 100}
      )

      %{consumer: consumer}
    end

    test "identifies messages for flushing after age threshold", %{consumer: consumer} do
      consumer_id = consumer.id

      ref = make_ref()
      :ok = SlotMessageStore.set_monitor_ref(consumer, ref)

      # Create a message that will be old enough to flush
      message =
        ConsumersFactory.consumer_message(
          message_kind: consumer.message_kind,
          consumer_id: consumer.id
        )

      # Put message in store
      :ok = SlotMessageStore.put_messages(consumer, [message])

      # Initially we have one unpersisted commit tuple
      assert SlotMessageStore.min_unpersisted_wal_cursors(consumer, ref) == [
               %{commit_lsn: message.commit_lsn, commit_idx: message.commit_idx}
             ]

      # Verify we receive flush_messages_done
      assert_receive {:flush_messages_done, ^consumer_id}, 1000

      persisted_messages = Consumers.list_consumer_messages_for_consumer(consumer)
      assert length(persisted_messages) == 1

      # This is the point- the wal cursor is now persisted!
      assert SlotMessageStore.min_unpersisted_wal_cursors(consumer, ref) == []
    end

    test "persisted messages are not identified for flushing regardless of age", %{consumer: consumer} do
      # Create a message that will be persisted
      message =
        ConsumersFactory.consumer_message(
          message_kind: consumer.message_kind,
          consumer_id: consumer.id
        )

      # Put message and fail it to persist it
      :ok = SlotMessageStore.put_messages(consumer, [message])
      {:ok, [delivered]} = SlotMessageStore.produce(consumer, 1, self())

      meta = %{
        ack_id: delivered.ack_id,
        deliver_count: 1,
        group_id: delivered.group_id,
        last_delivered_at: DateTime.utc_now(),
        not_visible_until: DateTime.add(DateTime.utc_now(), 60)
      }

      :ok = SlotMessageStore.messages_failed(consumer, [meta])

      refute_receive {:flush_messages_done, _}, 100
    end
  end

  describe "SlotMessageStore flush batching behavior" do
    setup do
      consumer = ConsumersFactory.insert_sink_consumer!(partition_count: 1)

      sup =
        start_supervised!(
          {SlotMessageStoreSupervisor,
           consumer_id: consumer.id, test_pid: self(), flush_interval: 999_999_999, message_age_before_flush_ms: 0}
        )

      [pid] = for {_name, pid, _, _} <- Supervisor.which_children(sup), do: pid

      %{consumer: consumer, slot_message_store_pid: pid}
    end

    test "many messages", %{consumer: consumer, slot_message_store_pid: pid} do
      ref = make_ref()
      :ok = SlotMessageStore.set_monitor_ref(consumer, ref)

      assert 8 == Application.get_env(:sequin, :slot_message_store, [])[:flush_batch_size]

      # Create a message that will be old enough to flush
      messages =
        for _ <- 1..(3 * 8) do
          ConsumersFactory.consumer_message(
            message_kind: consumer.message_kind,
            consumer_id: consumer.id
          )
        end

      # Put message in store
      :ok = SlotMessageStore.put_messages(consumer, messages)

      # Initially we have one unpersisted commit tuple
      assert [_] =
               SlotMessageStore.min_unpersisted_wal_cursors(consumer, ref)

      send(pid, :flush_messages)

      assert_receive {:flush_messages_count, 8}, 333
      assert_receive {:flush_messages_count, 8}, 333
      assert_receive {:flush_messages_count, 8}, 333

      # done
      assert [] = SlotMessageStore.min_unpersisted_wal_cursors(consumer, ref)
      # no more
      refute_receive {:flush_messages_count, _}, 111
    end
  end

  describe "SlotMessageStore visibility check behavior" do
    setup do
      consumer = ConsumersFactory.insert_sink_consumer!()

      start_supervised!(
        {SlotMessageStoreSupervisor,
         consumer_id: consumer.id, test_pid: self(), visibility_check_interval: 100, max_time_since_delivered_ms: 500}
      )

      %{consumer: consumer}
    end

    test "makes messages visible after being stuck in produced_message_groups", %{consumer: consumer} do
      consumer_id = consumer.id

      # Create a message
      message =
        ConsumersFactory.consumer_message(
          message_kind: consumer.message_kind,
          consumer_id: consumer.id
        )

      # Put message in store and produce it
      :ok = SlotMessageStore.put_messages(consumer, [message])
      {:ok, [delivered]} = SlotMessageStore.produce(consumer, 1, self())

      # Wait for visibility check
      assert_receive {:visibility_check_done, ^consumer_id}, 1000

      # Message should now be visible and available for delivery again
      {:ok, [redelivered]} = SlotMessageStore.produce(consumer, 1, self())
      assert redelivered.ack_id == delivered.ack_id
    end

    test "does not make messages visible before they are stuck", %{consumer: consumer} do
      # Create a message
      message =
        ConsumersFactory.consumer_message(
          message_kind: consumer.message_kind,
          consumer_id: consumer.id
        )

      # Put message in store and produce it
      :ok = SlotMessageStore.put_messages(consumer, [message])
      {:ok, [_delivered]} = SlotMessageStore.produce(consumer, 1, self())

      # Wait for visibility check
      refute_receive {:visibility_check_done, _}, 100

      # Message should still not be visible as it hasn't been stuck long enough
      {:ok, []} = SlotMessageStore.produce(consumer, 1, self())
    end
  end

  describe "SlotMessageStore max_retry_count behavior" do
    setup do
      consumer = ConsumersFactory.insert_sink_consumer!(max_retry_count: 2, type: :http_push, partition_count: 3)

      start_supervised!(
        {SlotMessageStoreSupervisor,
         consumer_id: consumer.id, test_pid: self(), visibility_check_interval: 100, max_time_since_delivered_ms: 500}
      )

      %{consumer: consumer}
    end

    test "discards messages that exceed max_retry_count", %{consumer: consumer} do
      consumer_id = consumer.id

      # Create a message with a specific group_id to ensure consistent partitioning
      message =
        ConsumersFactory.consumer_message(
          message_kind: consumer.message_kind,
          consumer_id: consumer.id,
          group_id: "test-group"
        )

      # Put message in store
      :ok = SlotMessageStore.put_messages(consumer, [message])
      assert_receive {:put_messages_done, ^consumer_id}, 1000

      # First delivery and failure
      {:ok, [delivered]} = SlotMessageStore.produce(consumer, 1, self())

      meta = %{
        ack_id: delivered.ack_id,
        deliver_count: 1,
        group_id: delivered.group_id,
        last_delivered_at: DateTime.utc_now(),
        not_visible_until: DateTime.utc_now()
      }

      :ok = SlotMessageStore.messages_failed(consumer, [meta])

      # Second delivery and failure should discard the message
      {:ok, [redelivered]} = SlotMessageStore.produce(consumer, 1, self())

      meta = %{
        ack_id: redelivered.ack_id,
        deliver_count: 2,
        group_id: redelivered.group_id,
        last_delivered_at: DateTime.utc_now(),
        not_visible_until: DateTime.utc_now()
      }

      :ok = SlotMessageStore.messages_failed(consumer, [meta])

      # Verify message is discarded
      assert [] == Consumers.list_consumer_messages_for_consumer(consumer)
      {:ok, acknowledged} = AcknowledgedMessages.fetch_messages(consumer.id, 100, 0)
      assert length(acknowledged) == 1
      assert hd(acknowledged).state == "discarded"
    end

    test "keeps messages when max_retry_count is nil", %{consumer: _consumer} do
      # Create a consumer with nil max_retry_count
      consumer = ConsumersFactory.insert_sink_consumer!(max_retry_count: nil)
      start_supervised!({SlotMessageStoreSupervisor, consumer_id: consumer.id, test_pid: self()})

      # Create a message
      message =
        ConsumersFactory.consumer_message(
          message_kind: consumer.message_kind,
          consumer_id: consumer.id
        )

      # Put message in store
      :ok = SlotMessageStore.put_messages(consumer, [message])

      # Deliver and fail the message multiple times
      for i <- 1..5 do
        {:ok, [delivered]} = SlotMessageStore.produce(consumer, 1, self())

        meta = %{
          ack_id: delivered.ack_id,
          deliver_count: i,
          group_id: delivered.group_id,
          last_delivered_at: DateTime.utc_now(),
          not_visible_until: DateTime.utc_now()
        }

        :ok = SlotMessageStore.messages_failed(consumer, [meta])
      end

      # Message should still be in the store
      persisted_messages = Consumers.list_consumer_messages_for_consumer(consumer)
      assert length(persisted_messages) == 1
      {:ok, acknowledged} = AcknowledgedMessages.fetch_messages(consumer.id, 100, 0)
      assert length(acknowledged) == 0
    end
  end

  describe "SlotMessageStore put_messages that exceed max_memory_bytes" do
    test "returns error when load_shedding_policy=pause_on_full" do
      consumer =
        ConsumersFactory.insert_sink_consumer!(
          load_shedding_policy: :pause_on_full,
          max_memory_mb: 128
        )

      start_supervised!({SlotMessageStoreSupervisor, consumer_id: consumer.id, test_pid: self()})

      # Create a message with a specific group_id to ensure consistent partitioning
      message =
        ConsumersFactory.consumer_message(
          message_kind: consumer.message_kind,
          consumer_id: consumer.id,
          group_id: "test-group",
          payload_size_bytes: 130 * 1000 * 1000
        )

      # Put message in store
      assert {:error, %Error.InvariantError{}} = SlotMessageStore.put_messages(consumer, [message])
    end

    test "returns ok when load_shedding_policy=discard_on_full" do
      consumer = ConsumersFactory.insert_sink_consumer!(load_shedding_policy: :discard_on_full)

      start_supervised!(
        {SlotMessageStoreSupervisor, consumer_id: consumer.id, test_pid: self(), setting_system_max_memory_bytes: 1}
      )

      # Create a message with a specific group_id to ensure consistent partitioning
      message =
        ConsumersFactory.consumer_message(
          message_kind: consumer.message_kind,
          consumer_id: consumer.id,
          group_id: "test-group"
        )

      # Put message in store
      assert :ok = SlotMessageStore.put_messages(consumer, [message])
    end
  end
end

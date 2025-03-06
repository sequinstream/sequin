defmodule Sequin.SlotMessageHandlerTest do
  use Sequin.DataCase, async: true

  alias Sequin.Error
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Runtime.MessageHandler
  alias Sequin.Runtime.MessageHandlerMock
  alias Sequin.Runtime.SlotMessageHandler

  setup do
    # Set up test data
    account = AccountsFactory.insert_account!()
    database = DatabasesFactory.insert_postgres_database!(account_id: account.id)

    slot =
      ReplicationFactory.insert_postgres_replication!(
        account_id: account.id,
        postgres_database_id: database.id,
        partition_count: 3
      )

    # Create a context for the message handler
    context = MessageHandler.context(slot)

    start_supervised_handlers!(slot)

    %{
      account: account,
      database: database,
      slot: slot,
      context: context
    }
  end

  describe "handle_messages/2 with multiple partitions" do
    test "fans out messages to the correct partitions", %{slot: slot, context: context} do
      # Create messages with different table_oid and ids
      message1 = ReplicationFactory.postgres_message(table_oid: 100, ids: [1001])
      message2 = ReplicationFactory.postgres_message(table_oid: 200, ids: [2001])
      message3 = ReplicationFactory.postgres_message(table_oid: 300, ids: [3001])

      messages = [message1, message2, message3]

      # Calculate expected partition for each message
      expected_partitions = %{
        message1 => SlotMessageHandler.message_partition_idx(message1, slot.partition_count),
        message2 => SlotMessageHandler.message_partition_idx(message2, slot.partition_count),
        message3 => SlotMessageHandler.message_partition_idx(message3, slot.partition_count)
      }

      # Group messages by their expected partition
      messages_by_partition =
        Enum.group_by(messages, fn message ->
          expected_partitions[message]
        end)

      # Set up expectations for each partition
      Enum.each(messages_by_partition, fn {_partition_idx, partition_messages} ->
        expect(MessageHandlerMock, :handle_messages, fn _ctx, msgs ->
          assert_lists_equal(msgs, partition_messages)
          {:ok, length(msgs)}
        end)

        # Verify that the messages received match the expected ones for this partition
      end)

      # Call the function under test
      assert :ok = SlotMessageHandler.handle_messages(context, messages)
      assert :ok = SlotMessageHandler.flush_messages(context)
    end

    test "consistently routes messages with the same table_oid and ids to the same partition", %{
      context: context
    } do
      # Create multiple messages with the same table_oid and ids but different data
      base_message = ReplicationFactory.postgres_message(table_oid: 500, ids: [5001])

      # Create variations of the same message with different fields but same table_oid and ids
      field1 = ReplicationFactory.field(column_name: "field1", value: "value1")
      field2 = ReplicationFactory.field(column_name: "field2", value: "value2")
      field3 = ReplicationFactory.field(column_name: "field3", value: "value3")

      message1 = %{base_message | fields: [field1]}
      message2 = %{base_message | fields: [field2]}
      message3 = %{base_message | fields: [field3]}

      messages = [message1, message2, message3]

      # Set up expectation for the expected partition
      expect(MessageHandlerMock, :handle_messages, fn _ctx, msgs ->
        assert length(msgs) == 3
        assert Enum.all?(msgs, fn msg -> msg.table_oid == base_message.table_oid && msg.ids == base_message.ids end)
        {:ok, length(msgs)}
      end)

      # Verify that all three messages were sent to this partition

      # Call the function under test
      assert :ok = SlotMessageHandler.handle_messages(context, messages)
      assert :ok = SlotMessageHandler.flush_messages(context)
    end
  end

  describe "handle_messages/2 with single partition" do
    setup %{account: account, database: database} do
      slot =
        ReplicationFactory.insert_postgres_replication!(
          account_id: account.id,
          postgres_database_id: database.id,
          partition_count: 1
        )

      # Create a context for the message handler
      context = MessageHandler.context(slot)

      start_supervised_handlers!(slot)

      %{slot: slot, context: context}
    end

    test "processes messages correctly with a single partition", %{context: context} do
      # Create multiple messages with different table_oid and ids
      message1 = ReplicationFactory.postgres_message(table_oid: 100, ids: [1001])
      message2 = ReplicationFactory.postgres_message(table_oid: 200, ids: [2001])
      message3 = ReplicationFactory.postgres_message(table_oid: 300, ids: [3001])

      messages = [message1, message2, message3]

      # With a single partition, all messages should go to partition 0
      expect(MessageHandlerMock, :handle_messages, fn _ctx, msgs ->
        # Verify that all messages were sent to the single partition
        assert length(msgs) == 3
        assert msgs == messages
        {:ok, length(msgs)}
      end)

      # Call the function under test
      assert :ok = SlotMessageHandler.handle_messages(context, messages)
      assert :ok = SlotMessageHandler.flush_messages(context)
    end
  end

  describe "reload_entities/1" do
    test "reloads entities for all partitions", %{slot: slot, context: context} do
      # First, modify an entity that's passed into context - we'll insert a new SinkConsumer
      new_consumer = ConsumersFactory.insert_sink_consumer!(replication_slot_id: slot.id, account_id: slot.account_id)

      # Call reload_entities
      assert :ok = SlotMessageHandler.reload_entities(context)

      # Now create a message and handle it
      message = ReplicationFactory.postgres_message()

      # Set up expectation for the message handler
      # The context should contain the updated slot name
      test_pid = self()

      expect(MessageHandlerMock, :handle_messages, fn ctx, _msgs ->
        assert Enum.find(ctx.consumers, fn consumer -> consumer.id == new_consumer.id end)
        send(test_pid, :handled_message)
        {:ok, 1}
      end)

      # Handle the message and flush
      assert :ok = SlotMessageHandler.handle_messages(context, [message])
      assert :ok = SlotMessageHandler.flush_messages(context)
      assert_received :handled_message
    end
  end

  describe "handle_call/3 for :handle_messages" do
    @tag capture_log: true
    # This test still logs a Postgrex.Protocol error, so we skip it
    test "raises payload_size_limit_exceeded error when message handler returns the error", %{
      context: context
    } do
      # Create a test message
      message = ReplicationFactory.postgres_message()

      # Mock the MessageHandler to return a payload_size_limit_exceeded error
      expect(MessageHandlerMock, :handle_messages, fn _ctx, _msgs ->
        {:error, %Error.InvariantError{code: :payload_size_limit_exceeded, message: "payload_size_limit_exceeded"}}
      end)

      # Send a :handle_messages call to the GenServer
      # This should raise an error, so we need to catch it

      try do
        SlotMessageHandler.handle_messages(context, [message])
        SlotMessageHandler.flush_messages(context)
        raise "should not get here, should have exited first"
      catch
        :exit, error ->
          assert match?({{%Sequin.Error.ServiceError{code: :payload_size_limit_exceeded}, _}, _}, error)
      end
    end
  end

  defp start_supervised_handlers!(slot) do
    for idx <- 0..(slot.partition_count - 1) do
      start_supervised!(
        SlotMessageHandler.child_spec(
          replication_slot_id: slot.id,
          processor_idx: idx,
          test_pid: self(),
          message_handler: MessageHandlerMock
        )
      )
    end
  end
end

defmodule Sequin.Functions.TestMessagesTest do
  use Sequin.Case, async: true

  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerEventData
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.ConsumerRecordData
  alias Sequin.Consumers.ConsumerRecordData.Metadata
  alias Sequin.Factory
  alias Sequin.Functions.TestMessages

  setup do
    TestMessages.create_ets_table()
    :ok
  end

  describe "initialization" do
    test "new/0 creates an empty ETS table" do
      database_id = Factory.uuid()
      table_oid = Factory.integer()
      assert TestMessages.get_test_messages(database_id, table_oid) == []
    end
  end

  describe "basic operations" do
    test "add_test_message/2 adds a message when under limit" do
      database_id = Factory.uuid()
      table_oid = Factory.integer()

      message = %ConsumerRecord{
        consumer_id: Ecto.UUID.generate(),
        commit_lsn: 1,
        commit_idx: 1,
        record_pks: ["1"],
        group_id: "group1",
        table_oid: 1,
        deliver_count: 0,
        replication_message_trace_id: Ecto.UUID.generate(),
        data: %ConsumerRecordData{
          metadata: %ConsumerRecordData.Metadata{
            table_name: "test_table",
            table_schema: "public"
          }
        }
      }

      assert TestMessages.add_test_message(database_id, table_oid, message)
      assert TestMessages.get_test_messages(database_id, table_oid) == [message]
    end

    test "add_test_message/2 adds a consumer event message" do
      database_id = Factory.uuid()
      table_oid = Factory.integer()

      message = %ConsumerEvent{
        consumer_id: Ecto.UUID.generate(),
        commit_lsn: 1,
        commit_idx: 1,
        record_pks: ["1"],
        group_id: "group1",
        table_oid: 1,
        deliver_count: 0,
        replication_message_trace_id: Ecto.UUID.generate(),
        data: %ConsumerEventData{
          metadata: %ConsumerEventData.Metadata{
            table_name: "test_table",
            table_schema: "public"
          }
        }
      }

      assert TestMessages.add_test_message(database_id, table_oid, message)
      assert TestMessages.get_test_messages(database_id, table_oid) == [message]
    end

    test "add_test_message/2 respects the 10 message limit" do
      database_id = Factory.uuid()
      table_oid = Factory.integer()

      messages =
        Enum.map(1..11, fn i ->
          %ConsumerRecord{
            consumer_id: Ecto.UUID.generate(),
            commit_lsn: i,
            commit_idx: i,
            record_pks: ["#{i}"],
            group_id: "group1",
            table_oid: 1,
            deliver_count: 0,
            replication_message_trace_id: Ecto.UUID.generate(),
            data: %ConsumerRecordData{
              metadata: %Metadata{
                table_name: "test_table",
                table_schema: "public"
              }
            }
          }
        end)

      # Add first 10 messages
      Enum.each(Enum.take(messages, 10), fn message ->
        assert TestMessages.add_test_message(database_id, table_oid, message)
      end)

      # Try to add the 11th message
      refute TestMessages.add_test_message(database_id, table_oid, Enum.at(messages, 10))

      # Verify only 10 messages are stored
      stored_messages = TestMessages.get_test_messages(database_id, table_oid)
      assert length(stored_messages) == 10
      assert stored_messages == Enum.take(messages, 10)
    end

    test "needs_test_messages?/1 returns false for empty sequence before test messages are requested" do
      database_id = Factory.uuid()
      refute TestMessages.needs_test_messages?(database_id)
    end

    test "needs_test_messages?/1 returns true for empty sequence after test messages are requested" do
      database_id = Factory.uuid()
      TestMessages.register_needs_messages(database_id)
      assert TestMessages.needs_test_messages?(database_id)
    end

    test "needs_test_messages?/1 returns true only while a registered process is alive" do
      test_pid = self()
      database_id = Factory.uuid()

      refute TestMessages.needs_test_messages?(database_id)

      Task.async(fn ->
        TestMessages.register_needs_messages(database_id)
        send(test_pid, :registered)

        Process.sleep(100)

        send(test_pid, :dying)
      end)

      assert_receive :registered
      assert TestMessages.needs_test_messages?(database_id)

      assert_receive :dying, 200
      Process.sleep(50)
      refute TestMessages.needs_test_messages?(database_id)
    end

    test "get_test_messages/1 returns empty list for non-existent sequence" do
      database_id = Factory.uuid()
      table_oid = Factory.integer()
      assert TestMessages.get_test_messages(database_id, table_oid) == []
    end

    test "get_test_messages/1 returns messages in order of insertion" do
      database_id = Factory.uuid()
      table_oid = Factory.integer()

      messages =
        Enum.map(1..3, fn i ->
          %ConsumerRecord{
            consumer_id: Ecto.UUID.generate(),
            commit_lsn: i,
            commit_idx: i,
            record_pks: ["#{i}"],
            group_id: "group1",
            table_oid: 1,
            deliver_count: 0,
            replication_message_trace_id: Ecto.UUID.generate(),
            data: %ConsumerRecordData{
              metadata: %Metadata{
                table_name: "test_table",
                table_schema: "public"
              }
            }
          }
        end)

      Enum.each(messages, fn message ->
        TestMessages.add_test_message(database_id, table_oid, message)
      end)

      assert TestMessages.get_test_messages(database_id, table_oid) == messages
    end

    test "delete message by replication_message_trace_id" do
      database_id = Factory.uuid()
      table_oid = Factory.integer()

      messages =
        Enum.map(1..3, fn i ->
          %ConsumerRecord{
            consumer_id: Ecto.UUID.generate(),
            commit_lsn: i,
            commit_idx: i,
            record_pks: ["#{i}"],
            group_id: "group1",
            table_oid: 1,
            deliver_count: 0,
            replication_message_trace_id: Ecto.UUID.generate(),
            data: %ConsumerRecordData{
              metadata: %Metadata{
                table_name: "test_table",
                table_schema: "public",
                idempotency_key: to_string(i)
              }
            }
          }
        end)

      Enum.each(messages, fn message ->
        TestMessages.add_test_message(database_id, table_oid, message)
      end)

      assert TestMessages.get_test_messages(database_id, table_oid) == messages

      assert TestMessages.delete_test_message(database_id, table_oid, Enum.at(messages, 0).replication_message_trace_id)
      assert TestMessages.get_test_messages(database_id, table_oid) == Enum.drop(messages, 1)

      # Deleting again returns false
      refute TestMessages.delete_test_message(database_id, table_oid, Enum.at(messages, 0).replication_message_trace_id)

      assert TestMessages.delete_test_message(database_id, table_oid, Enum.at(messages, 1).replication_message_trace_id)
      assert TestMessages.get_test_messages(database_id, table_oid) == Enum.drop(messages, 2)
    end
  end

  describe "cleanup" do
    test "delete_sequence/1 removes all messages for a sequence" do
      database_id = Factory.uuid()
      table_oid = Factory.integer()

      message = %ConsumerRecord{
        consumer_id: Ecto.UUID.generate(),
        commit_lsn: 1,
        commit_idx: 1,
        record_pks: ["1"],
        group_id: "group1",
        table_oid: 1,
        deliver_count: 0,
        replication_message_trace_id: Ecto.UUID.generate(),
        data: %ConsumerRecordData{
          metadata: %Metadata{
            table_name: "test_table",
            table_schema: "public"
          }
        }
      }

      TestMessages.add_test_message(database_id, table_oid, message)
      assert TestMessages.get_test_messages(database_id, table_oid) == [message]

      TestMessages.delete_test_messages(database_id, table_oid)
      assert TestMessages.get_test_messages(database_id, table_oid) == []
      refute TestMessages.needs_test_messages?(database_id)
    end

    test "destroy/0 removes the entire ETS table" do
      database_id = Factory.uuid()
      table_oid = Factory.integer()

      message = %ConsumerRecord{
        consumer_id: Ecto.UUID.generate(),
        commit_lsn: 1,
        commit_idx: 1,
        record_pks: ["1"],
        group_id: "group1",
        table_oid: 1,
        deliver_count: 0,
        replication_message_trace_id: Ecto.UUID.generate(),
        data: %ConsumerRecordData{
          metadata: %Metadata{
            table_name: "test_table",
            table_schema: "public"
          }
        }
      }

      TestMessages.add_test_message(database_id, table_oid, message)
      assert TestMessages.get_test_messages(database_id, table_oid) == [message]

      TestMessages.destroy()
      assert_raise ArgumentError, fn -> TestMessages.get_test_messages(database_id, table_oid) end
    end
  end
end

defmodule Sequin.Transforms.TestMessagesTest do
  use Sequin.Case, async: true

  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerEventData
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.ConsumerRecordData
  alias Sequin.Consumers.ConsumerRecordData.Metadata
  alias Sequin.Factory
  alias Sequin.Transforms.TestMessages

  setup do
    TestMessages.create_ets_table()
    :ok
  end

  describe "initialization" do
    test "new/0 creates an empty ETS table" do
      assert TestMessages.needs_test_messages?("sequence1")
      assert TestMessages.get_test_messages("sequence1") == []
    end
  end

  describe "basic operations" do
    test "add_test_message/2 adds a message when under limit" do
      sequence_id = Factory.uuid()

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

      assert TestMessages.add_test_message(sequence_id, message)
      assert TestMessages.get_test_messages(sequence_id) == [message]
    end

    test "add_test_message/2 adds a consumer event message" do
      sequence_id = Factory.uuid()

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

      assert TestMessages.add_test_message(sequence_id, message)
      assert TestMessages.get_test_messages(sequence_id) == [message]
    end

    test "add_test_message/2 respects the 10 message limit" do
      sequence_id = Factory.uuid()

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
        assert TestMessages.add_test_message(sequence_id, message)
      end)

      # Try to add the 11th message
      refute TestMessages.add_test_message(sequence_id, Enum.at(messages, 10))

      # Verify only 10 messages are stored
      stored_messages = TestMessages.get_test_messages(sequence_id)
      assert length(stored_messages) == 10
      assert stored_messages == Enum.reverse(Enum.take(messages, 10))
    end

    test "needs_test_messages?/1 returns true for empty sequence" do
      assert TestMessages.needs_test_messages?("sequence1")
    end

    test "needs_test_messages?/1 returns true when under limit" do
      sequence_id = Factory.uuid()

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

      TestMessages.add_test_message(sequence_id, message)
      assert TestMessages.needs_test_messages?(sequence_id)
    end

    test "needs_test_messages?/1 returns false when at limit" do
      sequence_id = Factory.uuid()

      messages =
        Enum.map(1..10, fn i ->
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
        TestMessages.add_test_message(sequence_id, message)
      end)

      refute TestMessages.needs_test_messages?(sequence_id)
    end

    test "get_test_messages/1 returns empty list for non-existent sequence" do
      assert TestMessages.get_test_messages("nonexistent") == []
    end

    test "get_test_messages/1 returns messages in reverse order of insertion" do
      sequence_id = Factory.uuid()

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
        TestMessages.add_test_message(sequence_id, message)
      end)

      assert TestMessages.get_test_messages(sequence_id) == Enum.reverse(messages)
    end
  end

  describe "cleanup" do
    test "delete_sequence/1 removes all messages for a sequence" do
      sequence_id = Factory.uuid()

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

      TestMessages.add_test_message(sequence_id, message)
      assert TestMessages.get_test_messages(sequence_id) == [message]

      TestMessages.delete_sequence(sequence_id)
      assert TestMessages.get_test_messages(sequence_id) == []
      assert TestMessages.needs_test_messages?(sequence_id)
    end

    test "destroy/0 removes the entire ETS table" do
      sequence_id = Factory.uuid()

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

      TestMessages.add_test_message(sequence_id, message)
      assert TestMessages.get_test_messages(sequence_id) == [message]

      TestMessages.destroy()
      assert_raise ArgumentError, fn -> TestMessages.get_test_messages(sequence_id) end
    end
  end
end

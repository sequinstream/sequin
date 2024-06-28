defmodule Sequin.StreamsTest do
  use Sequin.DataCase, async: true

  alias Sequin.Factory.StreamsFactory
  alias Sequin.Streams

  describe "upsert_messages/1" do
    test "inserts new messages" do
      stream = StreamsFactory.insert_stream!()
      messages = for _ <- 1..2, do: StreamsFactory.message_attrs(%{stream_id: stream.id, data_hash: nil})

      assert {:ok, 2} = Streams.upsert_messages(messages)

      inserted_messages = Repo.all(Streams.Message)
      assert length(inserted_messages) == 2

      assert_lists_equal(inserted_messages, messages, &assert_maps_equal(&1, &2, [:key, :stream_id, :data]))

      assert Enum.all?(inserted_messages, fn message ->
               is_binary(message.data_hash) and is_nil(message.seq)
             end)
    end

    test "updates existing message when data_hash changes" do
      msg = StreamsFactory.insert_message!()
      new_data = StreamsFactory.message_data()

      assert {:ok, 1} = Streams.upsert_messages([%{msg | data: new_data, data_hash: nil}])
      updated_msg = Streams.get_message!(msg.key, msg.stream_id)

      assert updated_msg.data == new_data
      assert is_binary(updated_msg.data_hash)
      refute updated_msg.data_hash == msg.data_hash
      assert is_nil(updated_msg.seq)
    end

    test "does not update existing message when data_hash is the same" do
      msg = StreamsFactory.insert_message!()

      assert {:ok, 0} = Streams.upsert_messages([msg])

      updated_msg = Streams.get_message!(msg.key, msg.stream_id)
      assert updated_msg.seq == msg.seq
      assert updated_msg.data == msg.data
      assert updated_msg.data_hash == msg.data_hash
      assert updated_msg.updated_at == msg.updated_at
    end

    test "does not affect messages with same key but different stream_id" do
      stream1 = StreamsFactory.insert_stream!()
      stream2 = StreamsFactory.insert_stream!()

      msg1 = StreamsFactory.insert_message!(%{stream_id: stream1.id, key: "same_key"})
      msg2_attrs = StreamsFactory.message_attrs(%{stream_id: stream2.id, key: "same_key"})

      assert {:ok, 1} = Streams.upsert_messages([msg2_attrs])

      unchanged_msg1 = Streams.get_message!(msg1.key, msg1.stream_id)
      assert unchanged_msg1.seq == msg1.seq
      assert unchanged_msg1.data == msg1.data

      new_msg2 = Streams.get_message!(msg2_attrs.key, msg2_attrs.stream_id)
      assert new_msg2.data == msg2_attrs.data
    end

    test "retries on untranslatable_character error" do
      stream = StreamsFactory.insert_stream!()
      msg_attrs = StreamsFactory.message_attrs(%{stream_id: stream.id, data: "data with null byte \u0000"})

      assert {:ok, 1} = Streams.upsert_messages([msg_attrs])

      inserted_msg = Streams.get_message!(msg_attrs.key, msg_attrs.stream_id)
      assert inserted_msg.data == "data with null byte "
    end
  end

  describe "next_for_consumer/2" do
    setup do
      stream = StreamsFactory.insert_stream!()

      consumer =
        StreamsFactory.insert_consumer!(%{stream_id: stream.id, account_id: stream.account_id, max_ack_pending: 1_000})

      {:ok, stream: stream, consumer: consumer}
    end

    test "returns nothing if outstanding messages is empty", %{consumer: consumer} do
      assert {:ok, []} = Streams.next_for_consumer(consumer)
    end

    test "delivers available outstanding messages", %{stream: stream, consumer: consumer} do
      message = StreamsFactory.insert_message!(%{stream_id: stream.id})
      ack_wait_ms = consumer.ack_wait_ms

      om =
        StreamsFactory.insert_outstanding_message!(%{
          consumer_id: consumer.id,
          message: message,
          state: :available,
          deliver_count: 0
        })

      assert {:ok, [delivered_message]} = Streams.next_for_consumer(consumer)
      # Add a buffer for the comparison
      not_visible_until = DateTime.add(DateTime.utc_now(), ack_wait_ms - 1000, :millisecond)
      assert delivered_message.ack_id == om.id
      assert delivered_message.key == message.key
      updated_om = Repo.reload(om)
      assert updated_om.state == :delivered
      assert DateTime.after?(updated_om.not_visible_until, not_visible_until)
      assert updated_om.deliver_count == 1
      assert updated_om.last_delivered_at
    end

    test "redelivers expired outstanding messages", %{stream: stream, consumer: consumer} do
      message = StreamsFactory.insert_message!(%{stream_id: stream.id})

      om =
        StreamsFactory.insert_outstanding_message!(%{
          consumer_id: consumer.id,
          message: message,
          state: :delivered,
          not_visible_until: DateTime.add(DateTime.utc_now(), -1, :second),
          deliver_count: 1,
          last_delivered_at: DateTime.add(DateTime.utc_now(), -30, :second)
        })

      assert {:ok, [redelivered_msg]} = Streams.next_for_consumer(consumer)
      assert redelivered_msg.key == message.key
      assert redelivered_msg.ack_id == om.id
      updated_om = Repo.reload(om)
      assert updated_om.state == :delivered
      assert DateTime.compare(updated_om.not_visible_until, om.not_visible_until) != :eq
      assert updated_om.deliver_count == 2
      assert DateTime.compare(updated_om.last_delivered_at, om.last_delivered_at) != :eq
    end

    test "does not redeliver unexpired outstanding messages", %{stream: stream, consumer: consumer} do
      message = StreamsFactory.insert_message!(%{stream_id: stream.id})

      StreamsFactory.insert_outstanding_message!(%{
        consumer_id: consumer.id,
        message: message,
        state: :delivered,
        not_visible_until: DateTime.add(DateTime.utc_now(), 30, :second)
      })

      assert {:ok, []} = Streams.next_for_consumer(consumer)
    end

    test "delivers only up to batch_size", %{stream: stream, consumer: consumer} do
      for _ <- 1..3 do
        message = StreamsFactory.insert_message!(%{stream_id: stream.id})

        StreamsFactory.insert_outstanding_message!(%{
          consumer_id: consumer.id,
          message: message,
          state: :available
        })
      end

      assert {:ok, delivered} = Streams.next_for_consumer(consumer, batch_size: 2)
      assert length(delivered) == 2
      assert length(Streams.list_outstanding_messages_for_consumer(consumer.id)) == 3
    end

    test "does not deliver outstanding messages for another consumer", %{stream: stream, consumer: consumer} do
      other_consumer = StreamsFactory.insert_consumer!(%{stream_id: stream.id, account_id: stream.account_id})
      message = StreamsFactory.insert_message!(%{stream_id: stream.id})

      StreamsFactory.insert_outstanding_message!(%{
        consumer_id: other_consumer.id,
        message: message,
        state: :available
      })

      assert {:ok, []} = Streams.next_for_consumer(consumer)
    end

    test "with a mix of available and unavailble messages, delivers only available outstanding messages", %{
      stream: stream,
      consumer: consumer
    } do
      # Available messages
      available =
        for _ <- 1..3 do
          msg = StreamsFactory.insert_message!(%{stream_id: stream.id})

          StreamsFactory.insert_outstanding_message!(%{
            consumer_id: consumer.id,
            message: msg,
            state: :available
          })

          msg
        end

      redeliver =
        for _ <- 1..3 do
          msg = StreamsFactory.insert_message!(%{stream_id: stream.id})

          StreamsFactory.insert_outstanding_message!(%{
            consumer_id: consumer.id,
            message: msg,
            state: :delivered,
            not_visible_until: DateTime.add(DateTime.utc_now(), -30, :second)
          })

          msg
        end

      # Not available message
      for _ <- 1..3 do
        msg = StreamsFactory.insert_message!(%{stream_id: stream.id})

        StreamsFactory.insert_outstanding_message!(%{
          consumer_id: consumer.id,
          message: msg,
          state: :delivered,
          not_visible_until: DateTime.add(DateTime.utc_now(), 30, :second)
        })

        msg
      end

      assert {:ok, messages} = Streams.next_for_consumer(consumer)
      assert length(messages) == length(available ++ redeliver)
      assert_lists_equal(messages, available ++ redeliver, &assert_maps_equal(&1, &2, [:key, :stream_id]))
    end

    test "delivers messages according to asc last_delivered_at nulls last", %{stream: stream, consumer: consumer} do
      now = DateTime.utc_now()

      # Oldest delivered message
      message1 = StreamsFactory.insert_message!(%{stream_id: stream.id})

      StreamsFactory.insert_outstanding_message!(%{
        consumer_id: consumer.id,
        message: message1,
        state: :delivered,
        last_delivered_at: DateTime.add(now, -2, :minute),
        not_visible_until: DateTime.add(now, -1, :minute)
      })

      # Second oldest delivered message
      message2 = StreamsFactory.insert_message!(%{stream_id: stream.id})

      StreamsFactory.insert_outstanding_message!(%{
        consumer_id: consumer.id,
        message: message2,
        state: :delivered,
        last_delivered_at: DateTime.add(now, -1, :minute),
        not_visible_until: DateTime.add(now, -1, :minute)
      })

      # Newest delivered message (should not be returned)
      message3 = StreamsFactory.insert_message!(%{stream_id: stream.id})

      StreamsFactory.insert_outstanding_message!(%{
        consumer_id: consumer.id,
        message: message3,
        state: :delivered,
        last_delivered_at: now,
        not_visible_until: DateTime.add(now, -1, :minute)
      })

      # Available message (null last_delivered_at)
      message4 = StreamsFactory.insert_message!(%{stream_id: stream.id})

      StreamsFactory.insert_outstanding_message!(%{
        consumer_id: consumer.id,
        message: message4,
        state: :available,
        last_delivered_at: nil
      })

      assert {:ok, delivered} = Streams.next_for_consumer(consumer, batch_size: 2)
      assert length(delivered) == 2
      delivered_message_keys = Enum.map(delivered, & &1.key)
      assert_lists_equal(delivered_message_keys, [message1.key, message2.key])
    end

    test "respect's a consumer's max_ack_pending", %{consumer: consumer} do
      max_ack_pending = 3
      consumer = %{consumer | max_ack_pending: max_ack_pending}

      om =
        StreamsFactory.insert_outstanding_message_with_message!(%{
          consumer_id: consumer.id,
          message_stream_id: consumer.stream_id,
          state: :available
        })

      # These messages can't be delivered
      for _ <- 1..2 do
        StreamsFactory.insert_outstanding_message_with_message!(%{
          consumer_id: consumer.id,
          message_stream_id: consumer.stream_id,
          state: :delivered,
          not_visible_until: DateTime.add(DateTime.utc_now(), 30, :second)
        })
      end

      # These messages *can* be delivered, but are outside max_ack_pending
      for _ <- 1..2 do
        StreamsFactory.insert_outstanding_message_with_message!(%{
          consumer_id: consumer.id,
          message_stream_id: consumer.stream_id,
          state: :available
        })
      end

      assert {:ok, delivered} = Streams.next_for_consumer(consumer)
      assert length(delivered) == 1
      delivered = List.last(delivered)
      assert delivered.key == om.message_key
      assert {:ok, []} = Streams.next_for_consumer(consumer)
    end
  end

  test "updating a message resets its seq to nil via a trigger" do
    msg = StreamsFactory.insert_message!()

    new_data = StreamsFactory.message_data()

    {1, _} =
      msg.key
      |> Streams.Message.where_key_and_stream_id(msg.stream_id)
      |> Repo.update_all(set: [data: new_data])

    updated_msg = Streams.get_message!(msg.key, msg.stream_id)
    assert is_nil(updated_msg.seq)
    assert updated_msg.data == new_data
  end
end

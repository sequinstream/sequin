defmodule Sequin.StreamsTest do
  use Sequin.DataCase, async: true

  alias Sequin.Factory
  alias Sequin.Factory.StreamsFactory
  alias Sequin.Streams
  alias Sequin.Streams.ConsumerBackfillWorker

  describe "upsert_messages/1" do
    test "inserts new messages" do
      stream = StreamsFactory.insert_stream!()
      messages = for _ <- 1..2, do: StreamsFactory.message_attrs(%{stream_id: stream.id, data_hash: nil})

      assert {:ok, 2} = Streams.upsert_messages(stream.id, messages)

      inserted_messages = Repo.all(Streams.Message)
      assert length(inserted_messages) == 2

      assert_lists_equal(inserted_messages, messages, &assert_maps_equal(&1, &2, [:key, :stream_id, :data]))

      assert Enum.all?(inserted_messages, fn message ->
               is_binary(message.data_hash)
             end)
    end

    test "updates existing message when data_hash changes" do
      stream = StreamsFactory.insert_stream!()
      msg = StreamsFactory.insert_message!(stream_id: stream.id)
      new_data = StreamsFactory.message_data()

      assert {:ok, 1} = Streams.upsert_messages(stream.id, [%{msg | data: new_data, data_hash: nil}])
      updated_msg = Streams.get_message_for_stream!(msg.stream_id, msg.key)

      assert updated_msg.data == new_data
      assert is_binary(updated_msg.data_hash)
      refute updated_msg.data_hash == msg.data_hash
    end

    test "does not update existing message when data_hash is the same" do
      stream = StreamsFactory.insert_stream!()
      msg = StreamsFactory.insert_message!(stream_id: stream.id)

      assert {:ok, 0} = Streams.upsert_messages(stream.id, [msg])

      updated_msg = Streams.get_message_for_stream!(msg.stream_id, msg.key)
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

      assert {:ok, 1} = Streams.upsert_messages(stream2.id, [msg2_attrs])

      unchanged_msg1 = Streams.get_message_for_stream!(msg1.stream_id, msg1.key)
      assert unchanged_msg1.seq == msg1.seq
      assert unchanged_msg1.data == msg1.data

      new_msg2 = Streams.get_message_for_stream!(msg2_attrs.stream_id, msg2_attrs.key)
      assert new_msg2.data == msg2_attrs.data
    end

    test "retries on untranslatable_character error" do
      stream = StreamsFactory.insert_stream!()
      msg_attrs = StreamsFactory.message_attrs(%{stream_id: stream.id, data: "data with null byte \u0000"})

      assert {:ok, 1} = Streams.upsert_messages(stream.id, [msg_attrs])

      inserted_msg = Streams.get_message_for_stream!(msg_attrs.stream_id, msg_attrs.key)
      assert inserted_msg.data == "data with null byte "
    end
  end

  describe "create_consumer_for_account_with_lifecycle/2" do
    setup do
      account = Factory.AccountsFactory.insert_account!()
      stream = Factory.StreamsFactory.insert_stream!(account_id: account.id)
      {:ok, stream: stream, account: account}
    end

    test "creates a consumer and backfills few messages synchronously", %{stream: stream, account: account} do
      attrs =
        StreamsFactory.consumer_attrs(%{
          stream_id: stream.id,
          kind: :pull,
          filter_key_pattern: "test.key.>",
          backfill_completed_at: nil
        })

      for i <- 1..3 do
        # 3 messages that match the filter
        StreamsFactory.insert_message!(%{stream_id: stream.id, key: "test.key.#{i}"})

        # and 3 that do not match the filter
        StreamsFactory.insert_message!(%{stream_id: stream.id, key: "test.other_key.#{i}"})
      end

      assert {:ok, consumer} = Streams.create_consumer_for_account_with_lifecycle(account.id, attrs)

      # We do enqueue a job even if we have synchonously backfilled
      # This is to patch a race and to delete acked messages later
      assert_enqueued(worker: ConsumerBackfillWorker)

      assert messages = Streams.list_consumer_messages_for_consumer(stream.id, consumer.id)
      assert length(messages) == 3

      assert consumer.backfill_completed_at
    end
  end

  describe "upsert_messages/2 with consumer message fan out" do
    setup do
      stream = StreamsFactory.insert_stream!()
      {:ok, stream: stream, account_id: stream.account_id}
    end

    test "also upserts to a matching consumer", %{stream: stream, account_id: account_id} do
      consumer =
        StreamsFactory.insert_consumer!(%{
          stream_id: stream.id,
          filter_key_pattern: "test.key",
          account_id: account_id,
          backfill_completed_at: DateTime.utc_now()
        })

      message = StreamsFactory.message_attrs(%{stream_id: stream.id, key: "test.key"})

      assert {:ok, 1} = Streams.upsert_messages(stream.id, [message])

      message = Streams.get_message_for_stream!(message.stream_id, message.key)

      assert [consumer_message] = Streams.list_consumer_messages_for_consumer(consumer.stream_id, consumer.id)
      assert consumer_message.message_key == message.key
      assert consumer_message.ack_id
      assert consumer_message.message_seq == message.seq
      assert consumer_message.state == :available
      assert consumer_message.deliver_count == 0
      refute consumer_message.last_delivered_at
      refute consumer_message.not_visible_until
    end

    test "fans out to multiple consumers", %{stream: stream, account_id: account_id} do
      consumer1 =
        StreamsFactory.insert_consumer!(%{
          stream_id: stream.id,
          filter_key_pattern: "test.key",
          account_id: account_id,
          backfill_completed_at: DateTime.utc_now()
        })

      consumer2 =
        StreamsFactory.insert_consumer!(%{
          stream_id: stream.id,
          filter_key_pattern: "test.key",
          account_id: account_id,
          backfill_completed_at: DateTime.utc_now()
        })

      message = StreamsFactory.message_attrs(%{stream_id: stream.id, key: "test.key"})

      assert {:ok, 1} = Streams.upsert_messages(stream.id, [message])

      assert [_] = Streams.list_consumer_messages_for_consumer(consumer1.stream_id, consumer1.id)
      assert [_] = Streams.list_consumer_messages_for_consumer(consumer2.stream_id, consumer2.id)
    end

    test "does not fan out to a consumer in another stream even with matching keys", %{
      stream: stream,
      account_id: account_id
    } do
      other_stream = StreamsFactory.insert_stream!(account_id: account_id)

      consumer =
        StreamsFactory.insert_consumer!(%{
          stream_id: other_stream.id,
          filter_key_pattern: "test.key",
          account_id: account_id,
          backfill_completed_at: DateTime.utc_now()
        })

      message = StreamsFactory.message_attrs(%{stream_id: stream.id, key: "test.key"})

      assert {:ok, 1} = Streams.upsert_messages(stream.id, [message])

      assert [] = Streams.list_consumer_messages_for_consumer(consumer.stream_id, consumer.id)
    end

    test "does not fan out to a consumer in the same stream but without a matching key", %{
      stream: stream,
      account_id: account_id
    } do
      consumer =
        StreamsFactory.insert_consumer!(%{
          stream_id: stream.id,
          filter_key_pattern: "other.key",
          account_id: account_id,
          backfill_completed_at: DateTime.utc_now()
        })

      message = StreamsFactory.message_attrs(%{stream_id: stream.id, key: "test.key"})

      assert {:ok, 1} = Streams.upsert_messages(stream.id, [message])

      assert [] = Streams.list_consumer_messages_for_consumer(consumer.stream_id, consumer.id)
    end

    test "upserts over consumer_messages for a :delivered message", %{stream: stream, account_id: account_id} do
      consumer =
        StreamsFactory.insert_consumer!(%{
          stream_id: stream.id,
          filter_key_pattern: "test.key",
          account_id: account_id,
          backfill_completed_at: DateTime.utc_now()
        })

      message = StreamsFactory.insert_message!(%{stream_id: stream.id, key: "test.key"})

      # Insert initial consumer message
      StreamsFactory.insert_consumer_message!(%{
        consumer_id: consumer.id,
        message_key: message.key,
        message_seq: message.seq,
        state: :delivered
      })

      # Update the message
      updated_message = %{message | data: "updated data", data_hash: nil}
      assert {:ok, 1} = Streams.upsert_messages(stream.id, [updated_message])

      [updated_consumer_message] = Streams.list_consumer_messages_for_consumer(consumer.stream_id, consumer.id)
      assert updated_consumer_message.message_seq > message.seq
      assert updated_consumer_message.state == :pending_redelivery
    end

    test "upserts over consumer_messages for a :available message", %{stream: stream, account_id: account_id} do
      consumer =
        StreamsFactory.insert_consumer!(%{
          stream_id: stream.id,
          filter_key_pattern: "test.key",
          account_id: account_id,
          backfill_completed_at: DateTime.utc_now()
        })

      message = StreamsFactory.insert_message!(%{stream_id: stream.id, key: "test.key"})

      # Test with an available state
      StreamsFactory.insert_consumer_message!(%{
        consumer_id: consumer.id,
        message_key: message.key,
        message_seq: message.seq,
        state: :available
      })

      updated_message = %{message | data: "new data"}

      assert {:ok, 1} = Streams.upsert_messages(stream.id, [updated_message])

      [re_updated_consumer_message] = Streams.list_consumer_messages_for_consumer(consumer.stream_id, consumer.id)
      assert re_updated_consumer_message.message_seq > message.seq
      assert re_updated_consumer_message.state == :available
    end

    test "upserts over consumer_messages for a :acked message", %{stream: stream, account_id: account_id} do
      consumer =
        StreamsFactory.insert_consumer!(%{
          stream_id: stream.id,
          filter_key_pattern: "test.key",
          account_id: account_id,
          backfill_completed_at: DateTime.utc_now()
        })

      message = StreamsFactory.insert_message!(%{stream_id: stream.id, key: "test.key"})

      StreamsFactory.insert_consumer_message!(%{
        consumer_id: consumer.id,
        message_key: message.key,
        message_seq: message.seq,
        state: :acked
      })

      updated_message = %{message | data: "new data"}

      assert {:ok, 1} = Streams.upsert_messages(stream.id, [updated_message])

      [re_updated_consumer_message] = Streams.list_consumer_messages_for_consumer(consumer.stream_id, consumer.id)
      assert re_updated_consumer_message.state == :available
    end

    test "does not upsert for a consumer that has not finished backfilling", %{stream: stream, account_id: account_id} do
      consumer =
        StreamsFactory.insert_consumer!(%{
          stream_id: stream.id,
          filter_key_pattern: "test.key",
          account_id: account_id,
          backfill_completed_at: nil
        })

      message = StreamsFactory.message_attrs(%{stream_id: stream.id, key: "test.key"})
      assert {:ok, 1} = Streams.upsert_messages(stream.id, [message])
      assert [] = Streams.list_consumer_messages_for_consumer(consumer.stream_id, consumer.id)
    end
  end

  describe "list_messages_for_stream/2" do
    setup do
      stream = StreamsFactory.insert_stream!()
      {:ok, stream: stream}
    end

    test "filters messages with seq_gt", %{stream: stream} do
      message1 = StreamsFactory.insert_message!(%{stream_id: stream.id, key: "test.1"})
      StreamsFactory.insert_message!(%{stream_id: stream.id, key: "test.2"})
      StreamsFactory.insert_message!(%{stream_id: stream.id, key: "test.3"})

      results = Streams.list_messages_for_stream(stream.id, seq_gt: message1.seq)
      assert length(results) == 2
      assert Enum.map(results, & &1.key) == ["test.2", "test.3"]
    end

    test "applies order_by with limit", %{stream: stream} do
      StreamsFactory.insert_message!(%{stream_id: stream.id, key: "test.1"})
      StreamsFactory.insert_message!(%{stream_id: stream.id, key: "test.2"})
      StreamsFactory.insert_message!(%{stream_id: stream.id, key: "test.3"})

      results = Streams.list_messages_for_stream(stream.id, order_by: [desc: :seq], limit: 2)
      assert length(results) == 2
      assert Enum.map(results, & &1.key) == ["test.3", "test.2"]
    end

    test "filters messages with key_pattern - exact match", %{stream: stream} do
      StreamsFactory.insert_message!(%{stream_id: stream.id, key: "a.b.c"})
      StreamsFactory.insert_message!(%{stream_id: stream.id, key: "x.y.z"})

      results = Streams.list_messages_for_stream(stream.id, key_pattern: "a.b.c")
      assert length(results) == 1
      assert hd(results).key == "a.b.c"
    end

    test "filters messages with key_pattern - wildcard", %{stream: stream} do
      StreamsFactory.insert_message!(%{stream_id: stream.id, key: "a"})
      StreamsFactory.insert_message!(%{stream_id: stream.id, key: "a.b"})
      StreamsFactory.insert_message!(%{stream_id: stream.id, key: "a.b.c"})
      StreamsFactory.insert_message!(%{stream_id: stream.id, key: "a.x.c"})
      StreamsFactory.insert_message!(%{stream_id: stream.id, key: "a.b.c.d"})
      StreamsFactory.insert_message!(%{stream_id: stream.id, key: "x.y.z"})

      results = Streams.list_messages_for_stream(stream.id, key_pattern: "a.*.c")
      assert length(results) == 2
      assert results |> Enum.map(& &1.key) |> Enum.sort() == ["a.b.c", "a.x.c"]
    end

    test "filters messages with key_pattern - trailing wildcard", %{stream: stream} do
      StreamsFactory.insert_message!(%{stream_id: stream.id, key: "a.b"})
      StreamsFactory.insert_message!(%{stream_id: stream.id, key: "a.b.c"})
      StreamsFactory.insert_message!(%{stream_id: stream.id, key: "a.b.c.d"})
      StreamsFactory.insert_message!(%{stream_id: stream.id, key: "a.b.c.d.e"})
      StreamsFactory.insert_message!(%{stream_id: stream.id, key: "x.y.z"})

      results = Streams.list_messages_for_stream(stream.id, key_pattern: "a.b.>")
      assert length(results) == 3
      assert results |> Enum.map(& &1.key) |> Enum.sort() == ["a.b.c", "a.b.c.d", "a.b.c.d.e"]
    end
  end

  describe "list_consumer_messages_for_consumer/3" do
    setup do
      stream = StreamsFactory.insert_stream!()

      consumer =
        StreamsFactory.insert_consumer!(%{
          stream_id: stream.id,
          account_id: stream.account_id,
          backfill_completed_at: DateTime.utc_now()
        })

      {:ok, stream: stream, consumer: consumer}
    end

    test "applies order_by with limit", %{stream: stream, consumer: consumer} do
      message1 = StreamsFactory.insert_message!(%{stream_id: stream.id, key: "test.1"})
      message2 = StreamsFactory.insert_message!(%{stream_id: stream.id, key: "test.2"})
      message3 = StreamsFactory.insert_message!(%{stream_id: stream.id, key: "test.3"})

      StreamsFactory.insert_consumer_message!(%{consumer_id: consumer.id, message: message1})
      StreamsFactory.insert_consumer_message!(%{consumer_id: consumer.id, message: message2})
      StreamsFactory.insert_consumer_message!(%{consumer_id: consumer.id, message: message3})

      results =
        Streams.list_consumer_messages_for_consumer(stream.id, consumer.id, order_by: [desc: :message_seq], limit: 2)

      assert length(results) == 2
      assert Enum.map(results, & &1.message.key) == ["test.3", "test.2"]
    end

    test "filters consumer messages with key_pattern - exact match", %{stream: stream, consumer: consumer} do
      message1 = StreamsFactory.insert_message!(%{stream_id: stream.id, key: "a.b.c"})
      message2 = StreamsFactory.insert_message!(%{stream_id: stream.id, key: "x.y.z"})

      StreamsFactory.insert_consumer_message!(%{consumer_id: consumer.id, message: message1})
      StreamsFactory.insert_consumer_message!(%{consumer_id: consumer.id, message: message2})

      results = Streams.list_consumer_messages_for_consumer(stream.id, consumer.id, key_pattern: "a.b.c")
      assert length(results) == 1
      assert hd(results).message.key == "a.b.c"
    end

    test "filters consumer messages with key_pattern - wildcard", %{stream: stream, consumer: consumer} do
      message1 = StreamsFactory.insert_message!(%{stream_id: stream.id, key: "a.b.c"})
      message2 = StreamsFactory.insert_message!(%{stream_id: stream.id, key: "a.x.c"})
      message3 = StreamsFactory.insert_message!(%{stream_id: stream.id, key: "x.y.z"})

      StreamsFactory.insert_consumer_message!(%{consumer_id: consumer.id, message: message1})
      StreamsFactory.insert_consumer_message!(%{consumer_id: consumer.id, message: message2})
      StreamsFactory.insert_consumer_message!(%{consumer_id: consumer.id, message: message3})

      results = Streams.list_consumer_messages_for_consumer(stream.id, consumer.id, key_pattern: "a.*.c")
      assert length(results) == 2
      assert results |> Enum.map(& &1.message.key) |> Enum.sort() == ["a.b.c", "a.x.c"]
    end

    test "filters consumer messages with key_pattern - trailing wildcard", %{stream: stream, consumer: consumer} do
      message1 = StreamsFactory.insert_message!(%{stream_id: stream.id, key: "a.b.c"})
      message2 = StreamsFactory.insert_message!(%{stream_id: stream.id, key: "a.b.c.d"})
      message3 = StreamsFactory.insert_message!(%{stream_id: stream.id, key: "x.y.z"})

      StreamsFactory.insert_consumer_message!(%{consumer_id: consumer.id, message: message1})
      StreamsFactory.insert_consumer_message!(%{consumer_id: consumer.id, message: message2})
      StreamsFactory.insert_consumer_message!(%{consumer_id: consumer.id, message: message3})

      results = Streams.list_consumer_messages_for_consumer(stream.id, consumer.id, key_pattern: "a.b.>")
      assert length(results) == 2
      assert results |> Enum.map(& &1.message.key) |> Enum.sort() == ["a.b.c", "a.b.c.d"]
    end
  end

  describe "receive_for_consumer/2" do
    setup do
      stream = StreamsFactory.insert_stream!()

      consumer =
        StreamsFactory.insert_consumer!(%{stream_id: stream.id, account_id: stream.account_id, max_ack_pending: 1_000})

      {:ok, stream: stream, consumer: consumer}
    end

    test "returns nothing if outstanding messages is empty", %{consumer: consumer} do
      assert {:ok, []} = Streams.receive_for_consumer(consumer)
    end

    test "delivers available outstanding messages", %{stream: stream, consumer: consumer} do
      message = StreamsFactory.insert_message!(%{stream_id: stream.id})
      ack_wait_ms = consumer.ack_wait_ms

      cm =
        StreamsFactory.insert_consumer_message!(%{
          consumer_id: consumer.id,
          message: message,
          state: :available,
          deliver_count: 0
        })

      assert {:ok, [delivered_message]} = Streams.receive_for_consumer(consumer)
      # Add a buffer for the comparison
      not_visible_until = DateTime.add(DateTime.utc_now(), ack_wait_ms - 1000, :millisecond)
      assert delivered_message.ack_id == cm.ack_id
      assert delivered_message.key == message.key
      updated_om = Streams.reload(cm)
      assert updated_om.state == :delivered
      assert DateTime.after?(updated_om.not_visible_until, not_visible_until)
      assert updated_om.deliver_count == 1
      assert updated_om.last_delivered_at
    end

    test "redelivers expired outstanding messages", %{stream: stream, consumer: consumer} do
      message = StreamsFactory.insert_message!(%{stream_id: stream.id})

      cm =
        StreamsFactory.insert_consumer_message!(%{
          consumer_id: consumer.id,
          message: message,
          state: :delivered,
          not_visible_until: DateTime.add(DateTime.utc_now(), -1, :second),
          deliver_count: 1,
          last_delivered_at: DateTime.add(DateTime.utc_now(), -30, :second)
        })

      assert {:ok, [redelivered_msg]} = Streams.receive_for_consumer(consumer)
      assert redelivered_msg.key == message.key
      assert redelivered_msg.ack_id == cm.ack_id
      updated_om = Streams.reload(cm)
      assert updated_om.state == :delivered
      assert DateTime.compare(updated_om.not_visible_until, cm.not_visible_until) != :eq
      assert updated_om.deliver_count == 2
      assert DateTime.compare(updated_om.last_delivered_at, cm.last_delivered_at) != :eq
    end

    test "does not redeliver unexpired outstanding messages", %{stream: stream, consumer: consumer} do
      message = StreamsFactory.insert_message!(%{stream_id: stream.id})

      StreamsFactory.insert_consumer_message!(%{
        consumer_id: consumer.id,
        message: message,
        state: :delivered,
        not_visible_until: DateTime.add(DateTime.utc_now(), 30, :second)
      })

      assert {:ok, []} = Streams.receive_for_consumer(consumer)
    end

    test "delivers only up to batch_size", %{stream: stream, consumer: consumer} do
      for _ <- 1..3 do
        message = StreamsFactory.insert_message!(%{stream_id: stream.id})

        StreamsFactory.insert_consumer_message!(%{
          consumer_id: consumer.id,
          message: message,
          state: :available
        })
      end

      assert {:ok, delivered} = Streams.receive_for_consumer(consumer, batch_size: 2)
      assert length(delivered) == 2
      assert length(Streams.list_consumer_messages_for_consumer(consumer.stream_id, consumer.id)) == 3
    end

    test "does not deliver outstanding messages for another consumer", %{stream: stream, consumer: consumer} do
      other_consumer = StreamsFactory.insert_consumer!(%{stream_id: stream.id, account_id: stream.account_id})
      message = StreamsFactory.insert_message!(%{stream_id: stream.id})

      StreamsFactory.insert_consumer_message!(%{
        consumer_id: other_consumer.id,
        message: message,
        state: :available
      })

      assert {:ok, []} = Streams.receive_for_consumer(consumer)
    end

    test "with a mix of available and unavailble messages, delivers only available outstanding messages", %{
      stream: stream,
      consumer: consumer
    } do
      # Available messages
      available =
        for _ <- 1..3 do
          msg = StreamsFactory.insert_message!(%{stream_id: stream.id})

          StreamsFactory.insert_consumer_message!(%{
            consumer_id: consumer.id,
            message: msg,
            state: :available
          })

          msg
        end

      redeliver =
        for _ <- 1..3 do
          msg = StreamsFactory.insert_message!(%{stream_id: stream.id})

          StreamsFactory.insert_consumer_message!(%{
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

        StreamsFactory.insert_consumer_message!(%{
          consumer_id: consumer.id,
          message: msg,
          state: :delivered,
          not_visible_until: DateTime.add(DateTime.utc_now(), 30, :second)
        })

        msg
      end

      assert {:ok, messages} = Streams.receive_for_consumer(consumer)
      assert length(messages) == length(available ++ redeliver)
      assert_lists_equal(messages, available ++ redeliver, &assert_maps_equal(&1, &2, [:key, :stream_id]))
    end

    test "delivers messages according to message_seq asc", %{stream: stream, consumer: consumer} do
      now = DateTime.utc_now()

      # Oldest delivered message
      message1 = StreamsFactory.insert_message!(%{stream_id: stream.id, seq: 1})

      StreamsFactory.insert_consumer_message!(%{
        consumer_id: consumer.id,
        message: message1,
        state: :delivered,
        not_visible_until: DateTime.add(now, -1, :minute)
      })

      # Second oldest delivered message
      message2 = StreamsFactory.insert_message!(%{stream_id: stream.id, seq: 2})

      StreamsFactory.insert_consumer_message!(%{
        consumer_id: consumer.id,
        message: message2,
        state: :delivered,
        not_visible_until: DateTime.add(now, -1, :minute)
      })

      # Newest delivered message (should not be returned)
      message3 = StreamsFactory.insert_message!(%{stream_id: stream.id, seq: 3})

      StreamsFactory.insert_consumer_message!(%{
        consumer_id: consumer.id,
        message: message3,
        state: :delivered,
        not_visible_until: DateTime.add(now, -1, :minute)
      })

      message4 = StreamsFactory.insert_message!(%{stream_id: stream.id, seq: 4})

      StreamsFactory.insert_consumer_message!(%{
        consumer_id: consumer.id,
        message: message4,
        state: :available
      })

      assert {:ok, delivered} = Streams.receive_for_consumer(consumer, batch_size: 2)
      assert length(delivered) == 2
      delivered_message_keys = Enum.map(delivered, & &1.key)
      assert_lists_equal(delivered_message_keys, [message1.key, message2.key])
    end

    test "respect's a consumer's max_ack_pending", %{consumer: consumer} do
      max_ack_pending = 3
      consumer = %{consumer | max_ack_pending: max_ack_pending}

      cm =
        StreamsFactory.insert_consumer_message_with_message!(%{
          consumer_id: consumer.id,
          message_stream_id: consumer.stream_id,
          state: :available
        })

      # These messages can't be delivered
      for _ <- 1..2 do
        StreamsFactory.insert_consumer_message_with_message!(%{
          consumer_id: consumer.id,
          message_stream_id: consumer.stream_id,
          state: :delivered,
          not_visible_until: DateTime.add(DateTime.utc_now(), 30, :second)
        })
      end

      # These messages *can* be delivered, but are outside max_ack_pending
      for _ <- 1..2 do
        StreamsFactory.insert_consumer_message_with_message!(%{
          consumer_id: consumer.id,
          message_stream_id: consumer.stream_id,
          state: :available
        })
      end

      assert {:ok, delivered} = Streams.receive_for_consumer(consumer)
      assert length(delivered) == 1
      delivered = List.last(delivered)
      assert delivered.key == cm.message_key
      assert {:ok, []} = Streams.receive_for_consumer(consumer)
    end
  end

  describe "receive_for_consumer with concurrent workers" do
    setup do
      stream = StreamsFactory.insert_stream!()

      consumer =
        StreamsFactory.insert_consumer!(stream_id: stream.id, account_id: stream.account_id, max_ack_pending: 100)

      # Insert 10 messages
      messages =
        Enum.map(1..10, fn _ ->
          StreamsFactory.insert_message!(stream_id: stream.id)
        end)

      # Insert consumer messages for each message
      Enum.each(messages, fn message ->
        StreamsFactory.insert_consumer_message!(
          consumer_id: consumer.id,
          message: message,
          state: :available
        )
      end)

      {:ok, stream: stream, consumer: consumer}
    end

    test "ensures unique messages are received by concurrent workers", %{consumer: consumer} do
      # Create 20 tasks that each try to receive one message
      tasks =
        Enum.map(1..20, fn _ ->
          Task.async(fn ->
            Streams.receive_for_consumer(consumer, batch_size: 1)
          end)
        end)

      # Wait for all tasks to complete
      results = Task.await_many(tasks, 5000)

      # Count successful and empty results
      {successful, empty} =
        Enum.reduce(results, {[], []}, fn result, {successful, empty} ->
          case result do
            {:ok, [message]} -> {[message | successful], empty}
            {:ok, []} -> {successful, ["empty" | empty]}
          end
        end)

      # Verify that we got 10 unique messages and 10 empty results
      assert length(successful) == 10
      assert length(empty) == 10

      # Verify that all received messages are unique
      unique_messages = Enum.uniq_by(successful, & &1.key)
      assert length(unique_messages) == 10
    end
  end

  @one_day_ago DateTime.add(DateTime.utc_now(), -24, :hour)

  describe "ack_messages/2" do
    setup do
      stream = StreamsFactory.insert_stream!()

      consumer =
        StreamsFactory.insert_consumer!(
          stream_id: stream.id,
          account_id: stream.account_id,
          backfill_completed_at: @one_day_ago
        )

      %{stream: stream, consumer: consumer}
    end

    test "acks delivered messages and ignores non-existent message_ids", %{stream: stream, consumer: consumer} do
      message1 = StreamsFactory.insert_message!(stream_id: stream.id)
      message2 = StreamsFactory.insert_message!(stream_id: stream.id)

      om1 =
        StreamsFactory.insert_consumer_message_with_message!(%{
          consumer_id: consumer.id,
          message_stream_id: stream.id,
          state: :delivered,
          message: message1
        })

      om2 =
        StreamsFactory.insert_consumer_message_with_message!(%{
          consumer_id: consumer.id,
          message_stream_id: stream.id,
          state: :delivered,
          message: message2
        })

      non_existent_id = Factory.uuid()

      :ok = Streams.ack_messages(consumer, [om1.ack_id, om2.ack_id, non_existent_id])

      assert Streams.all_consumer_messages() == []
    end

    test "ignores messages with different consumer_id", %{stream: stream, consumer: consumer} do
      other_consumer = StreamsFactory.insert_consumer!(stream_id: stream.id, account_id: stream.account_id)

      om1 =
        StreamsFactory.insert_consumer_message_with_message!(%{
          consumer_id: consumer.id,
          message_stream_id: stream.id,
          state: :delivered
        })

      om2 =
        StreamsFactory.insert_consumer_message_with_message!(%{
          consumer_id: other_consumer.id,
          message_stream_id: stream.id,
          state: :delivered
        })

      :ok = Streams.ack_messages(consumer, [om1.ack_id, om2.ack_id])

      outstanding = Streams.all_consumer_messages()
      assert length(outstanding) == 1
      assert List.first(outstanding).ack_id == om2.ack_id
    end

    test "flips pending_redelivery messages to available", %{stream: stream, consumer: consumer} do
      cm =
        StreamsFactory.insert_consumer_message_with_message!(%{
          consumer_id: consumer.id,
          message_stream_id: stream.id,
          state: :pending_redelivery,
          not_visible_until: DateTime.utc_now()
        })

      :ok = Streams.ack_messages(consumer, [cm.ack_id])

      updated_om = Streams.get_consumer_message!(cm.consumer_id, cm.message_key)
      assert updated_om.state == :available
      refute updated_om.not_visible_until
    end
  end

  describe "nack_messages/2" do
    setup do
      stream = StreamsFactory.insert_stream!()
      consumer = StreamsFactory.insert_consumer!(stream_id: stream.id, account_id: stream.account_id)
      %{stream: stream, consumer: consumer}
    end

    test "nacks messages and ignores unknown message ID", %{stream: stream, consumer: consumer} do
      om1 =
        StreamsFactory.insert_consumer_message_with_message!(%{
          consumer_id: consumer.id,
          message_stream_id: stream.id,
          state: :delivered,
          not_visible_until: DateTime.utc_now()
        })

      om2 =
        StreamsFactory.insert_consumer_message_with_message!(%{
          consumer_id: consumer.id,
          message_stream_id: stream.id,
          state: :delivered,
          not_visible_until: DateTime.utc_now()
        })

      non_existent_id = Factory.uuid()

      :ok = Streams.nack_messages(consumer, [om1.ack_id, om2.ack_id, non_existent_id])

      updated_om1 = Streams.reload(om1)
      updated_om2 = Streams.reload(om2)

      assert updated_om1.state == :available
      refute updated_om1.not_visible_until
      assert updated_om2.state == :available
      refute updated_om2.not_visible_until
    end

    test "does not nack messages belonging to another consumer", %{stream: stream, consumer: consumer} do
      other_consumer = StreamsFactory.insert_consumer!(stream_id: stream.id, account_id: stream.account_id)

      om1 =
        StreamsFactory.insert_consumer_message_with_message!(%{
          consumer_id: consumer.id,
          message_stream_id: stream.id,
          state: :delivered,
          not_visible_until: DateTime.utc_now()
        })

      om2 =
        StreamsFactory.insert_consumer_message_with_message!(%{
          consumer_id: other_consumer.id,
          message_stream_id: stream.id,
          state: :delivered,
          not_visible_until: DateTime.utc_now()
        })

      :ok = Streams.nack_messages(consumer, [om1.ack_id, om2.ack_id])

      updated_om1 = Streams.reload(om1)
      updated_om2 = Streams.reload(om2)

      assert updated_om1.state == :available
      refute updated_om1.not_visible_until
      assert updated_om2.state == :delivered
      assert DateTime.compare(updated_om2.not_visible_until, om2.not_visible_until) == :eq
    end
  end
end

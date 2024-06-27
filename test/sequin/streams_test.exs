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
      msg = StreamsFactory.insert_message!(%{seq: 1})
      new_data = StreamsFactory.message_data()

      assert {:ok, 1} = Streams.upsert_messages([%{msg | data: new_data, data_hash: nil}])
      updated_msg = Streams.message!(msg.key, msg.stream_id)

      assert updated_msg.data == new_data
      assert is_binary(updated_msg.data_hash)
      refute updated_msg.data_hash == msg.data_hash
      assert is_nil(updated_msg.seq)
    end

    test "does not update existing message when data_hash is the same" do
      msg = StreamsFactory.insert_message!(%{seq: 1})

      assert {:ok, 0} = Streams.upsert_messages([msg])

      updated_msg = Streams.message!(msg.key, msg.stream_id)
      assert updated_msg.seq == 1
      assert updated_msg.data == msg.data
      assert updated_msg.data_hash == msg.data_hash
      assert updated_msg.updated_at == msg.updated_at
    end

    test "does not affect messages with same key but different stream_id" do
      stream1 = StreamsFactory.insert_stream!()
      stream2 = StreamsFactory.insert_stream!()

      msg1 = StreamsFactory.insert_message!(%{stream_id: stream1.id, key: "same_key", seq: 1})
      msg2_attrs = StreamsFactory.message_attrs(%{stream_id: stream2.id, key: "same_key"})

      assert {:ok, 1} = Streams.upsert_messages([msg2_attrs])

      unchanged_msg1 = Streams.message!(msg1.key, msg1.stream_id)
      assert unchanged_msg1.seq == 1
      assert unchanged_msg1.data == msg1.data

      new_msg2 = Streams.message!(msg2_attrs.key, msg2_attrs.stream_id)
      assert new_msg2.data == msg2_attrs.data
    end

    test "retries on untranslatable_character error" do
      stream = StreamsFactory.insert_stream!()
      msg_attrs = StreamsFactory.message_attrs(%{stream_id: stream.id, data: "data with null byte \u0000"})

      assert {:ok, 1} = Streams.upsert_messages([msg_attrs])

      inserted_msg = Streams.message!(msg_attrs.key, msg_attrs.stream_id)
      assert inserted_msg.data == "data with null byte "
    end
  end

  test "updating a message resets its seq to nil via a trigger" do
    msg = StreamsFactory.insert_message!(%{seq: 1})

    new_data = StreamsFactory.message_data()

    {1, _} =
      msg.key
      |> Streams.Message.where_key_and_stream_id(msg.stream_id)
      |> Repo.update_all(set: [data: new_data])

    updated_msg = Streams.message!(msg.key, msg.stream_id)
    assert is_nil(updated_msg.seq)
    assert updated_msg.data == new_data
  end
end

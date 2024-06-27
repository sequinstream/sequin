defmodule Sequin.OutstandingMessagesTest do
  use Sequin.DataCase, async: true

  alias Sequin.Factory.StreamsFactory
  alias Sequin.Streams

  describe "buffering messages into outstanding messages" do
    @max_ack_pending 10
    @max_outstanding_messages @max_ack_pending * 5

    setup do
      stream = StreamsFactory.insert_stream!()

      {:ok, consumer} =
        [max_ack_pending: @max_ack_pending, stream_id: stream.id, account_id: stream.account_id]
        |> StreamsFactory.consumer_attrs()
        |> Streams.create_consumer_with_lifecycle()

      %{stream: stream, consumer: consumer}
    end

    test "query populates outstanding messages with a buffer", %{stream: stream, consumer: consumer} do
      messages = for n <- 1..100, do: StreamsFactory.insert_message!(%{stream_id: consumer.stream_id, seq: n})
      pulled_messages = Enum.take(messages, @max_outstanding_messages)

      assert length(Streams.outstanding_messages_for_consumer(consumer.id)) == 0

      :ok = Streams.populate_outstanding_messages(consumer)

      outstanding_messages = Streams.outstanding_messages_for_consumer(consumer.id)
      assert length(outstanding_messages) == @max_outstanding_messages

      assert_lists_equal(pulled_messages, outstanding_messages, fn m, om -> m.key == om.message_key end)

      assert Enum.all?(outstanding_messages, fn om ->
               om.state == :available and om.deliver_count == 0 and om.message_stream_id == stream.id
             end)

      assert %{count_pulled_into_outstanding: @max_outstanding_messages, message_seq_cursor: next_seq} =
               Streams.consumer_state(consumer.id)

      assert next_seq == pulled_messages |> Enum.map(& &1.seq) |> Enum.max()
    end

    test "only buffers messages for this consumer-stream", %{stream: stream, consumer: consumer} do
      other_stream = StreamsFactory.insert_stream!()
      for n <- 1..10, do: StreamsFactory.insert_message!(%{stream_id: consumer.stream_id, seq: n})
      for n <- 1..10, do: StreamsFactory.insert_message!(%{stream_id: other_stream.id, seq: n})

      :ok = Streams.populate_outstanding_messages(consumer)

      outstanding_messages = Streams.outstanding_messages_for_consumer(consumer.id)
      assert length(outstanding_messages) == 10
      assert Enum.all?(outstanding_messages, &(&1.message_stream_id == stream.id))
    end

    test "if message already outstanding and delivered, flipped to `pending_redelivery`", %{consumer: consumer} do
      message = StreamsFactory.insert_message!(%{stream_id: consumer.stream_id, seq: 1})

      StreamsFactory.insert_outstanding_message!(%{
        consumer_id: consumer.id,
        message_key: message.key,
        message_seq: message.seq,
        message_stream_id: message.stream_id,
        state: :delivered
      })

      :ok = Streams.populate_outstanding_messages(consumer)

      [outstanding_message] = Streams.outstanding_messages_for_consumer(consumer.id)
      assert outstanding_message.state == :pending_redelivery
    end

    test "if message already outstanding and available, stays available", %{consumer: consumer} do
      message = StreamsFactory.insert_message!(%{stream_id: consumer.stream_id, seq: 1})

      StreamsFactory.insert_outstanding_message!(%{
        consumer_id: consumer.id,
        message_key: message.key,
        message_seq: message.seq,
        message_stream_id: message.stream_id,
        state: :available
      })

      :ok = Streams.populate_outstanding_messages(consumer)

      [outstanding_message] = Streams.outstanding_messages_for_consumer(consumer.id)
      assert outstanding_message.state == :available
    end

    test "`count_pulled_into_outstanding` is updated", %{consumer: consumer} do
      for n <- 1..5, do: StreamsFactory.insert_message!(%{stream_id: consumer.stream_id, seq: n})

      :ok = Streams.populate_outstanding_messages(consumer)

      assert %{count_pulled_into_outstanding: 5} = Streams.consumer_state(consumer.id)

      for n <- 6..10, do: StreamsFactory.insert_message!(%{stream_id: consumer.stream_id, seq: n})

      :ok = Streams.populate_outstanding_messages(consumer)

      assert %{count_pulled_into_outstanding: 10} = Streams.consumer_state(consumer.id)
    end

    test "if no messages to pull, consumer_state/message count does not get updated", %{consumer: consumer} do
      assert %{count_pulled_into_outstanding: 0, message_seq_cursor: 0} = Streams.consumer_state(consumer.id)

      :ok = Streams.populate_outstanding_messages(consumer)

      assert %{count_pulled_into_outstanding: 0, message_seq_cursor: 0} = Streams.consumer_state(consumer.id)
    end
  end
end

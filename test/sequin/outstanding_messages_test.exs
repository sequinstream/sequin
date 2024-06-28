defmodule Sequin.OutstandingMessagesTest do
  use Sequin.DataCase, async: true

  alias Sequin.Factory.StreamsFactory
  alias Sequin.Streams
  alias Sequin.StreamsRuntime.PopulateOutstandingMessages

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
      messages = for _ <- 1..100, do: StreamsFactory.insert_message!(%{stream_id: consumer.stream_id})
      pulled_messages = Enum.take(messages, @max_outstanding_messages)

      assert length(Streams.list_outstanding_messages_for_consumer(consumer.id)) == 0

      :ok = Streams.populate_outstanding_messages(consumer)

      outstanding_messages = Streams.list_outstanding_messages_for_consumer(consumer.id)
      assert length(outstanding_messages) == @max_outstanding_messages

      assert_lists_equal(pulled_messages, outstanding_messages, fn m, om -> m.key == om.message_key end)

      assert Enum.all?(outstanding_messages, fn om ->
               om.state == :available and om.deliver_count == 0 and om.message_stream_id == stream.id
             end)

      assert %{count_pulled_into_outstanding: @max_outstanding_messages, message_seq_cursor: next_seq} =
               Streams.get_consumer_state(consumer.id)

      assert next_seq == pulled_messages |> Enum.map(& &1.seq) |> Enum.max()
    end

    test "only buffers messages for this consumer-stream", %{stream: stream, consumer: consumer} do
      other_stream = StreamsFactory.insert_stream!()
      for _ <- 1..10, do: StreamsFactory.insert_message!(%{stream_id: consumer.stream_id})
      for _ <- 1..10, do: StreamsFactory.insert_message!(%{stream_id: other_stream.id})

      :ok = Streams.populate_outstanding_messages(consumer)

      outstanding_messages = Streams.list_outstanding_messages_for_consumer(consumer.id)
      assert length(outstanding_messages) == 10
      assert Enum.all?(outstanding_messages, &(&1.message_stream_id == stream.id))
    end

    test "if nothing to buffer, get_consumer_state is untouched", %{consumer: consumer} do
      %{seq: seq} = StreamsFactory.insert_message!(%{stream_id: consumer.stream_id})

      :ok = Streams.populate_outstanding_messages(consumer)

      assert length(Streams.list_outstanding_messages_for_consumer(consumer.id)) == 1
      assert %{message_seq_cursor: ^seq} = Streams.get_consumer_state(consumer.id)

      :ok = Streams.populate_outstanding_messages(consumer)
      assert length(Streams.list_outstanding_messages_for_consumer(consumer.id)) == 1
      assert %{message_seq_cursor: ^seq} = Streams.get_consumer_state(consumer.id)
    end

    test "if message already outstanding and delivered, flipped to `pending_redelivery`", %{consumer: consumer} do
      message = StreamsFactory.insert_message!(%{stream_id: consumer.stream_id})

      StreamsFactory.insert_outstanding_message!(%{
        consumer_id: consumer.id,
        message_key: message.key,
        message_seq: message.seq,
        message_stream_id: message.stream_id,
        state: :delivered
      })

      :ok = Streams.populate_outstanding_messages(consumer)

      [outstanding_message] = Streams.list_outstanding_messages_for_consumer(consumer.id)
      assert outstanding_message.state == :pending_redelivery
    end

    test "if message already outstanding and available, stays available", %{consumer: consumer} do
      message = StreamsFactory.insert_message!(%{stream_id: consumer.stream_id})

      StreamsFactory.insert_outstanding_message!(%{
        consumer_id: consumer.id,
        message_key: message.key,
        message_seq: message.seq,
        message_stream_id: message.stream_id,
        state: :available
      })

      :ok = Streams.populate_outstanding_messages(consumer)

      [outstanding_message] = Streams.list_outstanding_messages_for_consumer(consumer.id)
      assert outstanding_message.state == :available
    end

    test "`count_pulled_into_outstanding` is updated", %{consumer: consumer} do
      for _ <- 1..5, do: StreamsFactory.insert_message!(%{stream_id: consumer.stream_id})

      :ok = Streams.populate_outstanding_messages(consumer)

      assert %{count_pulled_into_outstanding: 5} = Streams.get_consumer_state(consumer.id)

      for _ <- 1..5, do: StreamsFactory.insert_message!(%{stream_id: consumer.stream_id})

      :ok = Streams.populate_outstanding_messages(consumer)

      assert %{count_pulled_into_outstanding: 10} = Streams.get_consumer_state(consumer.id)
    end

    test "if no messages to pull, get_consumer_state/message count does not get updated", %{consumer: consumer} do
      assert %{count_pulled_into_outstanding: 0, message_seq_cursor: 0} = Streams.get_consumer_state(consumer.id)

      :ok = Streams.populate_outstanding_messages(consumer)

      assert %{count_pulled_into_outstanding: 0, message_seq_cursor: 0} = Streams.get_consumer_state(consumer.id)
    end
  end

  describe "PopulateOutstandingMessages" do
    setup do
      stream = StreamsFactory.insert_stream!()

      {:ok, consumer} =
        [stream_id: stream.id, account_id: stream.account_id]
        |> StreamsFactory.consumer_attrs()
        |> Streams.create_consumer_with_lifecycle()

      %{consumer: consumer}
    end

    test "populates outstanding messages for a consumer", %{consumer: consumer} do
      messages = for _ <- 1..5, do: StreamsFactory.insert_message!(%{stream_id: consumer.stream_id})

      # Use a long interval to prevent the server from querying the db while it's being
      # killed, which produces noisy logs
      start_supervised!({PopulateOutstandingMessages, consumer_id: consumer.id, test_pid: self(), interval_ms: 5_000})

      assert_receive {PopulateOutstandingMessages, :populate_done}

      outstanding_messages = Streams.list_outstanding_messages_for_consumer(consumer.id)
      assert length(outstanding_messages) == 5

      assert_lists_equal(messages, outstanding_messages, fn m, om ->
        m.key == om.message_key and m.seq == om.message_seq and m.stream_id == om.message_stream_id
      end)
    end

    test "updates consumer state after populating outstanding messages", %{consumer: consumer} do
      messages = for _ <- 1..5, do: StreamsFactory.insert_message!(%{stream_id: consumer.stream_id})
      msg = List.last(messages)

      start_supervised!({PopulateOutstandingMessages, consumer_id: consumer.id, test_pid: self(), interval_ms: 5_000})

      assert_receive {PopulateOutstandingMessages, :populate_done}

      consumer_state = Streams.get_consumer_state(consumer.id)
      assert consumer_state.message_seq_cursor == msg.seq
      assert consumer_state.count_pulled_into_outstanding == 5
    end
  end
end

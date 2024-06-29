defmodule Sequin.StreamsIntegrationTest do
  use Sequin.DataCase, async: true

  alias Sequin.Factory.StreamsFactory
  alias Sequin.Streams
  alias Sequin.StreamsRuntime.AssignMessageSeq
  alias Sequin.StreamsRuntime.PopulateOutstandingMessages

  describe "streams integration" do
    test "full message lifecycle" do
      # Create a stream and consumer
      stream = StreamsFactory.insert_stream!()
      other_stream = StreamsFactory.insert_stream!()

      {:ok, consumer} =
        %{
          stream_id: stream.id,
          account_id: stream.account_id,
          max_ack_pending: 10
        }
        |> StreamsFactory.consumer_attrs()
        |> Streams.create_consumer_with_lifecycle()

      # Upsert a batch of messages for both streams
      messages_for_stream = for _ <- 1..5, do: StreamsFactory.message_attrs(%{stream_id: stream.id})
      messages_for_other_stream = for _ <- 1..5, do: StreamsFactory.message_attrs(%{stream_id: other_stream.id})

      {:ok, _} = Streams.upsert_messages(messages_for_stream ++ messages_for_other_stream)

      # Boot AssignMessageSeq
      start_supervised!({AssignMessageSeq, stream_id: stream.id, test_pid: self(), interval_ms: 100})
      assert_receive {AssignMessageSeq, :assign_done}

      # Boot PopulateOutstandingMessages
      start_supervised!({PopulateOutstandingMessages, consumer_id: consumer.id, test_pid: self(), interval_ms: 100})
      assert_receive {PopulateOutstandingMessages, :populate_done}

      # Call next_for_consumer
      {:ok, received_messages} = Streams.next_for_consumer(consumer)

      # Assert we got the messages for the correct stream
      assert length(received_messages) == 5
      assert Enum.all?(received_messages, &(&1.stream_id == stream.id))

      # Call next_for_consumer again - we shouldn't receive any messages
      {:ok, empty_messages} = Streams.next_for_consumer(consumer)
      assert empty_messages == []

      # Get the outstanding messages
      outstanding_messages = Streams.list_outstanding_messages_for_consumer(consumer.id)
      assert length(outstanding_messages) == 5

      # Call ack_messages
      message_ids = Enum.map(outstanding_messages, & &1.id)
      :ok = Streams.ack_messages(consumer.id, message_ids)

      # Assert messages were deleted from OutstandingMessages
      assert Streams.list_outstanding_messages_for_consumer(consumer.id) == []
    end
  end
end

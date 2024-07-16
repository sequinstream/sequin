defmodule Sequin.StreamTest do
  use Sequin.DataCase, async: true

  alias Sequin.Factory.StreamsFactory
  alias Sequin.Streams.Stream

  describe "load_stats/1" do
    setup do
      stream = StreamsFactory.insert_stream!()
      {:ok, stream: stream}
    end

    test "loads the stats for a stream", %{stream: stream} do
      stream = Stream.load_stats(stream)
      assert stream.stats.message_count == 0
      assert stream.stats.consumer_count == 0

      StreamsFactory.insert_consumer!(stream_id: stream.id, account_id: stream.account_id)
      stream = Stream.load_stats(stream)
      assert stream.stats.message_count == 0
      assert stream.stats.consumer_count == 1

      for _ <- 1..10 do
        StreamsFactory.insert_message!(stream_id: stream.id)
      end

      stream = Stream.load_stats(stream)
      assert stream.stats.message_count == 10
      assert stream.stats.consumer_count == 1
    end
  end
end

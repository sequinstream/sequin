defmodule Sequin.AssignMessageSeqTest do
  use Sequin.DataCase, async: true

  alias Sequin.Factory
  alias Sequin.Factory.StreamsFactory
  alias Sequin.Streams
  alias Sequin.StreamsRuntime.AssignMessageSeq

  describe "assigning seq to messages with null seqs" do
    test "assigns seq to only messages with null seqs" do
      stream_id = StreamsFactory.insert_stream!().id
      new_messages = for _ <- 1..3, do: StreamsFactory.insert_message!(%{stream_id: stream_id})
      existing_messages = for _ <- 1..3, do: StreamsFactory.insert_message!(%{stream_id: stream_id})
      existing_message_seqs = Enum.map(existing_messages, & &1.seq)

      Streams.assign_message_seqs(stream_id)

      new_messages = Enum.map(new_messages, &Streams.reload(&1))

      updated_existing_message_seqs =
        existing_messages |> Enum.map(&Streams.reload(&1)) |> Enum.map(& &1.seq)

      assert Enum.all?(new_messages, &(not is_nil(&1.seq)))
      assert_lists_equal(existing_message_seqs, updated_existing_message_seqs)
    end

    test "does not touch messages for another stream" do
      stream_id = Factory.uuid()
      other_stream_messages = for _ <- 1..3, do: StreamsFactory.insert_message!(%{seq: nil})

      Streams.assign_message_seqs(stream_id)

      assert Enum.all?(other_stream_messages, &is_nil(&1.seq))
    end
  end

  describe "AssignMessageSeq" do
    test "assigns seq to messages with null seqs" do
      stream_id = StreamsFactory.insert_stream!().id
      messages = for _ <- 1..3, do: StreamsFactory.insert_message!(%{stream_id: stream_id})

      # Use a long interval to prevent the server from querying the db while it's being
      # killed, which produces noisy logs
      start_supervised!({AssignMessageSeq, test_pid: self(), stream_id: stream_id, interval_ms: 5_000})

      assert_receive {AssignMessageSeq, :assign_done}

      messages = Enum.map(messages, &Streams.reload(&1))

      assert Enum.all?(messages, & &1.seq)
    end
  end
end

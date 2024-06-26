defmodule Sequin.AssignMessageSeqTest do
  use Sequin.DataCase, async: true

  alias Sequin.Factory.StreamsFactory
  alias Sequin.Streams

  describe "assigning seq to messages with null seqs" do
    test "assigns seq to only messages with null seqs" do
      new_messages = for _ <- 1..3, do: StreamsFactory.insert_message!(%{seq: nil})
      existing_messages = for n <- 1..3, do: StreamsFactory.insert_message!(%{seq: n})
      existing_message_seqs = Enum.map(existing_messages, & &1.seq)

      Streams.assign_message_seqs()

      new_messages = Enum.map(new_messages, &Streams.get!(&1.key, &1.stream_id))

      updated_existing_message_seqs =
        existing_messages |> Enum.map(&Streams.get!(&1.key, &1.stream_id)) |> Enum.map(& &1.seq)

      assert Enum.all?(new_messages, &(not is_nil(&1.seq)))
      assert_lists_equal(existing_message_seqs, updated_existing_message_seqs)
    end
  end
end

defmodule Sequin.Consumers.RedisStreamSinkTest do
  use ExUnit.Case, async: true

  alias Sequin.Consumers.RedisStreamSink

  describe "changeset/2" do
    test "requires stream_key when routing_mode is static" do
      params = %{
        host: "localhost",
        port: 6379,
        stream_key: "my-stream",
        routing_mode: :static
      }

      changeset = RedisStreamSink.changeset(%RedisStreamSink{}, params)
      assert changeset.valid?
    end

    test "clears stream_key when routing_mode is dynamic" do
      params = %{
        host: "localhost",
        port: 6379,
        stream_key: "my-stream",
        routing_mode: :dynamic
      }

      changeset = RedisStreamSink.changeset(%RedisStreamSink{}, params)
      refute Map.has_key?(changeset.changes, :stream_key)
    end
  end
end

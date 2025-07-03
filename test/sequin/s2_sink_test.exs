defmodule Sequin.Consumers.S2SinkTest do
  use ExUnit.Case, async: true

  alias Sequin.Consumers.S2Sink

  describe "changeset/2" do
    test "requires basin and stream when routing_mode is static" do
      params = %{basin: "test-basin", stream: "s", access_token: "token", routing_mode: :static}
      changeset = S2Sink.changeset(%S2Sink{}, params)
      assert changeset.valid?
    end

    test "clears basin and stream when routing_mode is dynamic" do
      params = %{basin: "test-basin", stream: "s", access_token: "token", routing_mode: :dynamic}
      changeset = S2Sink.changeset(%S2Sink{}, params)
      refute Map.has_key?(changeset.changes, :basin)
      refute Map.has_key?(changeset.changes, :stream)
    end
  end
end

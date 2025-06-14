defmodule Sequin.Sinks.S2.ClientTest do
  use Sequin.Case, async: true

  alias Sequin.Consumers.S2Sink
  alias Sequin.Factory.SinkFactory
  alias Sequin.Sinks.S2.Client

  @sink %S2Sink{type: :s2, basin: "test", stream: "test-stream", access_token: "token"}

  describe "append_records/2" do
    test "successfully sends batch" do
      records = [SinkFactory.s2_record(), SinkFactory.s2_record()]

      Req.Test.expect(Client, fn conn ->
        assert conn.method == "POST"
        assert conn.request_path == "/v1/streams/test-stream/records"
        Req.Test.json(conn, %{"ok" => true})
      end)

      assert :ok = Client.append_records(@sink, records)
    end
  end

  describe "test_connection/1" do
    test "returns ok on 200" do
      Req.Test.expect(Client, fn conn ->
        assert conn.method == "GET"
        assert conn.request_path == "/v1/streams/test-stream"
        Req.Test.json(conn, %{})
      end)

      assert :ok = Client.test_connection(@sink)
    end
  end
end

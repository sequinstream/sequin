defmodule Sequin.Sinks.S2.ClientTest do
  use Sequin.Case, async: true

  alias Sequin.Sinks.S2.Client
  alias Sequin.Factory.SinkFactory
  alias Sequin.Consumers.S2Sink

  @sink %S2Sink{type: :s2, endpoint_url: "https://s2.dev", stream: "test-stream", access_token: "token"}

  describe "append_records/2" do
    test "successfully sends batch" do
      records = [SinkFactory.s2_record(), SinkFactory.s2_record()]

      Req.Test.stub(Client, fn conn ->
        assert conn.method == :post
        Req.Test.json(conn, %{"ok" => true})
      end)

      assert :ok = Client.append_records(@sink, records)
    end
  end

  describe "test_connection/1" do
    test "returns ok on 200" do
      Req.Test.stub(Client, fn conn ->
        assert conn.method == :get
        Req.Test.json(conn, %{}, status: 200)
      end)

      assert :ok = Client.test_connection(@sink)
    end
  end
end

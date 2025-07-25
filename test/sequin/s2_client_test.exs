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

  describe "retry behavior" do
    test "retries transient errors once before failing on append_records" do
      test_pid = self()
      call_count = :counters.new(1, [])
      records = [SinkFactory.s2_record()]

      Req.Test.stub(Client, fn conn ->
        count = :counters.get(call_count, 1)
        :counters.add(call_count, 1, 1)
        send(test_pid, {:attempt, count})

        if count == 0 do
          # First attempt fails with a transient error
          conn
          |> Plug.Conn.put_status(429)
          |> Req.Test.json(%{"message" => "Too Many Requests"})
        else
          # Second attempt succeeds
          Req.Test.json(conn, %{"ok" => true})
        end
      end)

      # Should succeed after retry
      assert :ok = Client.append_records(@sink, records)

      # Verify both attempts were made
      assert_receive {:attempt, 0}, 500
      assert_receive {:attempt, 1}, 1000
    end

    test "fails after exhausting retries on append_records" do
      test_pid = self()
      call_count = :counters.new(1, [])
      records = [SinkFactory.s2_record()]

      Req.Test.stub(Client, fn conn ->
        count = :counters.get(call_count, 1)
        :counters.add(call_count, 1, 1)
        send(test_pid, {:attempt, count})

        conn
        |> Plug.Conn.put_status(429)
        |> Req.Test.json(%{"message" => "Too Many Requests"})
      end)

      # Should fail after retries
      assert {:error, error} = Client.append_records(@sink, records)
      assert error.service == :s2

      # Verify both attempts were made (1 initial + 1 retry)
      assert_receive {:attempt, 0}, 500
      assert_receive {:attempt, 1}, 1000
      refute_receive {:attempt, 2}, 100
    end
  end
end

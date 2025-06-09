defmodule Sequin.Telemetry.PosthogReporterTest do
  use ExUnit.Case, async: true

  alias Sequin.Telemetry.PosthogReporter

  setup do
    test_pid = self()

    Req.Test.stub(Sequin.Posthog, fn req ->
      {:ok, body, _} = Plug.Conn.read_body(req)
      json = Jason.decode!(body)
      send(test_pid, {:posthog_request, json})
      Req.Test.json(req, %{})
    end)

    :ok
  end

  # Add this private helper function
  defp start_reporter(opts) do
    default_opts = [
      name: Module.concat(__MODULE__, PosthogReporter),
      posthog_opts: [req_opts: [plug: {Req.Test, Sequin.Posthog}], api_key: "test_api_key"]
    ]

    opts = Keyword.merge(default_opts, opts)
    pid = start_supervised!({PosthogReporter, opts})
    Req.Test.allow(Sequin.Posthog, self(), pid)
    pid
  end

  # This test is flaky. Re-enable if you touch PosthogReporter.
  # test "buffers and flushes events in batches" do
  #   start_reporter(publish_interval: 50)

  #   # Send a few events
  #   :telemetry.execute(
  #     [:sequin, :posthog, :event],
  #     %{event: "test_event_1"},
  #     %{distinct_id: "user1", properties: %{key: "value1"}}
  #   )

  #   :telemetry.execute(
  #     [:sequin, :posthog, :event],
  #     %{event: "test_event_2"},
  #     %{distinct_id: "user2", properties: %{key: "value2"}}
  #   )

  #   # Wait for first flush
  #   assert_receive {:posthog_request, body}, 1000

  #   # Verify first batch
  #   assert %{"batch" => events} = body
  #   assert length(events) == 2

  #   [event1, event2] = events
  #   assert event1["event"] == "test_event_1"
  #   assert event1["distinct_id"] == "user1"
  #   assert event1["properties"]["key"] == "value1"

  #   assert event2["event"] == "test_event_2"
  #   assert event2["distinct_id"] == "user2"
  #   assert event2["properties"]["key"] == "value2"

  #   # Send more events
  #   :telemetry.execute(
  #     [:sequin, :posthog, :event],
  #     %{event: "test_event_3"},
  #     %{distinct_id: "user3", properties: %{key: "value3"}}
  #   )

  #   # Wait for second flush
  #   assert_receive {:posthog_request, body}, 1000

  #   # Verify second batch
  #   assert %{"batch" => events} = body
  #   assert length(events) == 1

  #   [event3] = events
  #   assert event3["event"] == "test_event_3"
  #   assert event3["distinct_id"] == "user3"
  #   assert event3["properties"]["key"] == "value3"
  # end

  test "flushes remaining events on termination" do
    start_reporter(publish_interval: :timer.minutes(1))

    :telemetry.execute(
      [:sequin, :posthog, :event],
      %{event: "final_event"},
      %{distinct_id: "user_final", properties: %{key: "value_final"}}
    )

    stop_supervised!(PosthogReporter)

    # Should receive the flush from terminate/2
    assert_receive {:posthog_request, body}, 2000

    assert %{"batch" => events} = body
    assert length(events) == 1

    [event] = events
    assert event["event"] == "final_event"
    assert event["properties"]["distinct_id"] == "user_final"
    assert event["properties"]["key"] == "value_final"
  end

  @tag skip: true
  test "merges consumer events by consumer_id" do
    start_reporter(publish_interval: 10)

    # Send multiple consumer_receive events for same consumer
    :telemetry.execute(
      [:sequin, :posthog, :event],
      %{event: "consumer_receive"},
      %{
        distinct_id: "00000000-0000-0000-0000-000000000000",
        properties: %{
          consumer_id: "consumer1",
          consumer_name: "test_consumer",
          message_count: 5,
          message_kind: :event,
          "$groups": %{account: "account1"}
        }
      }
    )

    :telemetry.execute(
      [:sequin, :posthog, :event],
      %{event: "consumer_receive"},
      %{
        distinct_id: "00000000-0000-0000-0000-000000000000",
        properties: %{
          consumer_id: "consumer1",
          consumer_name: "test_consumer",
          message_count: 3,
          message_kind: :event,
          "$groups": %{account: "account1"}
        }
      }
    )

    # Send consumer_ack events for same consumer
    :telemetry.execute(
      [:sequin, :posthog, :event],
      %{event: "consumer_ack"},
      %{
        distinct_id: "00000000-0000-0000-0000-000000000000",
        properties: %{
          consumer_id: "consumer1",
          consumer_name: "test_consumer",
          message_count: 2,
          message_kind: :event,
          "$groups": %{account: "account1"}
        }
      }
    )

    :telemetry.execute(
      [:sequin, :posthog, :event],
      %{event: "consumer_ack"},
      %{
        distinct_id: "00000000-0000-0000-0000-000000000000",
        properties: %{
          consumer_id: "consumer1",
          consumer_name: "test_consumer",
          message_count: 3,
          message_kind: :event,
          "$groups": %{account: "account1"}
        }
      }
    )

    # Send an unmergeable event
    :telemetry.execute(
      [:sequin, :posthog, :event],
      %{event: "other_event"},
      %{
        distinct_id: "user1",
        properties: %{key: "value1"}
      }
    )

    # Wait for flush
    assert_receive {:posthog_request, body}, 1000

    # Verify batch
    assert %{"batch" => events} = body
    # 1 merged receive + 1 merged ack + 1 unmergeable
    assert length(events) == 3

    merged_receive = Enum.find(events, &(&1["event"] == "consumer_receive"))
    assert merged_receive["properties"]["consumer_id"] == "consumer1"
    assert merged_receive["properties"]["event_count"] == 2
    # 5 + 3
    assert merged_receive["properties"]["message_count"] == 8
    assert merged_receive["properties"]["$groups"] == %{"account" => "account1"}

    merged_ack = Enum.find(events, &(&1["event"] == "consumer_ack"))
    assert merged_ack["properties"]["consumer_id"] == "consumer1"
    assert merged_ack["properties"]["event_count"] == 2
    # 2 + 3
    assert merged_ack["properties"]["message_count"] == 5
    assert merged_ack["properties"]["$groups"] == %{"account" => "account1"}

    unmergeable = Enum.find(events, &(&1["event"] == "other_event"))
    assert unmergeable["properties"]["key"] == "value1"
  end
end

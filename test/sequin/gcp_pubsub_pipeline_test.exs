defmodule Sequin.Runtime.GcpPubsubPipelineTest do
  use Sequin.DataCase, async: true

  alias Sequin.Consumers
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.FunctionsFactory
  alias Sequin.Functions.MiniElixir
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Sinks.Gcp.PubSub

  describe "events are sent to GCP PubSub" do
    setup do
      consumer = ConsumersFactory.insert_sink_consumer!(type: :gcp_pubsub, batch_size: 10)

      # Setup auth token expectation for all tests
      Req.Test.expect(PubSub, fn conn ->
        if conn.host == "oauth2.googleapis.com" do
          Req.Test.json(conn, %{"access_token" => "test_token"})
        end
      end)

      {:ok, %{consumer: consumer}}
    end

    test "events are sent to PubSub", %{consumer: consumer} do
      message = ConsumersFactory.consumer_message(message_kind: consumer.message_kind)

      # Mock PubSub publish
      Req.Test.expect(PubSub, fn conn ->
        assert conn.method == "POST"
        assert conn.host == "pubsub.googleapis.com"
        assert String.contains?(conn.request_path, ":publish")

        {:ok, body, _} = Plug.Conn.read_body(conn)
        body = body |> :zlib.gunzip() |> Jason.decode!()

        data = get_in(body, ["messages", Access.at(0), "data"])
        data = data |> Base.decode64!() |> Jason.decode!()
        assert Map.has_key?(data, "record")
        assert Map.has_key?(data, "metadata")

        Req.Test.json(conn, %{})
      end)

      start_pipeline!(consumer)

      ref = send_test_event(consumer, message)
      assert_receive {:ack, ^ref, [_successful], []}, 1_000
    end

    test "batched messages are processed together", %{consumer: consumer} do
      group_id = UUID.uuid4()
      message1 = ConsumersFactory.consumer_message(group_id: group_id)
      message2 = ConsumersFactory.consumer_message(group_id: group_id)

      # Mock PubSub publish and verify batch
      Req.Test.expect(PubSub, fn conn ->
        assert conn.method == "POST"
        assert conn.host == "pubsub.googleapis.com"

        {:ok, body, _} = Plug.Conn.read_body(conn)
        body = body |> :zlib.gunzip() |> Jason.decode!()
        assert length(body["messages"]) == 2

        Req.Test.json(conn, %{})
      end)

      start_pipeline!(consumer)

      ref = send_test_batch(consumer, [message1, message2])
      assert_receive {:ack, ^ref, [_message1, _message2], []}, 1_000
    end

    @tag capture_log: true
    test "failed PubSub publish results in failed events", %{consumer: consumer} do
      # Mock failed PubSub publish
      Req.Test.expect(PubSub, fn conn ->
        conn
        |> Plug.Conn.put_status(500)
        |> Req.Test.json(%{"error" => "Failed to publish to PubSub"})
      end)

      start_pipeline!(consumer)

      ref = send_test_event(consumer)
      assert_receive {:ack, ^ref, [], [_failed]}, 2_000
    end

    test "PubSub routing can assign topic", %{consumer: consumer} do
      router =
        FunctionsFactory.insert_routing_function!(
          account_id: consumer.account_id,
          function_attrs: [
            sink_type: :gcp_pubsub,
            body: """
            %{topic_id: "my_topic"}
            """
          ]
        )

      MiniElixir.create(router.id, router.function.code)

      {:ok, consumer} = Consumers.update_sink_consumer(consumer, %{routing_id: router.id, routing_mode: "dynamic"})

      message = ConsumersFactory.consumer_message(message_kind: consumer.message_kind)

      # Mock PubSub publish and verify topic
      Req.Test.expect(PubSub, fn conn ->
        assert conn.method == "POST"
        assert conn.host == "pubsub.googleapis.com"
        assert String.contains?(conn.request_path, "/my_topic:publish")

        Req.Test.json(conn, %{})
      end)

      start_pipeline!(consumer)

      ref = send_test_event(consumer, message)
      assert_receive {:ack, ^ref, [_successful], []}, 1_000
    end
  end

  defp start_pipeline!(consumer, opts \\ []) do
    {dummy_producer, _opts} = Keyword.pop(opts, :dummy_producer, true)

    opts = [
      consumer_id: consumer.id,
      test_pid: self()
    ]

    opts =
      if dummy_producer do
        Keyword.put(opts, :producer, Broadway.DummyProducer)
      else
        opts
      end

    start_supervised!({SinkPipeline, opts})
  end

  defp send_test_event(consumer, message \\ nil) do
    message = message || ConsumersFactory.consumer_message()

    Broadway.test_message(broadway(consumer), message)
  end

  defp send_test_batch(consumer, messages) do
    Broadway.test_batch(broadway(consumer), messages)
  end

  defp broadway(consumer) do
    SinkPipeline.via_tuple(consumer.id)
  end
end

defmodule Sequin.Runtime.HttpPushSqsPipelineTest do
  use Sequin.DataCase, async: true

  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Factory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Runtime.HttpPushSqsPipeline

  @sqs_config %{
    main_queue_url: "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
    region: "us-east-1",
    access_key_id: "test-access-key",
    secret_access_key: "test-secret-key"
  }

  describe "HttpPushSqsPipeline" do
    setup do
      account = Factory.AccountsFactory.insert_account!()
      http_endpoint = Factory.ConsumersFactory.insert_http_endpoint!(account_id: account.id)

      consumer =
        Factory.ConsumersFactory.insert_sink_consumer!(
          account_id: account.id,
          type: :http_push,
          sink: %{
            type: :http_push,
            http_endpoint_id: http_endpoint.id,
            batch: false,
            via_sqs: true
          },
          max_retry_count: Enum.random(5..10)
        )

      # Configure the pipeline for testing
      expect_application_get_env(4, fn
        :sequin, HttpPushSqsPipeline -> [sqs: @sqs_config, discards_disabled?: false]
      end)

      {:ok, %{consumer: consumer, http_endpoint: http_endpoint}}
    end

    test "processes SQS message and makes HTTP request", %{
      http_endpoint: http_endpoint,
      consumer: consumer
    } do
      test_pid = self()

      # Create a test event
      event =
        [consumer_id: consumer.id, action: :insert]
        |> ConsumersFactory.insert_consumer_event!()
        |> ConsumerEvent.map_from_struct()

      # Create a message that looks like what would come from SQS
      binary_data = :erlang.term_to_binary(event)
      message_body = Jason.encode!(%{data: Base.encode64(binary_data)})

      # Stub the HTTP request that the pipeline will make
      Req.Test.stub(HttpPushSqsPipeline, fn conn ->
        send(test_pid, {:req, conn})

        # Return a success response
        Req.Test.json(conn, %{})
      end)

      # Start the pipeline and process the message
      start_sqs_pipeline()
      ref = send_test_sqs_event(message_body)

      assert_receive {:req, conn}, 1_000

      # Verify request details
      assert conn.method == "POST"

      # Construct the URL from conn parts
      conn_url = "#{conn.scheme}://#{conn.host}#{conn.request_path}"
      assert conn_url == HttpEndpoint.url(http_endpoint)

      # Extract and verify the body
      {:ok, body, _} = Plug.Conn.read_body(conn)
      body_json = Jason.decode!(body)

      # Verify it's the transformed data
      assert body_json["action"] == "insert"
      assert body_json["record"] == event.data.record

      # Verify the message was acked
      assert_receive {:ack, ^ref, [_successful], []}, 1000
    end

    @tag capture_log: true
    test "handles HTTP request failures", %{consumer: consumer} do
      test_pid = self()

      # Create a test event
      event =
        [consumer_id: consumer.id, action: :insert]
        |> ConsumersFactory.insert_consumer_event!()
        |> ConsumerEvent.map_from_struct()

      binary_data = :erlang.term_to_binary(event)
      message_body = Jason.encode!(%{data: Base.encode64(binary_data)})

      # Stub the HTTP request to return a failure
      Req.Test.stub(HttpPushSqsPipeline, fn conn ->
        send(test_pid, {:req, conn})

        conn
        |> Plug.Conn.put_status(500)
        |> Req.Test.json(%{})
      end)

      # Start the pipeline and process the message
      start_sqs_pipeline()
      ref = send_test_sqs_event(message_body)

      # Verify the HTTP request was made
      assert_receive {:req, _conn}, 1000

      # Verify the message was failed
      assert_receive {:ack, ^ref, [], [_failed]}, 1000
    end

    @tag capture_log: true
    test "discards message if max retry count is exceeded", %{consumer: consumer} do
      test_pid = self()

      # Create a test event
      event =
        [consumer_id: consumer.id, action: :insert]
        |> ConsumersFactory.insert_consumer_event!()
        |> ConsumerEvent.map_from_struct()

      binary_data = :erlang.term_to_binary(event)
      message_body = Jason.encode!(%{data: Base.encode64(binary_data)})

      # Stub the HTTP request to return a failure
      Req.Test.stub(HttpPushSqsPipeline, fn conn ->
        send(test_pid, {:req, conn})

        conn
        |> Plug.Conn.put_status(500)
        |> Req.Test.json(%{})
      end)

      # Start the pipeline and process the message
      start_sqs_pipeline()

      ref =
        send_test_sqs_event(message_body,
          metadata: %{attributes: %{"approximate_receive_count" => consumer.max_retry_count + 2}}
        )

      # Verify the HTTP request was made
      assert_receive {:req, _conn}, 1000

      # Verify the message was acked
      assert_receive {:ack, ^ref, [_successful], []}, 1000
    end
  end

  @sqs_pipeline_name Module.concat(__MODULE__, TestPipeline)
  defp start_sqs_pipeline do
    # Configure the Broadway pipeline with a DummyProducer
    pipeline_config = [
      name: @sqs_pipeline_name,
      producer_mod: Broadway.DummyProducer,
      test_pid: self(),
      queue_url: @sqs_config.main_queue_url,
      queue_kind: :main
    ]

    # Start the pipeline and process the message
    start_supervised!({HttpPushSqsPipeline, pipeline_config})
    :ok
  end

  defp send_test_sqs_event(event, opts \\ []) do
    metadata = Keyword.get(opts, :metadata, %{attributes: %{"approximate_receive_count" => 1}})
    Broadway.test_message(@sqs_pipeline_name, event, metadata: metadata)
  end
end

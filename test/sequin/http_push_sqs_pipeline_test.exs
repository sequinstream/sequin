defmodule Sequin.Runtime.HttpPushSqsPipelineTest do
  use Sequin.DataCase, async: true

  alias Sequin.Aws.HttpClient
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Factory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Transforms

  @via_config %{
    queue_url: "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
    region: "us-east-1",
    access_key_id: "test-access-key",
    secret_access_key: "test-secret-key"
  }

  setup do
    account = Factory.AccountsFactory.insert_account!()
    http_endpoint = Factory.ConsumersFactory.insert_http_endpoint!(account_id: account.id)

    stub_application_get_env(fn
      :sequin, Sequin.Consumers.HttpPushSink -> [via: @via_config]
      app, key -> Application.get_env(app, key)
    end)

    consumer =
      Factory.ConsumersFactory.insert_sink_consumer!(
        account_id: account.id,
        type: :http_push,
        sink: %{
          type: :http_push,
          http_endpoint_id: http_endpoint.id,
          batch: false
        },
        message_kind: :event
      )

    consumer = Repo.preload(consumer, [:sequence, :transform, :routing])

    # Mock AWS config if necessary using Application.put_env, e.g., for credentials
    # For now, we rely on Req.Test.stub for SQS interaction.

    {:ok, %{consumer: consumer, http_endpoint: http_endpoint, account: account}}
  end

  describe "HttpPushSink with :via SQS routing" do
    test "sends message to SQS with correct payload and does not call HTTP endpoint directly",
         %{consumer: consumer, http_endpoint: http_endpoint} do
      test_pid = self()

      event = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id, action: :insert)

      Req.Test.stub(HttpClient, fn %Plug.Conn{} = conn ->
        send(test_pid, {:req, conn})

        # Mock successful SQS response
        Req.Test.json(conn, %{"Successful" => [%{"Id" => "1", "MessageId" => "test-msg-id"}], "Failed" => []})
      end)

      # Send a test event into the pipeline
      start_pipeline!(consumer)
      ref = send_test_event(consumer, event)

      # Assert SQS request was made
      assert_receive {:req, %Plug.Conn{} = conn}, 1_000

      #  # Assertions for the SQS request
      # 1. Correct SQS service endpoint URL (host and scheme)
      assert @via_config.queue_url =~ conn.host

      #   # 2. Correct HTTP method (POST for SQS)
      assert conn.method == "POST"

      assert {_, content_type} = List.keyfind(conn.req_headers, "content-type", 0)
      assert content_type == "application/x-amz-json-1.0"

      decoded_body = conn |> Plug.Conn.read_body() |> elem(1) |> Jason.decode!()

      assert %{
               "Entries" => [
                 %{
                   "Id" => _id,
                   "MessageAttributes" => %{},
                   "MessageBody" => envelope_json
                 }
               ]
             } = decoded_body

      envelope = Jason.decode!(envelope_json)

      assert envelope["request"]["method"] == "POST"
      assert envelope["request"]["base_url"] == HttpEndpoint.url(http_endpoint)
      assert envelope["request"]["url"] == (consumer.sink.http_endpoint_path || "")
      # Ensure headers from http_endpoint are present
      expected_headers = Map.merge(http_endpoint.headers || %{}, http_endpoint.encrypted_headers || %{})
      assert envelope["request"]["headers"] == expected_headers

      # Envelope.metadata part
      assert envelope["metadata"]["consumer_id"] == consumer.id
      assert envelope["metadata"]["max_retry_count"] == consumer.max_retry_count
      assert envelope["metadata"]["request_timeout"] == 30_000

      original_payload_in_sqs_message = envelope["request"]["json"]

      expected_transformed_payload =
        consumer
        |> Transforms.Message.to_external(event)
        |> Sequin.Map.stringify_keys()

      expected_transformed_metadata = Sequin.Map.stringify_keys(expected_transformed_payload["metadata"])

      assert_maps_equal(original_payload_in_sqs_message, expected_transformed_payload, ["action", "changes", "record"])

      assert_maps_equal(original_payload_in_sqs_message["metadata"], expected_transformed_metadata, [
        "commit_lsn",
        "table_schema",
        "table_name"
      ])

      # Assert ack for the message (indicating successful handoff to SQS)
      assert_receive {:ack, ^ref, [_acked_message], []}, 1_000
    end
  end

  defp start_pipeline!(consumer, opts \\ []) do
    opts =
      Keyword.merge(
        [
          consumer: consumer,
          test_pid: self(),
          producer: Broadway.DummyProducer
        ],
        opts
      )

    start_supervised!({SinkPipeline, opts})
  end

  defp send_test_event(consumer, event) do
    Broadway.test_message(broadway(consumer), event, metadata: %{})
  end

  defp broadway(consumer) do
    SinkPipeline.via_tuple(consumer.id)
  end
end

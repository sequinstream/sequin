defmodule Sequin.Runtime.ElasticsearchPipelineTest do
  use Sequin.DataCase, async: true

  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.FunctionsFactory
  alias Sequin.Functions.MiniElixir
  alias Sequin.Runtime.SinkPipeline

  describe "elasticsearch pipeline" do
    setup do
      account = AccountsFactory.insert_account!()

      transform =
        FunctionsFactory.insert_function!(
          account_id: account.id,
          function_type: :transform,
          function_attrs: %{body: "record"}
        )

      MiniElixir.create(transform.id, transform.function.code)

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          account_id: account.id,
          type: :elasticsearch,
          transform_id: transform.id
        )

      {:ok, %{consumer: consumer}}
    end

    test "requests use compression", %{consumer: consumer} do
      test_pid = self()

      message =
        ConsumersFactory.consumer_message(
          consumer_id: consumer.id,
          data:
            ConsumersFactory.consumer_message_data(
              action: :insert,
              record: %{"name" => "test-name"}
            )
        )

      adapter = fn request ->
        send(test_pid, {:elasticsearch_request, request})

        # Verify body is compressed by attempting to decompress it
        body = :zlib.gunzip(request.body)
        assert String.contains?(body, "test-name")

        {request,
         Req.Response.new(
           status: 200,
           body: %{"items" => [%{"index" => %{"_id" => "1", "status" => 201}}]}
         )}
      end

      start_pipeline!(consumer, adapter)

      send_test_batch(consumer, [message])

      assert_receive {:elasticsearch_request, request}, 3000
      assert request.method == :post
      assert String.contains?(to_string(request.url), "/_bulk")

      assert_receive {:ack, _ref, [success], []}, 3000
      assert success.data == message
    end

    test "one message is published", %{consumer: consumer} do
      test_pid = self()

      message =
        ConsumersFactory.consumer_message(
          consumer_id: consumer.id,
          data: ConsumersFactory.consumer_message_data(action: :insert)
        )

      adapter = fn request ->
        send(test_pid, {:elasticsearch_request, request})

        assert request.method == :post
        assert String.contains?(to_string(request.url), "/_bulk")

        {request,
         Req.Response.new(
           status: 200,
           body: %{"items" => [%{"index" => %{"_id" => "1", "status" => 201}}]}
         )}
      end

      start_pipeline!(consumer, adapter)

      send_test_batch(consumer, [message])

      assert_receive {:elasticsearch_request, _request}, 3000

      assert_receive {:ack, _ref, [success], []}, 3000
      assert success.data == message
    end

    test "multiple messages are batched", %{consumer: consumer} do
      test_pid = self()

      messages =
        for _ <- 1..3 do
          ConsumersFactory.insert_consumer_message!(
            consumer_id: consumer.id,
            data: ConsumersFactory.consumer_message_data(action: :insert)
          )
        end

      adapter = fn request ->
        send(test_pid, {:elasticsearch_request, request})

        {request,
         Req.Response.new(
           status: 200,
           body: %{
             "items" => [
               %{"index" => %{"_id" => "1", "status" => 201}},
               %{"index" => %{"_id" => "2", "status" => 201}},
               %{"index" => %{"_id" => "3", "status" => 201}}
             ]
           }
         )}
      end

      start_pipeline!(consumer, adapter)

      send_test_batch(consumer, messages)

      assert_receive {:elasticsearch_request, _request}, 3000

      assert_receive {:ack, _ref, successful, []}, 3000
      assert length(successful) == 3
    end
  end

  defp start_pipeline!(consumer, req_adapter) do
    start_supervised!(
      {SinkPipeline,
       [
         consumer_id: consumer.id,
         producer: Broadway.DummyProducer,
         test_pid: self(),
         req_opts: [adapter: req_adapter]
       ]}
    )
  end

  defp send_test_batch(consumer, events) do
    Broadway.test_batch(broadway(consumer), events)
  end

  defp broadway(consumer) do
    SinkPipeline.via_tuple(consumer.id)
  end
end

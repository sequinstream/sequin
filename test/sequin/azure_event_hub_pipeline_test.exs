defmodule Sequin.Runtime.AzureEventHubPipelineTest do
  use Sequin.DataCase, async: true

  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.FunctionsFactory
  alias Sequin.Functions.MiniElixir
  alias Sequin.Runtime.SinkPipeline

  describe "azure event hub pipeline" do
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
          type: :azure_event_hub,
          message_kind: :event,
          transform_id: transform.id
        )

      {:ok, %{consumer: consumer}}
    end

    test "one message is published", %{consumer: consumer} do
      test_pid = self()

      message =
        ConsumersFactory.consumer_message(
          consumer_id: consumer.id,
          message_kind: consumer.message_kind,
          data:
            ConsumersFactory.consumer_message_data(
              message_kind: consumer.message_kind,
              action: :insert
            )
        )

      adapter = fn request ->
        send(test_pid, {:azure_event_hub_request, request})

        assert request.method == :post
        assert request.url.path =~ "/messages"

        {request, Req.Response.new(status: 201)}
      end

      start_pipeline!(consumer, adapter)

      send_test_batch(consumer, [message])

      assert_receive {:azure_event_hub_request, _request}, 3000

      assert_receive {:ack, _ref, [success], []}, 3000
      assert success.data == message
    end

    test "multiple messages are batched", %{consumer: consumer} do
      test_pid = self()

      messages =
        for _ <- 1..3 do
          ConsumersFactory.insert_consumer_message!(
            consumer_id: consumer.id,
            message_kind: consumer.message_kind,
            data:
              ConsumersFactory.consumer_message_data(
                message_kind: consumer.message_kind,
                action: :insert
              )
          )
        end

      adapter = fn request ->
        send(test_pid, {:azure_event_hub_request, request})

        {request, Req.Response.new(status: 201)}
      end

      start_pipeline!(consumer, adapter)

      send_test_batch(consumer, messages)

      assert_receive {:azure_event_hub_request, _request}, 3000

      assert_receive {:ack, _ref, successful, []}, 3000
      assert length(successful) == 3
    end
  end

  describe "azure event hub routing" do
    setup do
      account = AccountsFactory.insert_account!()

      transform =
        FunctionsFactory.insert_function!(
          account_id: account.id,
          function_type: :transform,
          function_attrs: %{body: "record"}
        )

      routing =
        FunctionsFactory.insert_function!(
          account_id: account.id,
          function_type: :routing,
          function_attrs: %{
            body: """
            if record["type"] == "notification" do
              %{event_hub_name: "notifications"}
            else
              %{event_hub_name: "default"}
            end
            """
          }
        )

      MiniElixir.create(transform.id, transform.function.code)
      MiniElixir.create(routing.id, routing.function.code)

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          account_id: account.id,
          type: :azure_event_hub,
          message_kind: :event,
          transform_id: transform.id,
          routing_mode: "dynamic",
          routing_id: routing.id
        )

      {:ok, %{consumer: consumer}}
    end

    test "routing can override event hub name", %{consumer: consumer} do
      test_pid = self()

      message =
        ConsumersFactory.consumer_message(
          consumer_id: consumer.id,
          message_kind: consumer.message_kind,
          data:
            ConsumersFactory.consumer_message_data(
              message_kind: consumer.message_kind,
              action: :insert,
              record: %{"type" => "notification"}
            )
        )

      adapter = fn request ->
        send(test_pid, {:azure_event_hub_request, request})

        assert request.url.path =~ "/notifications/messages"

        {request, Req.Response.new(status: 201)}
      end

      start_pipeline!(consumer, adapter)

      send_test_batch(consumer, [message])

      assert_receive {:azure_event_hub_request, _request}, 3000

      assert_receive {:ack, _ref, [success], []}, 3000
      assert success.data == message
    end

    test "multiple messages are routed to different event hubs", %{consumer: consumer} do
      test_pid = self()

      messages = [
        ConsumersFactory.consumer_message(
          consumer_id: consumer.id,
          message_kind: consumer.message_kind,
          data:
            ConsumersFactory.consumer_message_data(
              message_kind: consumer.message_kind,
              action: :insert,
              record: %{"type" => "notification"}
            )
        ),
        ConsumersFactory.consumer_message(
          consumer_id: consumer.id,
          message_kind: consumer.message_kind,
          data:
            ConsumersFactory.consumer_message_data(
              message_kind: consumer.message_kind,
              action: :insert,
              record: %{"type" => "other"}
            )
        )
      ]

      adapter = fn request ->
        send(test_pid, {:azure_event_hub_request, request})

        {request, Req.Response.new(status: 201)}
      end

      start_pipeline!(consumer, adapter)

      send_test_batch(consumer, messages)

      # Should receive two separate requests, one for each event hub
      assert_receive {:azure_event_hub_request, request1}, 3000
      assert_receive {:azure_event_hub_request, request2}, 3000

      assert Enum.any?([request1, request2], fn request -> request.url.path =~ "/notifications/messages" end)
      assert Enum.any?([request1, request2], fn request -> request.url.path =~ "/default/messages" end)

      # Should receive two acks, one for each batch
      assert_receive {:ack, _ref, [_successful], []}, 3000
      assert_receive {:ack, _ref, [_successful], []}, 3000
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

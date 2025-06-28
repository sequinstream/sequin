defmodule Sequin.Runtime.TypesensePipelineTest do
  use Sequin.DataCase, async: true

  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.FunctionsFactory
  alias Sequin.Functions.MiniElixir
  alias Sequin.Runtime.SinkPipeline

  describe "typesense pipeline" do
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
          type: :typesense,
          message_kind: :event,
          transform_id: transform.id
        )

      {:ok, %{consumer: consumer}}
    end

    test "one message is indexed", %{consumer: consumer} do
      test_pid = self()

      message =
        ConsumersFactory.consumer_message(
          consumer_id: consumer.id,
          message_kind: consumer.message_kind,
          data:
            ConsumersFactory.consumer_message_data(
              message_kind: consumer.message_kind,
              action: Enum.random([:insert, :update])
            )
        )

      adapter = fn request ->
        send(test_pid, {:typesense_request, request})

        assert request.method == :post
        assert request.url.path =~ "documents"
        assert request.url.query == "action=emplace"

        {request, Req.Response.new(status: 200)}
      end

      start_pipeline!(consumer, adapter)

      send_test_batch(consumer, [message])

      assert_receive {:typesense_request, _sink}, 3000

      assert_receive {:ack, _ref, [success], []}, 3000
      assert success.data == message
    end

    test "one message is deleted", %{consumer: consumer} do
      test_pid = self()

      message =
        ConsumersFactory.consumer_message(
          consumer_id: consumer.id,
          message_kind: consumer.message_kind,
          data:
            ConsumersFactory.consumer_message_data(
              message_kind: consumer.message_kind,
              action: :delete,
              record: %{"id" => 123}
            )
        )

      adapter = fn request ->
        send(test_pid, {:typesense_request, request})

        assert request.method == :delete
        assert request.url.path =~ "documents"
        assert request.url.query == "ignore_not_found=true"

        {request, Req.Response.new(status: 200)}
      end

      start_pipeline!(consumer, adapter)

      send_test_batch(consumer, [message])

      assert_receive {:typesense_request, _sink}, 3000

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
                action: Enum.random([:insert, :update])
              )
          )
        end

      adapter = fn request ->
        send(test_pid, {:typesense_request, request})

        {request,
         Req.Response.new(
           status: 200,
           body: ~s({"success": true}\n{"success": true}\n{"success": true})
         )}
      end

      start_pipeline!(consumer, adapter)

      send_test_batch(consumer, messages)

      assert_receive {:typesense_request, _sink}, 3000

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

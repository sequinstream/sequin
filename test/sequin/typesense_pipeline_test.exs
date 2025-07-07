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
        assert request.url.path =~ "collections/#{consumer.sink.collection_name}/documents"
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
        assert request.url.path =~ "collections/#{consumer.sink.collection_name}/documents"
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

    test "pipeline passes the right document_id to delete_document if id is an atom", %{consumer: consumer} do
      test_pid = self()

      message =
        ConsumersFactory.consumer_message(
          consumer_id: consumer.id,
          message_kind: consumer.message_kind,
          data:
            ConsumersFactory.consumer_message_data(
              message_kind: consumer.message_kind,
              action: :delete,
              record: %{id: 12_345}
            )
        )

      adapter = fn request ->
        send(test_pid, {:typesense_request, request})

        assert request.method == :delete
        assert request.url.path =~ "collections/#{consumer.sink.collection_name}/documents/12345"
        assert request.url.query == "ignore_not_found=true"

        {request, Req.Response.new(status: 200)}
      end

      start_pipeline!(consumer, adapter)

      send_test_batch(consumer, [message])

      assert_receive {:typesense_request, _sink}, 3000

      assert_receive {:ack, _ref, [success], []}, 3000
      assert success.data == message
    end
  end

  describe "typesense routing" do
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
            if record["deleted_at"] != nil do
              %{collection_name: "router-collection", action: "delete"}
            else
              %{collection_name: "router-collection"}
            end
            """
          }
        )

      MiniElixir.create(transform.id, transform.function.code)
      MiniElixir.create(routing.id, routing.function.code)

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          account_id: account.id,
          type: :typesense,
          message_kind: :event,
          transform_id: transform.id,
          routing_mode: "dynamic",
          routing_id: routing.id
        )

      {:ok, %{consumer: consumer}}
    end

    test "routing can override index name for single document index", %{consumer: consumer} do
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
        assert request.url.path =~ "collections/router-collection/documents"
        assert request.url.query == "action=emplace"

        {request, Req.Response.new(status: 200)}
      end

      start_pipeline!(consumer, adapter)

      send_test_batch(consumer, [message])

      assert_receive {:typesense_request, _sink}, 3000

      assert_receive {:ack, _ref, [success], []}, 3000
      assert success.data == message
    end

    test "routing can override index name for deletes", %{consumer: consumer} do
      test_pid = self()

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
        assert request.url.path =~ "collections/router-collection/documents"
        assert request.url.query == "ignore_not_found=true"

        {request, Req.Response.new(status: 200)}
      end

      start_pipeline!(consumer, adapter)
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

    test "router can change action to delete based on deleted_at field", %{consumer: consumer} do
      test_pid = self()

      message =
        ConsumersFactory.consumer_message(
          consumer_id: consumer.id,
          message_kind: consumer.message_kind,
          data:
            ConsumersFactory.consumer_message_data(
              message_kind: consumer.message_kind,
              action: :insert,
              record: %{"id" => 123, "deleted_at" => "2024-01-01T00:00:00Z"}
            )
        )

      adapter = fn request ->
        send(test_pid, {:typesense_request, request})

        assert request.method == :delete
        assert request.url.path =~ "collections/router-collection/documents/123"
        assert request.url.query == "ignore_not_found=true"

        {request, Req.Response.new(status: 200)}
      end

      start_pipeline!(consumer, adapter)

      send_test_batch(consumer, [message])

      assert_receive {:typesense_request, _sink}, 3000

      assert_receive {:ack, _ref, [success], []}, 3000
      assert success.data == message
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

defmodule Sequin.Runtime.RabbitMqPipelineTest do
  use Sequin.DataCase, async: true

  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.FunctionsFactory
  alias Sequin.Functions.MiniElixir
  alias Sequin.Runtime.Routing.RoutedMessage
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Sinks.RabbitMqMock

  describe "rabbitmq pipeline" do
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
          type: :rabbitmq,
          message_kind: :event,
          transform_id: transform.id
        )

      {:ok, %{consumer: consumer}}
    end

    test "one message is published", %{consumer: consumer} do
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

      Mox.expect(RabbitMqMock, :send_messages, fn _consumer, routed_messages ->
        assert length(routed_messages) == 1
        %RoutedMessage{routing_info: routing_info} = List.first(routed_messages)
        assert routing_info.exchange == consumer.sink.exchange
        :ok
      end)

      start_pipeline!(consumer)
      send_test_batch(consumer, [message])

      assert_receive {:ack, _ref, [success], []}, 3000
      assert success.data == message
    end

    test "multiple messages are batched", %{consumer: consumer} do
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

      Mox.expect(RabbitMqMock, :send_messages, fn _consumer, routed_messages ->
        assert length(routed_messages) == 3
        :ok
      end)

      start_pipeline!(consumer)
      send_test_batch(consumer, messages)

      assert_receive {:ack, _ref, successful, []}, 3000
      assert length(successful) == 3
    end
  end

  describe "rabbitmq routing" do
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
            %{
              exchange: "amq.fanout",
              routing_key: "\#{metadata.database_name}.\#{metadata.table_schema}.\#{metadata.table_name}.\#{action}",
              headers: %{"Idempotency-Key" => metadata.idempotency_key}
            }
            """
          }
        )

      MiniElixir.create(transform.id, transform.function.code)
      MiniElixir.create(routing.id, routing.function.code)

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          account_id: account.id,
          type: :rabbitmq,
          message_kind: :event,
          transform_id: transform.id,
          routing_mode: "dynamic",
          routing_id: routing.id
        )

      {:ok, %{consumer: consumer}}
    end

    test "routing can override exchange and add headers", %{consumer: consumer} do
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

      Mox.expect(RabbitMqMock, :send_messages, fn _consumer, routed_messages ->
        assert length(routed_messages) == 1

        %RoutedMessage{transformed_message: transformed_message, routing_info: routing_info} =
          List.first(routed_messages)

        assert transformed_message == message.data.record

        metadata = message.data.metadata
        assert routing_info.exchange == "amq.fanout"

        assert routing_info.routing_key ==
                 "#{metadata.database_name}.#{metadata.table_schema}.#{metadata.table_name}.insert"

        assert routing_info.headers == %{"Idempotency-Key" => metadata.idempotency_key}
        :ok
      end)

      start_pipeline!(consumer)
      send_test_batch(consumer, [message])

      assert_receive {:ack, _ref, [success], []}, 3000
      assert success.data == message
    end

    test "routing can change exchange based on deleted_at field", %{consumer: consumer} do
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

      Mox.expect(RabbitMqMock, :send_messages, fn _consumer, routed_messages ->
        assert length(routed_messages) == 1

        %RoutedMessage{transformed_message: transformed_message, routing_info: routing_info} =
          List.first(routed_messages)

        assert transformed_message == message.data.record

        metadata = message.data.metadata
        assert routing_info.exchange == "amq.fanout"

        assert routing_info.routing_key ==
                 "#{metadata.database_name}.#{metadata.table_schema}.#{metadata.table_name}.insert"

        assert routing_info.headers == %{"Idempotency-Key" => metadata.idempotency_key}
        :ok
      end)

      start_pipeline!(consumer)
      send_test_batch(consumer, [message])

      assert_receive {:ack, _ref, [success], []}, 3000
      assert success.data == message
    end
  end

  defp start_pipeline!(consumer) do
    start_supervised!(
      {SinkPipeline,
       [
         consumer_id: consumer.id,
         producer: Broadway.DummyProducer,
         test_pid: self()
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

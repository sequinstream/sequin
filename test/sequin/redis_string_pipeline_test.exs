defmodule Sequin.Runtime.RedisStringPipelineTest do
  use Sequin.DataCase, async: true

  alias Sequin.Consumers
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Functions.MiniElixir
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Sinks.RedisMock

  describe "redis string pipeline" do
    setup do
      account = AccountsFactory.insert_account!()

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          account_id: account.id,
          batch_size: 10,
          type: :redis_string,
          sink: %{type: :redis_string},
          message_kind: :event
        )

      {:ok, %{consumer: consumer}}
    end

    test "sends message to redis", %{consumer: consumer} do
      message = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id)

      start_pipeline!(consumer)

      ref = send_test_message(consumer, message)

      Mox.expect(RedisMock, :set_messages, fn _, _ -> :ok end)

      assert_receive {:ack, ^ref, [%{data: %{data: data}}], []}, 1_000
      assert data == message.data
    end

    test "sends messages to redis", %{consumer: consumer} do
      message1 = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id)
      message2 = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id)

      start_pipeline!(consumer)

      ref = send_test_batch(consumer, [message1, message2])

      Mox.expect(RedisMock, :set_messages, fn _, _ -> :ok end)

      assert_receive {:ack, ^ref, [%{data: %{data: data1}}, %{data: %{data: data2}}], []}, 1_000
      assert data1 == message1.data
      assert data2 == message2.data
    end

    test "sends message with expire ms set with px", %{consumer: consumer} do
      {:ok, consumer} = Consumers.update_sink_consumer(consumer, %{sink: %{expire_ms: 1000}})
      message = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id)

      start_pipeline!(consumer)

      ref = send_test_message(consumer, message)

      Mox.expect(RedisMock, :set_messages, fn _, [%{key: _, value: _, expire_ms: 1000}] -> :ok end)

      assert_receive {:ack, ^ref, [%{data: %{data: data}}], []}, 1_000
      assert data == message.data
    end

    test "transforms messages with function transforms", %{consumer: consumer} do
      transform_code = """
      def transform(action, record, changes, metadata) do
        record["column"]
      end
      """

      assert {:ok, transform} =
               Consumers.create_function(consumer.account_id, %{
                 name: "test",
                 function: %{
                   type: "transform",
                   code: transform_code
                 }
               })

      assert MiniElixir.create(transform.id, transform.function.code)
      {:ok, consumer} = Consumers.update_sink_consumer(consumer, %{transform_id: transform.id})

      message =
        ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id)

      column_value = message.data.record["column"]

      start_pipeline!(consumer)

      ref = send_test_message(consumer, message)

      Mox.expect(RedisMock, :set_messages, fn _, [%{key: _, value: ^column_value}] -> :ok end)

      assert_receive {:ack, ^ref, [%{data: %{data: data}}], []}, 1_000
      assert data == message.data
    end

    test "routes messages with routing transforms", %{consumer: consumer} do
      routing_code = """
      def transform(action, record, changes, metadata) do
        %{key: "custom:\#{metadata.table_name}:\#{record["id"]}"}
      end
      """

      assert {:ok, routing} =
               Consumers.create_function(consumer.account_id, %{
                 name: "routing_test",
                 function: %{
                   type: "routing",
                   sink_type: :redis_string,
                   code: routing_code
                 }
               })

      assert MiniElixir.create(routing.id, routing.function.code)
      {:ok, consumer} = Consumers.update_sink_consumer(consumer, %{routing_id: routing.id, routing_mode: "dynamic"})

      message =
        ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id)

      custom_key = "custom:#{message.data.metadata.table_name}:#{message.data.record["id"]}"

      start_pipeline!(consumer)

      ref = send_test_message(consumer, message)

      Mox.expect(RedisMock, :set_messages, fn _, [%{key: ^custom_key, value: _}] -> :ok end)

      assert_receive {:ack, ^ref, [%{data: %{data: data}}], []}, 1_000
      assert data == message.data
    end

    test "handles delete actions by sending del command to redis", %{consumer: consumer} do
      message =
        ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id)

      # Delete message
      message = put_in(message.data.action, :delete)

      start_pipeline!(consumer)

      ref = send_test_message(consumer, message)

      Mox.expect(RedisMock, :set_messages, fn _, [%{action: :del}] -> :ok end)

      assert_receive {:ack, ^ref, [%{data: %{data: data}}], []}, 1_000
      assert data == message.data
    end
  end

  defp start_pipeline!(consumer) do
    start_supervised!(
      {SinkPipeline,
       [
         consumer: consumer,
         producer: Broadway.DummyProducer,
         test_pid: self()
       ]}
    )
  end

  defp send_test_message(consumer, message) do
    Broadway.test_message(broadway(consumer), message)
  end

  defp send_test_batch(consumer, messages) do
    Broadway.test_batch(broadway(consumer), messages)
  end

  defp broadway(consumer) do
    SinkPipeline.via_tuple(consumer.id)
  end
end

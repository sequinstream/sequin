defmodule Sequin.RedisStreamPipelineTest do
  use Sequin.DataCase, async: true

  alias Sequin.Consumers
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Functions.MiniElixir
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Sinks.RedisMock

  describe "redis stream pipeline" do
    setup do
      account = AccountsFactory.insert_account!()

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          account_id: account.id,
          batch_size: 10,
          type: :redis_stream,
          sink: %{type: :redis_stream, stream_key: "test-stream"},
          message_kind: :event
        )

      {:ok, %{consumer: consumer}}
    end

    test "sends message to redis", %{consumer: consumer} do
      message = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id)

      start_pipeline!(consumer)

      ref = send_test_message(consumer, message)

      Mox.expect(RedisMock, :send_messages, fn _, _ -> :ok end)

      assert_receive {:ack, ^ref, [%{data: %{data: data}}], []}, 1_000
      assert data == message.data
    end

    test "sends messages to redis", %{consumer: consumer} do
      message1 = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id)
      message2 = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id)

      start_pipeline!(consumer)

      ref = send_test_batch(consumer, [message1, message2])

      Mox.expect(RedisMock, :send_messages, fn _, _ -> :ok end)

      assert_receive {:ack, ^ref, [%{data: %{data: data1}}, %{data: %{data: data2}}], []}, 1_000
      assert data1 == message1.data
      assert data2 == message2.data
    end

    test "transforms messages with function transforms", %{consumer: consumer} do
      transform_code = """
      def transform(action, record, changes, metadata) do
        %{"transformed" => record["column"]}
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

      message = ConsumersFactory.insert_consumer_event!(consumer_id: consumer.id)

      column_value = message.data.record["column"]

      start_pipeline!(consumer)

      ref = send_test_message(consumer, message)

      Mox.expect(RedisMock, :send_messages, fn _, [%{data: %{"transformed" => ^column_value}}] -> :ok end)

      assert_receive {:ack, ^ref, [%{data: %{data: data}}], []}, 1_000
      assert data == message.data
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

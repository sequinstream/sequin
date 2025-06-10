defmodule Sequin.SinkPipelineTest do
  use Sequin.DataCase, async: false

  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.FunctionsFactory
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Runtime.SinkPipelineMock

  describe "filtering messages" do
    setup do
      stub(SinkPipelineMock, :init, fn context, _ -> context end)
      stub(SinkPipelineMock, :processors_config, fn _ -> [] end)
      stub(SinkPipelineMock, :batchers_config, fn _ -> [] end)
      stub(SinkPipelineMock, :handle_message, fn message, context -> {:ok, message, context} end)

      stub(SinkPipelineMock, :handle_batch, fn _batch_name, messages, _batch_info, context -> {:ok, messages, context} end)

      stub(SinkPipelineMock, :apply_routing, fn _consumer, rinfo -> rinfo end)
      :ok
    end

    test "messages are not filtered when no filter is set" do
      consumer = ConsumersFactory.insert_sink_consumer!()

      message =
        ConsumersFactory.insert_consumer_message!(
          consumer_id: consumer.id,
          message_kind: consumer.message_kind
        )

      start_pipeline!(consumer)

      ref = send_test_message(consumer, message)

      assert_receive {:ack, ^ref, [acknowledged], []}
      assert acknowledged.data == message
    end

    test "messages are not filtered when the filter returns true" do
      filter_function =
        FunctionsFactory.insert_filter_function!(
          function_attrs: [
            body: "true"
          ]
        )

      consumer = ConsumersFactory.insert_sink_consumer!(filter_id: filter_function.id)

      message =
        ConsumersFactory.insert_consumer_message!(
          consumer_id: consumer.id,
          message_kind: consumer.message_kind
        )

      start_pipeline!(consumer)

      ref = send_test_message(consumer, message)

      assert_receive {:ack, ^ref, [acknowledged], []}
      assert acknowledged.data == message
      refute acknowledged.batcher == :filtered_messages
    end

    test "messages are filtered when the filter returns false" do
      filter_function =
        FunctionsFactory.insert_filter_function!(
          function_attrs: [
            body: "false"
          ]
        )

      consumer = ConsumersFactory.insert_sink_consumer!(filter_id: filter_function.id)

      message =
        ConsumersFactory.insert_consumer_message!(
          consumer_id: consumer.id,
          message_kind: consumer.message_kind
        )

      start_pipeline!(consumer)

      ref = send_test_message(consumer, message)

      assert_receive {SinkPipeline, :filtered_messages, [filtered]}
      assert filtered.data == message

      assert_receive {:ack, ^ref, [acknowledged], []}
      assert acknowledged.data == message
      assert acknowledged.batcher == :filtered_messages
    end

    @tag :capture_log
    test "messages are marked failed when a filter raises an error" do
      filter_function =
        FunctionsFactory.insert_filter_function!(
          function_attrs: [
            body: "1 = 2"
          ]
        )

      consumer = ConsumersFactory.insert_sink_consumer!(filter_id: filter_function.id)

      message =
        ConsumersFactory.insert_consumer_message!(
          consumer_id: consumer.id,
          message_kind: consumer.message_kind
        )

      start_pipeline!(consumer)

      ref = send_test_message(consumer, message)

      assert_receive {:ack, ^ref, [], [failed]}
      assert failed.data == message
    end
  end

  defp start_pipeline!(consumer) do
    start_supervised!(
      {SinkPipeline,
       [
         consumer_id: consumer.id,
         producer: Broadway.DummyProducer,
         test_pid: self(),
         pipeline_mod: SinkPipelineMock
       ]}
    )
  end

  defp send_test_message(consumer, message) do
    Broadway.test_message(broadway(consumer), message)
  end

  # defp send_test_batch(consumer, messages) do
  #   Broadway.test_batch(broadway(consumer), messages)
  # end

  defp broadway(consumer) do
    SinkPipeline.via_tuple(consumer.id)
  end
end

defmodule Sequin.Runtime.KafkaPipelineTest do
  use Sequin.DataCase, async: true

  alias Sequin.Consumers
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.FunctionsFactory
  alias Sequin.Functions.MiniElixir
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Sinks.KafkaMock

  describe "events are sent to Kafka" do
    setup do
      consumer = ConsumersFactory.insert_sink_consumer!(type: :kafka, batch_size: 10)

      {:ok, %{consumer: consumer}}
    end

    test "events are sent to Kafka", %{consumer: consumer} do
      message = ConsumersFactory.consumer_message()

      # Mock Kafka partition count lookup
      KafkaMock
      |> Mox.expect(:get_partition_count, fn _sink, _topic -> {:ok, 3} end)
      |> Mox.expect(:publish, fn _sink, _topic, _partition, [_message] -> :ok end)

      start_pipeline!(consumer)

      ref = send_test_event(consumer, message)
      assert_receive {:ack, ^ref, [_successful], []}, 1_000
    end

    test "batched messages are processed together", %{consumer: consumer} do
      message1 = ConsumersFactory.consumer_message()
      message2 = ConsumersFactory.consumer_message()

      # Mock Kafka partition count lookup and publish
      KafkaMock
      |> Mox.expect(:get_partition_count, fn _sink, _topic -> {:ok, 1} end)
      |> Mox.expect(:publish, fn _sink, _topic, _partition, messages ->
        assert length(messages) == 2
        :ok
      end)

      start_pipeline!(consumer)

      ref = send_test_batch(consumer, [message1, message2])
      assert_receive {:ack, ^ref, [_message1, _message2], []}, 1_000
    end

    @tag capture_log: true
    test "failed Kafka publish results in failed events", %{consumer: consumer} do
      # Mock Kafka partition count lookup and failed publish
      KafkaMock
      |> Mox.expect(:get_partition_count, fn _sink, _topic -> {:ok, 3} end)
      |> Mox.expect(:publish, fn _sink, _topic, _partition, _messages ->
        {:error, %RuntimeError{message: "Failed to publish to Kafka"}}
      end)

      start_pipeline!(consumer)

      ref = send_test_event(consumer)
      assert_receive {:ack, ^ref, [], [_failed]}, 2_000
    end

    test "messages are partitioned consistently based on message key", %{consumer: consumer} do
      partition_assignments = :ets.new(:partition_assignments, [:set, :public])

      message = ConsumersFactory.consumer_message()

      # Mock Kafka partition count lookup and capture partition assignments
      KafkaMock
      |> Mox.expect(:get_partition_count, 3, fn _sink, _topic -> {:ok, 3} end)
      |> Mox.expect(:publish, 3, fn _sink, _topic, partition, [message] ->
        :ets.insert(partition_assignments, {message.key, partition})
        :ok
      end)

      start_pipeline!(consumer)

      # Send same event multiple times
      Enum.each(1..3, fn _ ->
        ref = send_test_event(consumer, message)
        assert_receive {:ack, ^ref, [_successful], []}, 1_000
      end)

      # Verify that the same message key was assigned to the same partition each time
      assignments = :ets.tab2list(partition_assignments)
      assert length(assignments) == 1, "Same message key should be assigned to same partition"
    end

    test "Kafka routing can assign topic and message key", %{consumer: consumer} do
      router =
        FunctionsFactory.insert_routing_function!(
          account_id: consumer.account_id,
          function_attrs: [
            sink_type: :kafka,
            body: """
            %{topic: "my_topic", message_key: "my_message_key"}
            """
          ]
        )

      MiniElixir.create(router.id, router.function.code)

      {:ok, consumer} = Consumers.update_sink_consumer(consumer, %{routing_id: router.id, routing_mode: "dynamic"})

      message = ConsumersFactory.consumer_message()

      # Mock Kafka partition count lookup and capture partition assignments
      KafkaMock
      |> Mox.expect(:get_partition_count, fn _sink, _topic -> {:ok, 3} end)
      |> Mox.expect(:publish, fn _sink, "my_topic", _partition, [message] ->
        assert message.key == "my_message_key"
        :ok
      end)

      start_pipeline!(consumer)

      ref = send_test_event(consumer, message)
      assert_receive {:ack, ^ref, [_successful], []}, 1_000
    end
  end

  defp start_pipeline!(consumer, opts \\ []) do
    {dummy_producer, _opts} = Keyword.pop(opts, :dummy_producer, true)

    opts = [
      consumer_id: consumer.id,
      test_pid: self()
    ]

    opts =
      if dummy_producer do
        Keyword.put(opts, :producer, Broadway.DummyProducer)
      else
        opts
      end

    start_supervised!({SinkPipeline, opts})
  end

  defp send_test_event(consumer, message \\ nil) do
    message = message || ConsumersFactory.consumer_message()

    Broadway.test_message(broadway(consumer), message)
  end

  defp send_test_batch(consumer, messages) do
    Broadway.test_batch(broadway(consumer), messages)
  end

  defp broadway(consumer) do
    SinkPipeline.via_tuple(consumer.id)
  end
end

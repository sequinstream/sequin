defmodule Sequin.ConsumersRuntime.NatsPipelineTest do
  use Sequin.DataCase, async: true

  import Hammox

  alias Sequin.Consumers
  alias Sequin.ConsumersRuntime.NatsPipeline
  alias Sequin.Error
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Sinks.NatsMock

  describe "message handling" do
    setup do
      consumer =
        ConsumersFactory.sink_consumer(
          sink: %{
            type: :nats,
            host: "localhost",
            port: 4222
          }
        )

      {:ok, %{consumer: consumer}}
    end

    test "successfully publishes event messages to NATS", %{consumer: consumer} do
      # Expect successful message delivery
      expect(NatsMock, :send_messages, fn _sink, messages ->
        assert length(messages) == 1
        assert hd(messages).data.action == :insert
        :ok
      end)

      # Create and send test event
      event =
        ConsumersFactory.consumer_event(
          consumer_id: consumer.id,
          action: :insert
        )

      ref = send_test_events(consumer, [event])

      # Verify successful acknowledgment
      assert_receive {:ack, ^ref, [%{data: [%{data: %{action: :insert}}]}], []}, 1_000

      # Verify event was processed (deleted)
      refute Consumers.reload(event)
    end

    @tag capture_log: true
    test "handles NATS publish failures", %{consumer: consumer} do
      # Expect failed message delivery
      expect(NatsMock, :send_messages, fn _sink, _messages ->
        {:error, Error.service(service: :nats, code: "unknown_error", message: "Failed to publish")}
      end)

      event =
        ConsumersFactory.consumer_event(
          consumer_id: consumer.id,
          action: :insert
        )

      ref = send_test_events(consumer, [event])

      # Verify failed acknowledgment
      assert_receive {:ack, ^ref, [], [_failed]}, 2_000
    end

    test "batches multiple messages together", %{consumer: consumer} do
      # Update consumer to use batching
      consumer = %{consumer | batch_size: 2}

      expect(NatsMock, :send_messages, fn _sink, messages ->
        assert length(messages) == 2
        assert Enum.map(messages, & &1.data.action) == [:insert, :update]
        :ok
      end)

      event1 =
        ConsumersFactory.consumer_event(
          consumer_id: consumer.id,
          action: :insert
        )

      event2 =
        ConsumersFactory.consumer_event(
          consumer_id: consumer.id,
          action: :update
        )

      ref = send_test_events(consumer, [event1, event2])

      assert_receive {:ack, ^ref, [%{data: [%{data: %{action: :insert}}, %{data: %{action: :update}}]}], []}, 1_000
    end
  end

  defp send_test_events(consumer, events) do
    start_supervised!(
      {NatsPipeline,
       [
         consumer: consumer,
         producer: Broadway.DummyProducer,
         test_pid: self()
       ]}
    )

    Broadway.test_message(
      NatsPipeline.via_tuple(consumer.id),
      events,
      metadata: %{topic: "test_topic", headers: []}
    )
  end
end

defmodule Sequin.ConsumersRuntime.ConsumerProducerTest do
  use Sequin.DataCase, async: true

  alias Sequin.Consumers
  alias Sequin.ConsumersRuntime.ConsumerProducer
  alias Sequin.DatabasesRuntime.SlotMessageStoreMock
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.TestSupport.DateTimeMock

  defmodule Forwarder do
    @moduledoc false
    use Broadway

    alias Broadway.Message
    alias Sequin.DatabasesRuntime.SlotMessageStoreMock

    def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

    def init(opts) do
      {:ok, opts}
    end

    def handle_message(_, broadway_message, %{fail?: true, test_pid: test_pid}) do
      Mox.allow(SlotMessageStoreMock, test_pid, self())
      Mox.allow(DateTimeMock, test_pid, self())

      Message.failed(broadway_message, "failed")
    end

    def handle_message(_, broadway_message, %{test_pid: test_pid}) do
      Mox.allow(SlotMessageStoreMock, test_pid, self())
      Mox.allow(DateTimeMock, test_pid, self())

      send(test_pid, {:messages_handled, broadway_message.data})
      broadway_message
    end

    def handle_batch(_, messages, _, _) do
      messages
    end
  end

  describe "producing messages when messages persisted" do
    setup do
      consumer = ConsumersFactory.insert_sink_consumer!()

      stub(SlotMessageStoreMock, :produce, fn _consumer_id, _count, _producer_pid ->
        {:ok, []}
      end)

      stub(SlotMessageStoreMock, :ack, fn _consumer_id, ack_ids ->
        {:ok, length(ack_ids)}
      end)

      %{consumer: consumer}
    end

    test "produces messages that were already in the database", %{consumer: consumer} do
      msg1 =
        ConsumersFactory.insert_deliverable_consumer_message!(
          message_kind: consumer.message_kind,
          consumer_id: consumer.id
        )

      msg2 =
        ConsumersFactory.insert_deliverable_consumer_message!(
          message_kind: consumer.message_kind,
          consumer_id: consumer.id
        )

      start_broadway(consumer: consumer)
      assert_receive {:messages_handled, messages}, 1_000

      assert_lists_equal(messages, [msg1, msg2], &(&1.id == &2.id))
      stop_broadway()
    end

    test "produces messages that are added to the database after boot", %{consumer: consumer} do
      test_pid = self()
      start_broadway(consumer: consumer)

      stub(SlotMessageStoreMock, :produce, fn _consumer_id, _count, _producer_pid ->
        send(test_pid, :produce_called)
        {:ok, []}
      end)

      assert_receive :produce_called, 1_000

      msg1 =
        ConsumersFactory.insert_deliverable_consumer_message!(
          message_kind: consumer.message_kind,
          consumer_id: consumer.id
        )

      msg2 =
        ConsumersFactory.insert_deliverable_consumer_message!(
          message_kind: consumer.message_kind,
          consumer_id: consumer.id
        )

      :syn.publish(:consumers, {:messages_changed, consumer.id}, :messages_changed)
      assert_receive {:messages_handled, messages}, 1_000

      assert_lists_equal(messages, [msg1, msg2], &(&1.id == &2.id))
      stop_broadway()
    end

    test "mix postgres and sms messages are interleaved", %{consumer: consumer} do
      test_pid = self()

      msg1 =
        ConsumersFactory.insert_deliverable_consumer_message!(
          message_kind: consumer.message_kind,
          consumer_id: consumer.id
        )

      msg2 =
        ConsumersFactory.insert_deliverable_consumer_message!(
          message_kind: consumer.message_kind,
          consumer_id: consumer.id
        )

      msg3 =
        ConsumersFactory.deliverable_consumer_message(
          message_kind: consumer.message_kind,
          consumer_id: consumer.id
        )

      msg4 =
        ConsumersFactory.deliverable_consumer_message(
          message_kind: consumer.message_kind,
          consumer_id: consumer.id
        )

      start_broadway(consumer: consumer)

      stub(SlotMessageStoreMock, :produce, fn _consumer_id, _count, _producer_pid ->
        send(test_pid, :produce_called)
        {:ok, [msg3, msg4]}
      end)

      assert_receive :produce_called, 1_000

      :syn.publish(:consumers, {:messages_changed, consumer.id}, :messages_changed)
      assert_receive {:messages_handled, messages}, 1_000

      assert_lists_equal(messages, [msg1, msg2, msg3, msg4], &(&1.id == &2.id))
      stop_broadway()

      refute Consumers.reload(msg1)
      refute Consumers.reload(msg2)
    end

    test "failed messages are upserted to postgres", %{consumer: consumer} do
      test_pid = self()

      msg1 =
        ConsumersFactory.deliverable_consumer_message(
          message_kind: consumer.message_kind,
          consumer_id: consumer.id
        )

      msg2 =
        ConsumersFactory.insert_deliverable_consumer_message!(
          message_kind: consumer.message_kind,
          consumer_id: consumer.id
        )

      start_broadway(consumer: consumer, context: %{test_pid: test_pid, fail?: true})

      stub(SlotMessageStoreMock, :produce, fn _consumer_id, _count, _producer_pid ->
        send(test_pid, :produce_called)
        {:ok, [msg1]}
      end)

      assert_receive :produce_called, 1_000

      assert_receive {ConsumerProducer, :ack_finished, [], failed_ack_ids}, 1_000
      assert_lists_equal(failed_ack_ids, [msg1.ack_id, msg2.ack_id])

      failed_messages = Consumers.list_consumer_messages_for_consumer(consumer)
      assert_lists_equal(failed_messages, [msg1, msg2], &(&1.ack_id == &2.ack_id))

      stop_broadway()
    end

    test "sms message with a blocked group_id is removed from sms and upserted to postgres", %{consumer: consumer} do
      msg1 =
        ConsumersFactory.deliverable_consumer_message(
          message_kind: consumer.message_kind,
          consumer_id: consumer.id
        )

      msg2 =
        ConsumersFactory.insert_deliverable_consumer_message!(
          message_kind: consumer.message_kind,
          consumer_id: consumer.id,
          group_id: msg1.group_id
        )

      start_broadway(consumer: consumer)

      stub(SlotMessageStoreMock, :produce, fn _consumer_id, _count, _producer_pid ->
        {:ok, [msg1]}
      end)

      expect(SlotMessageStoreMock, :ack, 2, fn _consumer_id, ack_ids ->
        {:ok, length(ack_ids)}
      end)

      assert_receive {ConsumerProducer, :ack_finished, [successful_ack_id], []}, 1_000
      assert successful_ack_id == msg2.ack_id

      assert [inserted_msg] = Consumers.list_consumer_messages_for_consumer(consumer)
      assert inserted_msg.ack_id == msg1.ack_id
    end
  end

  # - Are there still races?
  # - We're not doing any filtering by group_id after pulling from SMS
  #    - And writing back to SMS
  # - tests to add
  #   - blow up in handle_message
  #   - idempotency, both trimming and applying
  #   - demand?

  defp start_broadway(opts) do
    opts =
      Keyword.merge(
        [slot_message_store_mod: SlotMessageStoreMock, test_pid: self()],
        opts
      )

    {context, opts} = Keyword.pop(opts, :context, %{})

    {:ok, pid} =
      Broadway.start_link(
        Forwarder,
        name: Module.concat(__MODULE__, "Forwarder"),
        context: Map.put(context, :test_pid, self()),
        producer: [
          module: {ConsumerProducer, opts},
          concurrency: 1
        ],
        processors: [
          default: [concurrency: 1, max_demand: 1]
        ]
      )

    pid
  end

  defp stop_broadway do
    pid = GenServer.whereis(Module.concat(__MODULE__, "Forwarder"))
    ref = Process.monitor(pid)
    Process.exit(pid, :normal)

    receive do
      {:DOWN, ^ref, _, _, _} -> :ok
    end
  end
end

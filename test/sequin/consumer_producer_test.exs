defmodule Sequin.Runtime.ConsumerProducerTest do
  use Sequin.DataCase, async: true

  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Runtime.ConsumerProducer
  alias Sequin.Runtime.MessageLedgers
  alias Sequin.Runtime.SlotMessageStoreMock
  alias Sequin.TestSupport.DateTimeMock

  defmodule Forwarder do
    @moduledoc false
    use Broadway

    alias Broadway.Message
    alias Sequin.Runtime.SlotMessageStoreMock

    def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

    def init(opts) do
      {:ok, opts}
    end

    def handle_message(_, broadway_message, %{fail?: true, test_pid: test_pid}) do
      Mox.allow(SlotMessageStoreMock, test_pid, self())
      Mox.allow(DateTimeMock, test_pid, self())

      send(test_pid, {:messages_failed, broadway_message.data})
      Message.failed(broadway_message, "failed")
    end

    def handle_message(_, broadway_message, %{test_pid: test_pid}) do
      Mox.allow(SlotMessageStoreMock, test_pid, self())
      Mox.allow(DateTimeMock, test_pid, self())

      send(test_pid, {:messages_succeeded, broadway_message.data})
      broadway_message
    end

    def handle_batch(_, messages, _, _) do
      messages
    end
  end

  describe "ConsumerProducer produces and acks messages end-to-end" do
    setup do
      consumer = ConsumersFactory.insert_sink_consumer!()

      stub(SlotMessageStoreMock, :produce, fn _consumer_id, _count, _producer_pid ->
        {:ok, []}
      end)

      stub(SlotMessageStoreMock, :messages_succeeded, fn _consumer_id, ack_ids ->
        {:ok, length(ack_ids)}
      end)

      %{consumer: consumer}
    end

    test "successful messages are succeeded to sms", %{consumer: consumer} do
      msg1 = ConsumersFactory.consumer_message(message_kind: consumer.message_kind, consumer_id: consumer.id)
      msg2 = ConsumersFactory.consumer_message(message_kind: consumer.message_kind, consumer_id: consumer.id)

      # only the first call produces messages
      expect(SlotMessageStoreMock, :produce, fn _consumer_id, _count, _producer_pid -> {:ok, [msg1, msg2]} end)
      stub(SlotMessageStoreMock, :produce, fn _consumer_id, _count, _producer_pid -> {:ok, []} end)

      expect(SlotMessageStoreMock, :messages_succeeded, fn _consumer_id, ack_ids ->
        assert length(ack_ids) == 2
        {:ok, 2}
      end)

      start_broadway(consumer: consumer)

      assert_receive {:messages_succeeded, messages}, 1_000
      assert length(messages) == 2

      stop_broadway()
    end

    test "failed messages are failed to sms", %{consumer: consumer} do
      msg = ConsumersFactory.consumer_message(message_kind: consumer.message_kind, consumer_id: consumer.id)

      expect(SlotMessageStoreMock, :produce, fn _consumer_id, _count, _producer_pid -> {:ok, [msg]} end)
      stub(SlotMessageStoreMock, :produce, fn _consumer_id, _count, _producer_pid -> {:ok, []} end)

      expect(SlotMessageStoreMock, :messages_failed, fn _consumer_id, message_metas ->
        assert length(message_metas) == 1
        assert Enum.all?(message_metas, &is_map/1)
        :ok
      end)

      start_broadway(consumer: consumer, context: %{fail?: true})

      # No message received since it failed
      assert_receive {:messages_failed, _messages}, 1_000

      stop_broadway()
    end

    test "produced messages are rejected due to idempotency", %{consumer: consumer} do
      test_pid = self()
      msg = ConsumersFactory.consumer_message(message_kind: consumer.message_kind, consumer_id: consumer.id)

      wal_cursor = %{commit_lsn: msg.commit_lsn, commit_idx: msg.commit_idx, commit_timestamp: msg.commit_timestamp}
      MessageLedgers.wal_cursors_delivered(consumer.id, [wal_cursor])

      expect(SlotMessageStoreMock, :produce, fn _consumer_id, _count, _producer_pid -> {:ok, [msg]} end)
      stub(SlotMessageStoreMock, :produce, fn _consumer_id, _count, _producer_pid -> {:ok, []} end)

      expect(SlotMessageStoreMock, :messages_already_succeeded, fn _consumer_id, ack_ids ->
        send(test_pid, {:ack_ids, ack_ids})
        {:ok, 0}
      end)

      start_broadway(consumer: consumer)

      assert_receive {:ack_ids, ack_ids}, 1_000
      assert length(ack_ids) == 1

      # No message received since it was rejected
      refute_receive {:messages_succeeded, []}, 200

      stop_broadway()
    end
  end

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

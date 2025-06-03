defmodule Sequin.Runtime.ConsumerProducerTest do
  use Sequin.DataCase, async: true

  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Runtime.ConsumerProducer
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

    def handle_message(_, broadway_message, _ctx) do
      broadway_message
    end

    def handle_batch(_, messages, _, %{fail?: true, test_pid: test_pid}) do
      Mox.allow(SlotMessageStoreMock, test_pid, self())
      Mox.allow(DateTimeMock, test_pid, self())

      send(test_pid, {:messages_failed, messages})
      Enum.map(messages, &Message.failed(&1, "failed"))
    end

    def handle_batch(_, messages, _, %{test_pid: test_pid}) do
      Mox.allow(SlotMessageStoreMock, test_pid, self())
      Mox.allow(DateTimeMock, test_pid, self())

      send(test_pid, {:messages_succeeded, messages})
      messages
    end
  end

  describe "ConsumerProducer produces and acks messages end-to-end" do
    setup do
      consumer = ConsumersFactory.insert_sink_consumer!()

      stub(SlotMessageStoreMock, :produce, fn _consumer, _count, _producer_pid ->
        {:ok, []}
      end)

      stub(SlotMessageStoreMock, :messages_succeeded, fn _consumer, consumer_messages ->
        {:ok, length(consumer_messages)}
      end)

      %{consumer: consumer}
    end

    test "successful messages are succeeded to sms", %{consumer: consumer} do
      msg1 = ConsumersFactory.consumer_message(message_kind: consumer.message_kind, consumer_id: consumer.id)
      msg2 = ConsumersFactory.consumer_message(message_kind: consumer.message_kind, consumer_id: consumer.id)

      # only the first call produces messages
      expect(SlotMessageStoreMock, :produce, fn _consumer, _count, _producer_pid -> {:ok, [msg1, msg2]} end)
      stub(SlotMessageStoreMock, :produce, fn _consumer, _count, _producer_pid -> {:ok, []} end)

      expect(SlotMessageStoreMock, :messages_succeeded, fn _consumer, consumer_messages ->
        assert length(consumer_messages) == 2
        {:ok, 2}
      end)

      start_broadway(consumer_id: consumer.id, batch_size: 2)

      assert_receive {:messages_succeeded, messages}, 2_000
      message_data = Enum.map(messages, & &1.data)
      assert_lists_equal(message_data, [msg1, msg2], fn a, b -> a.data == b.data end)

      stop_broadway()
    end

    @tag capture_log: true
    test "failed messages are failed to sms", %{consumer: consumer} do
      msg = ConsumersFactory.consumer_message(message_kind: consumer.message_kind, consumer_id: consumer.id)

      expect(SlotMessageStoreMock, :produce, fn _consumer, _count, _producer_pid -> {:ok, [msg]} end)
      stub(SlotMessageStoreMock, :produce, fn _consumer, _count, _producer_pid -> {:ok, []} end)

      expect(SlotMessageStoreMock, :messages_failed, fn _consumer, message_metas ->
        assert length(message_metas) == 1
        assert Enum.all?(message_metas, &is_map/1)
        :ok
      end)

      start_broadway(consumer_id: consumer.id, context: %{fail?: true})

      # No message received since it failed
      assert_receive {:messages_failed, _messages}, 1_000

      stop_broadway()
    end
  end

  defp start_broadway(opts) do
    opts =
      Keyword.merge(
        [slot_message_store_mod: SlotMessageStoreMock, test_pid: self()],
        opts
      )

    batch_size = Keyword.get(opts, :batch_size, 1)

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
        ],
        batchers: [
          default: [concurrency: 1, batch_size: batch_size]
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

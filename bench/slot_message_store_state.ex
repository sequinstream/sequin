defmodule Sequin.Bench.SlotMessageStoreState do
  @moduledoc false
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Runtime.SlotMessageStore.State
  alias Sequin.Factory
  alias Sequin.Factory.ConsumersFactory

  def run do
    # Setup initial states with different message counts
    {state_5k, messages_5k} = setup_state_with_messages(5_000)
    {state_50k, messages_50k} = setup_state_with_messages(50_000)

    # Sample messages for operations that need specific messages
    sample_messages_5k = Enum.take_random(messages_5k, 100)
    sample_messages_50k = Enum.take_random(messages_50k, 100)

    Benchee.run(
      %{
        "put_messages (5k)" => fn ->
          State.put_messages(state_5k, [ConsumersFactory.consumer_message()])
        end,
        "put_messages (50k)" => fn ->
          State.put_messages(state_50k, [ConsumersFactory.consumer_message()])
        end,
        "put_persisted_messages (5k)" => fn ->
          State.put_persisted_messages(state_5k, [ConsumersFactory.consumer_message()])
        end,
        "put_persisted_messages (50k)" => fn ->
          State.put_persisted_messages(state_50k, [ConsumersFactory.consumer_message()])
        end,
        "pop_messages (5k)" => fn ->
          State.pop_messages(state_5k, Enum.map(sample_messages_5k, & &1.ack_id))
        end,
        "pop_messages (50k)" => fn ->
          State.pop_messages(state_50k, Enum.map(sample_messages_50k, & &1.ack_id))
        end,
        "produce_messages (5k)" => fn ->
          State.produce_messages(state_5k, 100)
        end,
        "produce_messages (50k)" => fn ->
          State.produce_messages(state_50k, 100)
        end
        # "peek_messages count (5k)" => fn ->
        #   State.peek_messages(state_5k, 100)
        # end,
        # "peek_messages count (50k)" => fn ->
        #   State.peek_messages(state_50k, 100)
        # end,
        # "peek_messages ack_ids (5k)" => fn ->
        #   State.peek_messages(state_5k, Enum.map(sample_messages_5k, & &1.ack_id))
        # end,
        # "peek_messages ack_ids (50k)" => fn ->
        #   State.peek_messages(state_50k, Enum.map(sample_messages_50k, & &1.ack_id))
        # end,
        # "reset_message_visibilities (5k)" => fn ->
        #   State.reset_message_visibilities(state_5k, Enum.map(sample_messages_5k, & &1.ack_id))
        # end,
        # "reset_message_visibilities (50k)" => fn ->
        #   State.reset_message_visibilities(state_50k, Enum.map(sample_messages_50k, & &1.ack_id))
        # end,
        # "reset_all_message_visibilities (5k)" => fn ->
        #   State.reset_all_message_visibilities(state_5k)
        # end,
        # "reset_all_message_visibilities (50k)" => fn ->
        #   State.reset_all_message_visibilities(state_50k)
        # end
      },
      time: 5,
      memory_time: 2
    )
  end

  # Helper function to set up state with n messages
  defp setup_state_with_messages(count) do
    consumer = %SinkConsumer{id: "test-consumer", seq: Factory.unique_integer()}
    messages = Enum.map(1..count, fn _ -> ConsumersFactory.consumer_message() end)
    state = %State{consumer: consumer}
    :ok = State.setup_ets(consumer)
    {:ok, state_with_messages} = State.put_messages(state, messages)
    {state_with_messages, messages}
  end
end

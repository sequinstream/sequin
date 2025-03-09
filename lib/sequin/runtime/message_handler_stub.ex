defmodule Sequin.Runtime.MessageHandlerStub do
  @moduledoc false
  @behaviour Sequin.Runtime.SlotMessageHandler

  alias Sequin.Runtime.SlotMessageHandler

  def handle_messages(_context, messages) do
    if id = config()[:consumer_id] do
      count = length(messages)
      Sequin.Metrics.incr_consumer_messages_processed_count(%{id: id}, count)
      Sequin.Metrics.incr_consumer_messages_processed_throughput(%{id: id}, count)
    end

    :ok
  end

  defdelegate before_handle_messages(context, messages), to: SlotMessageHandler
  defdelegate handle_logical_message(context, seq, message), to: SlotMessageHandler
  defdelegate reload_entities(context), to: SlotMessageHandler
  defdelegate flush_messages(context), to: SlotMessageHandler

  defp config do
    Application.get_env(:sequin, __MODULE__, [])
  end
end

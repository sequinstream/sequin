defmodule SequinWeb.MessageJSON do
  alias Sequin.Streams.ConsumerMessage

  def render("publish.json", %{count: count}) do
    %{data: %{published: count}}
  end

  def render("stream_list.json", %{messages: messages}) do
    %{data: messages}
  end

  def render("consumer_list.json", %{consumer_messages: consumer_messages}) do
    messages =
      Enum.map(consumer_messages, fn %ConsumerMessage{} = cm ->
        state = if cm.state == :pending_redelivery, do: :delivered, else: cm.state

        %{
          message: cm.message,
          info: %{
            ack_id: cm.ack_id,
            deliver_count: cm.deliver_count,
            last_delivered_at: cm.last_delivered_at,
            not_visible_until: cm.not_visible_until,
            state: state
          }
        }
      end)

    %{data: messages}
  end
end

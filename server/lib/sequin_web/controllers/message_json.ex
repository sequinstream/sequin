defmodule SequinWeb.MessageJSON do
  alias Sequin.Streams.ConsumerMessage

  def render("publish.json", %{count: count}) do
    %{data: %{published: count}}
  end

  def render("stream_list.json", %{messages: messages}) do
    %{data: messages}
  end

  def render("stream_get.json", %{message: message}) do
    message
  end

  def render("consumer_list.json", %{consumer_messages: consumer_messages}) do
    messages =
      Enum.map(consumer_messages, fn %ConsumerMessage{} = cm ->
        {state, not_visible_until} = get_current_state(cm)

        %{
          message: cm.message,
          info: %{
            ack_id: cm.ack_id,
            deliver_count: cm.deliver_count,
            last_delivered_at: cm.last_delivered_at,
            not_visible_until: not_visible_until,
            state: state
          }
        }
      end)

    %{data: messages}
  end

  def render("stream_count.json", %{count: count}) do
    %{count: count}
  end

  defp get_current_state(cm) do
    now = DateTime.utc_now()

    cond do
      cm.state == :available ->
        {:available, nil}

      cm.state in [:delivered, :pending_redelivery] and DateTime.before?(cm.not_visible_until, now) ->
        {:available, nil}

      cm.state == :pending_redelivery ->
        {:delivered, cm.not_visible_until}

      true ->
        {cm.state, cm.not_visible_until}
    end
  end
end

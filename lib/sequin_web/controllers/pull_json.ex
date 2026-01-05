defmodule SequinWeb.PullJSON do
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Transforms.Message

  def render("receive.json", %{consumer: consumer, messages: messages}) do
    %{data: Enum.map(messages, &render_message(consumer, &1))}
  end

  defp render_message(%SinkConsumer{} = consumer, %ConsumerEvent{} = event) do
    %{
      ack_id: event.ack_id,
      data: render_data(consumer, event)
    }
  end

  defp render_data(%SinkConsumer{} = consumer, %ConsumerEvent{} = event) do
    Message.to_external(consumer, event)
  end
end

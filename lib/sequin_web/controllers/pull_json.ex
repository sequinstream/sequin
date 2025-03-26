defmodule SequinWeb.PullJSON do
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Transforms.Message

  def render("receive.json", %{consumer: consumer, messages: messages}) do
    %{data: Enum.map(messages, &render_message(consumer, &1))}
  end

  defp render_message(%SinkConsumer{message_kind: :event} = consumer, %ConsumerEvent{} = event) do
    %{
      ack_id: event.ack_id,
      data: render_data(consumer, event)
    }
  end

  defp render_message(%SinkConsumer{message_kind: :record} = consumer, %ConsumerRecord{} = record) do
    %{
      ack_id: record.ack_id,
      data: render_data(consumer, record)
    }
  end

  defp render_data(%SinkConsumer{} = consumer, %ConsumerEvent{} = event) do
    Message.to_external(consumer, event)
  end

  defp render_data(%SinkConsumer{} = consumer, %ConsumerRecord{} = record) do
    Message.to_external(consumer, record)
  end
end

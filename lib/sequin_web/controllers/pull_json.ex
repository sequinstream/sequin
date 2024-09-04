defmodule SequinWeb.PullJSON do
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerEventData
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.ConsumerRecordData

  def render("receive.json", %{messages: messages}) do
    %{data: Enum.map(messages, &render_message/1)}
  end

  defp render_message(%ConsumerEvent{} = event) do
    %{
      ack_id: event.ack_id,
      data: render_data(event.data)
    }
  end

  defp render_message(%ConsumerRecord{} = record) do
    %{
      ack_id: record.ack_id,
      data: render_data(record.data)
    }
  end

  defp render_data(%ConsumerEventData{action: action, record: record, changes: changes}) do
    %{
      action: action,
      record: record,
      changes: changes
    }
  end

  defp render_data(%ConsumerRecordData{record: record}) do
    %{
      record: record
    }
  end
end

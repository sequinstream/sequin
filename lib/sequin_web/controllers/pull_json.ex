defmodule SequinWeb.PullJSON do
  alias Sequin.Consumers.ConsumerEvent

  def render("receive.json", %{messages: messages}) do
    %{data: Enum.map(messages, &render_message/1)}
  end

  defp render_message(%ConsumerEvent{} = event) do
    %{
      ack_id: event.ack_id,
      data: render_data(event.data)
    }
  end

  defp render_data(%{action: action, record: record, changes: changes}) do
    %{
      action: action,
      record: record,
      changes: changes
    }
  end
end

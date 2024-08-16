defmodule SequinWeb.PullJSON do
  alias Sequin.Streams.Message

  def render("receive.json", %{messages: messages}) do
    %{data: Enum.map(messages, &render_message/1)}
  end

  defp render_message(%Message{} = message) do
    %{ack_id: message.ack_id, message: message}
  end
end

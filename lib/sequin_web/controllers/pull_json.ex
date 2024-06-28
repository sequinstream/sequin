defmodule SequinWeb.PullJSON do
  alias Sequin.Streams.Message

  def render("next.json", %{messages: messages}) do
    %{data: Enum.map(messages, &render_message/1)}
  end

  defp render_message(%Message{} = message) do
    %{ack_token: message.ack_id, message: message}
  end
end

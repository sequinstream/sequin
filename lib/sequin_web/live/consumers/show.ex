defmodule SequinWeb.ConsumersLive.Show do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Consumers

  @impl Phoenix.LiveView
  def mount(%{"id" => id}, _session, socket) do
    consumer = Consumers.get_consumer_for_account(current_account_id(socket), id)
    {:ok, assign(socket, :consumer, consumer)}
  end

  @impl Phoenix.LiveView
  def render(assigns) do
    ~H"""
    <div id="consumer-show">
      <h1>Consumer Details</h1>
      <p>ID: <%= @consumer.id %></p>
      <p>Name: <%= @consumer.name %></p>
      <p>Type: <%= consumer_type(@consumer) %></p>
      <p>Status: <%= @consumer.status %></p>
    </div>
    """
  end

  defp consumer_type(%Consumers.HttpPullConsumer{}), do: "pull"
  defp consumer_type(%Consumers.HttpPushConsumer{}), do: "push"
end

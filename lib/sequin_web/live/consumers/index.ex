defmodule SequinWeb.ConsumersLive.Index do
  @moduledoc false
  use SequinWeb, :live_view

  def mount(_params, _session, socket) do
    {:ok, assign(socket, count: 0)}
  end

  def handle_event("increment", _, socket) do
    {:noreply, update(socket, :count, &(&1 + 1))}
  end

  def render(assigns) do
    ~H"""
    <div>
      <h1>ShadCN Svelte Button Test</h1>
      <.svelte name="Button" props={%{text: "ShadCN Button", count: @count}} socket={@socket} />
    </div>
    """
  end
end

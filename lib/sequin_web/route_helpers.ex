defmodule SequinWeb.RouteHelpers do
  @moduledoc false
  use SequinWeb, :verified_routes

  alias Sequin.Consumers.SinkConsumer

  def consumer_path(consumer, subpath \\ "")

  def consumer_path(%SinkConsumer{id: id, type: type}, subpath) do
    ~p"/sinks/#{type}/#{id}" <> subpath
  end
end

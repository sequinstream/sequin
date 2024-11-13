defmodule SequinWeb.RouteHelpers do
  @moduledoc false
  use SequinWeb, :verified_routes

  alias Sequin.Consumers.DestinationConsumer
  alias Sequin.Consumers.HttpPullConsumer

  def consumer_path(consumer, subpath \\ "")

  def consumer_path(%HttpPullConsumer{id: id}, subpath) do
    ~p"/consumer-groups/#{id}" <> subpath
  end

  def consumer_path(%DestinationConsumer{id: id, type: type}, subpath) do
    ~p"/consumers/#{type}/#{id}" <> subpath
  end
end

defmodule SequinWeb.RouteHelpers do
  @moduledoc false
  use SequinWeb, :verified_routes

  alias Sequin.Consumers.SinkConsumer

  def consumer_path(consumer, subpath \\ "", query_params \\ [])

  def consumer_path(%SinkConsumer{id: id, type: type}, subpath, query_params) do
    case subpath do
      "" -> ~p"/sinks/#{type}/#{id}?#{query_params}"
      "backfills" -> ~p"/sinks/#{type}/#{id}/backfills?#{query_params}"
      "messages" -> ~p"/sinks/#{type}/#{id}/messages?#{query_params}"
      "messages/" <> ack_id -> ~p"/sinks/#{type}/#{id}/messages/#{ack_id}?#{query_params}"
      "trace" -> ~p"/sinks/#{type}/#{id}/trace?#{query_params}"
      "edit" -> ~p"/sinks/#{type}/#{id}/edit?#{query_params}"
    end
  end
end

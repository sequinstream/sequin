defmodule SequinWeb.ConsumerController do
  use SequinWeb, :controller

  # alias Sequin.Consumers
  # alias Sequin.Streams
  # alias SequinWeb.ApiFallbackPlug

  # action_fallback ApiFallbackPlug

  # def index(conn, %{"stream_id_or_name" => stream_id_or_name}) do
  #   account_id = conn.assigns.account_id

  #   with {:ok, stream} <- Streams.get_stream_for_account(account_id, stream_id_or_name) do
  #     render(conn, "index.json", consumers: Consumers.list_consumers_for_stream(stream.id))
  #   end
  # end

  # def show(conn, %{"stream_id_or_name" => stream_id_or_name, "id_or_name" => id_or_name}) do
  #   account_id = conn.assigns.account_id

  #   with {:ok, stream} <- Streams.get_stream_for_account(account_id, stream_id_or_name),
  #        {:ok, consumer} <- Consumers.get_consumer_for_stream(stream.id, id_or_name) do
  #     render(conn, "show.json", consumer: consumer)
  #   end
  # end

  # def create(conn, %{"stream_id_or_name" => stream_id_or_name} = params) do
  #   account_id = conn.assigns.account_id

  #   with {:ok, stream} <- Streams.get_stream_for_account(account_id, stream_id_or_name),
  #        {:ok, consumer} <-
  #          Consumers.create_consumer_for_account_with_lifecycle(account_id, Map.put(params, "stream_id", stream.id)) do
  #     render(conn, "show.json", consumer: consumer)
  #   end
  # end

  # def update(conn, %{"stream_id_or_name" => stream_id_or_name, "id_or_name" => id_or_name} = params) do
  #   account_id = conn.assigns.account_id

  #   with {:ok, stream} <- Streams.get_stream_for_account(account_id, stream_id_or_name),
  #        {:ok, consumer} <- Consumers.get_consumer_for_stream(stream.id, id_or_name),
  #        {:ok, updated_consumer} <- Consumers.update_consumer_with_lifecycle(consumer, params) do
  #     render(conn, "show.json", consumer: updated_consumer)
  #   end
  # end

  # def delete(conn, %{"stream_id_or_name" => stream_id_or_name, "id_or_name" => id_or_name}) do
  #   account_id = conn.assigns.account_id

  #   with {:ok, stream} <- Streams.get_stream_for_account(account_id, stream_id_or_name),
  #        {:ok, consumer} <- Consumers.get_consumer_for_stream(stream.id, id_or_name),
  #        {:ok, _consumer} <- Consumers.delete_consumer_with_lifecycle(consumer) do
  #     render(conn, "delete.json", consumer: consumer)
  #   end
  # end
end

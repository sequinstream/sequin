defmodule SequinWeb.StreamController do
  use SequinWeb, :controller

  alias Sequin.Streams
  alias SequinWeb.ApiFallbackPlug

  action_fallback ApiFallbackPlug

  def index(conn, _params) do
    account_id = conn.assigns.account_id

    render(conn, "index.json", streams: Streams.list_streams_for_account(account_id))
  end

  def show(conn, %{"id" => id}) do
    account_id = conn.assigns.account_id

    with {:ok, stream} <- Streams.get_stream_for_account(account_id, id) do
      render(conn, "show.json", stream: stream)
    end
  end

  def create(conn, params) do
    account_id = conn.assigns.account_id

    with {:ok, stream} <- Streams.create_stream_for_account_with_lifecycle(account_id, params) do
      render(conn, "show.json", stream: stream)
    end
  end

  def delete(conn, %{"id" => id}) do
    account_id = conn.assigns.account_id

    with {:ok, stream} <- Streams.get_stream_for_account(account_id, id),
         {:ok, _stream} <- Streams.delete_stream(stream) do
      render(conn, "delete.json", stream: stream)
    end
  end
end

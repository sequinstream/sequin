defmodule SequinWeb.StreamController do
  use SequinWeb, :controller

  def index(conn, _params) do
    render(conn, "index.json",
      streams: [
        %{
          id: UXID.generate!(prefix: "strm"),
          idx: System.unique_integer([:positive]),
          consumer_count: 0,
          message_count: 0,
          created_at: DateTime.utc_now(),
          updated_at: DateTime.utc_now()
        },
        %{
          id: UXID.generate!(prefix: "strm"),
          idx: System.unique_integer([:positive]),
          consumer_count: 1,
          message_count: 1000,
          created_at: DateTime.utc_now(),
          updated_at: DateTime.utc_now()
        }
      ]
    )
  end

  def show(conn, %{"id" => id}) do
    render(conn, "show.json",
      stream: %{
        id: id,
        idx: System.unique_integer([:positive]),
        consumer_count: 0,
        message_count: 0,
        created_at: DateTime.utc_now(),
        updated_at: DateTime.utc_now()
      }
    )
  end
end

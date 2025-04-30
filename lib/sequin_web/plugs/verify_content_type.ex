defmodule SequinWeb.Plugs.VerifyContentType do
  @moduledoc """
  This plug verifies that the content type of the request is application/json
  for POSTs, PUTs, and PATCHes.
  """
  import Plug.Conn

  @methods ~w(POST PUT PATCH)

  def init(default), do: default

  def call(%Plug.Conn{method: method} = conn, _default) when method in @methods do
    conn
    |> get_req_header("content-type")
    |> Enum.any?(&(&1 =~ "application/json"))
    |> case do
      true ->
        conn

      false ->
        error = Sequin.Error.bad_request(message: "Content-Type: application/json is required")
        SequinWeb.ApiFallbackPlug.call(conn, {:error, error})
    end
  end

  def call(conn, _default), do: conn
end

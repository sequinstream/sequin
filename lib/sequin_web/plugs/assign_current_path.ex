defmodule SequinWeb.Plugs.AssignCurrentPath do
  @moduledoc false
  import Phoenix.Controller
  import Plug.Conn

  def init(options), do: options

  def call(conn, _opts) do
    assign(conn, :current_path, current_path(conn))
  end
end

defmodule SequinWeb.PageController do
  use SequinWeb, :controller

  def home(conn, _params) do
    render(conn, :home, layout: false)
  end
end

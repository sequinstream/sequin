defmodule SequinWeb.EasterEggController do
  use SequinWeb, :controller

  def home(conn, _params) do
    render(conn, :home, layout: false)
  end
end

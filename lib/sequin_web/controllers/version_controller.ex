defmodule SequinWeb.VersionController do
  use SequinWeb, :controller

  def show(conn, _params) do
    version = Application.get_env(:sequin, :release_version)
    json(conn, %{version: version})
  end
end

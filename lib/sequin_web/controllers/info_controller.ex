defmodule SequinWeb.InfoController do
  use SequinWeb, :controller

  def version(conn, _params) do
    version = Application.get_env(:sequin, :release_version)
    json(conn, %{version: version})
  end

  def info(conn, _params) do
    json(conn, %{
      version: Application.get_env(:sequin, :release_version),
      nodes: Enum.map(Node.list(), &Atom.to_string/1)
    })
  end
end

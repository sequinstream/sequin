defmodule SequinWeb.HealthCheckController do
  use SequinWeb, :controller

  alias Sequin.Repo

  def check(conn, _params) do
    Repo.query!("SELECT 1")
    {:ok, "PONG"} = Redix.command(:redix, ["PING"])

    send_resp(conn, 200, Jason.encode!(%{ok: true, rev: System.get_env("CURRENT_GIT_SHA")}))
  end
end

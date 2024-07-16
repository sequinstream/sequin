defmodule Sequin.Test.Support.Postgres do
  @moduledoc false
  def start_test_db_link! do
    {:ok, conn} =
      Postgrex.start_link(Keyword.take(Sequin.Repo.config(), [:hostname, :port, :database, :username, :password]))

    conn
  end
end

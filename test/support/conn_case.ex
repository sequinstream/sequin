defmodule SequinWeb.ConnCase do
  @moduledoc """
  This module defines the test case to be used by
  tests that require setting up a connection.

  Such tests rely on `Phoenix.ConnTest` and also
  import other functionality to make it easier
  to build common data structures and query the data layer.

  Finally, if the test case interacts with the database,
  we enable the SQL sandbox, so changes done to the database
  are reverted at the end of every test. If you are using
  PostgreSQL, you can even run database tests asynchronously
  by setting `use SequinWeb.ConnCase, async: true`, although
  this option is not recommended for other databases.
  """

  use ExUnit.CaseTemplate

  import Plug.Conn

  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ApiTokensFactory
  alias SequinWeb.Plugs.VerifyApiToken

  using opts do
    quote do
      use Sequin.DataCase, unquote(opts)
      use SequinWeb, :verified_routes

      import Phoenix.ConnTest
      import Plug.Conn
      import SequinWeb.ConnCase
      # The default endpoint for testing
      @endpoint SequinWeb.Endpoint

      # Import conveniences for testing with connections
    end
  end

  setup _tags do
    {:ok, conn: Phoenix.ConnTest.build_conn()}
  end

  def authenticated_conn(%{conn: conn}) do
    # TODO: Right now, FetchUser just uses this - in future, we'll need to add to conn here
    account = AccountsFactory.insert_account!()
    token = ApiTokensFactory.insert_token!(account_id: account.id)
    conn = put_req_header(conn, VerifyApiToken.header(), "Bearer #{token.token}")
    conn = put_req_header(conn, "content-type", "application/json")
    {:ok, conn: conn, account: account}
  end

  @doc """
  Setup helper that registers and logs in users.

      setup :logged_in_user

  It stores an updated connection and a registered user in the
  test context.
  """
  def logged_in_user(%{conn: conn}) do
    account = AccountsFactory.insert_account!()
    user = AccountsFactory.insert_user!(account_id: account.id)
    %{conn: log_in_user(conn, user), user: user, account: account}
  end

  @doc """
  Logs the given `user` into the `conn`.

  It returns an updated `conn`.
  """
  def log_in_user(conn, user) do
    token = Sequin.Accounts.generate_user_session_token(user)

    conn
    |> Phoenix.ConnTest.init_test_session(%{})
    |> Plug.Conn.put_session(:user_token, token)
  end
end

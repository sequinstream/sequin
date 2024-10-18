defmodule SequinWeb.UserSessionControllerTest do
  use SequinWeb.ConnCase, async: true

  alias Sequin.Factory.AccountsFactory

  setup do
    %{user: AccountsFactory.insert_user!()}
  end

  describe "POST /login" do
    test "logs the user in", %{conn: conn, user: user} do
      conn =
        post(conn, ~p"/login", %{
          "user" => %{"email" => user.email, "password" => user.password}
        })

      assert get_session(conn, :user_token)
      assert redirected_to(conn) == ~p"/"

      # Now do a logged in request
      conn = get(conn, ~p"/sequences")
      html_response(conn, 200)
    end

    test "logs the user in with remember me", %{conn: conn, user: user} do
      conn =
        post(conn, ~p"/login", %{
          "user" => %{
            "email" => user.email,
            "password" => user.password,
            "remember_me" => "true"
          }
        })

      assert conn.resp_cookies["_sequin_web_user_remember_me"]
      assert redirected_to(conn) == ~p"/"
    end

    test "logs the user in with return to", %{conn: conn, user: user} do
      conn =
        conn
        |> init_test_session(user_return_to: "/foo/bar")
        |> post(~p"/login", %{
          "user" => %{
            "email" => user.email,
            "password" => user.password
          }
        })

      assert redirected_to(conn) == "/foo/bar"
    end

    test "login following registration", %{conn: conn, user: user} do
      conn =
        post(conn, ~p"/login", %{
          "_action" => "registered",
          "user" => %{"email" => user.email, "password" => user.password}
        })

      assert redirected_to(conn) == ~p"/"
      assert flash_text(conn, :info) =~ "Account created successfully"
    end

    test "login following password update", %{conn: conn, user: user} do
      conn =
        post(conn, ~p"/login", %{
          "_action" => "password_updated",
          "user" => %{"email" => user.email, "password" => user.password}
        })

      assert redirected_to(conn) == ~p"/users/settings"
      assert flash_text(conn, :info) =~ "Password updated successfully"
    end

    test "redirects to login page with invalid credentials", %{conn: conn} do
      conn =
        post(conn, ~p"/login", %{
          "user" => %{"email" => "invalid@email.com", "password" => "invalid_password"}
        })

      assert flash_text(conn, :error) =~ "Invalid email or password"
      assert redirected_to(conn) == ~p"/login"
    end
  end

  describe "DELETE /logout" do
    test "logs the user out", %{conn: conn, user: user} do
      conn = conn |> log_in_user(user) |> delete(~p"/logout")
      assert redirected_to(conn) == ~p"/login"
      refute get_session(conn, :user_token)
      assert flash_text(conn, :info) =~ "Logged out successfully"
    end

    test "succeeds even if the user is not logged in", %{conn: conn} do
      conn = delete(conn, ~p"/logout")
      assert redirected_to(conn) == ~p"/login"
      refute get_session(conn, :user_token)
      assert flash_text(conn, :info) =~ "Logged out successfully"
    end
  end
end

defmodule SequinWeb.UserLoginLiveTest do
  use SequinWeb.ConnCase, async: true

  import Phoenix.LiveViewTest

  alias Sequin.Accounts
  alias Sequin.Factory.AccountsFactory

  describe "Log in page" do
    test "renders log in page", %{conn: conn} do
      {:ok, _lv, html} = live(conn, ~p"/login")

      assert html =~ "Welcome back"
      assert html =~ "Sign up"
      assert html =~ "Forgot your password?"
    end

    test "shows default credentials when only default user exists", %{conn: conn} do
      # Create default user but don't log them in
      AccountsFactory.insert_user!(%{
        email: Sequin.Accounts.default_user_email(),
        password: Sequin.Accounts.default_user_password()
      })

      {:ok, _lv, html} = live(conn, ~p"/login")

      assert html =~ "Default login credentials"
      assert html =~ Sequin.Accounts.default_user_email()
      assert html =~ Sequin.Accounts.default_user_password()
    end

    test "does not show default credentials when default user has logged in", %{conn: conn} do
      user =
        AccountsFactory.insert_user!(%{
          email: Sequin.Accounts.default_user_email(),
          password: Sequin.Accounts.default_user_password()
        })

      # Update the user to have a last_login_at timestamp
      Accounts.update_user(user, %{last_login_at: DateTime.utc_now()})

      {:ok, _lv, html} = live(conn, ~p"/login")

      refute html =~ "Default login credentials"
    end

    test "does not show default credentials when other users exist", %{conn: conn} do
      # Create default user
      AccountsFactory.insert_user!(%{
        email: Sequin.Accounts.default_user_email(),
        password: Sequin.Accounts.default_user_password()
      })

      # Create another user
      AccountsFactory.insert_user!()

      {:ok, _lv, html} = live(conn, ~p"/login")

      refute html =~ "Default login credentials"
    end

    test "redirects if already logged in", %{conn: conn} do
      result =
        conn
        |> log_in_user(AccountsFactory.insert_user!())
        |> live(~p"/login")
        |> follow_redirect(conn, "/")

      assert {:ok, _conn} = result
    end
  end

  describe "user login" do
    test "redirects if user login with valid credentials", %{conn: conn} do
      password = "123456789abcd"
      user = AccountsFactory.insert_user!(%{password: password})

      {:ok, lv, _html} = live(conn, ~p"/login")

      form =
        form(lv, "#login_form", user: %{email: user.email, password: password, remember_me: true})

      conn = submit_form(form, conn)

      assert redirected_to(conn) == ~p"/"
    end

    test "redirects to login page with a flash error if there are no valid credentials", %{
      conn: conn
    } do
      {:ok, lv, _html} = live(conn, ~p"/login")

      form =
        form(lv, "#login_form", user: %{email: "test@email.com", password: "123456", remember_me: true})

      conn = submit_form(form, conn)

      assert flash_text(conn, :error) =~ "Invalid email or password"

      assert redirected_to(conn) == "/login"
    end
  end

  describe "login navigation" do
    test "redirects to registration page when the Register button is clicked", %{conn: conn} do
      {:ok, lv, _html} = live(conn, ~p"/login")

      {:ok, _login_live, login_html} =
        lv
        |> element(~s|main a:fl-contains("Sign up")|)
        |> render_click()
        |> follow_redirect(conn, ~p"/register")

      assert login_html =~ "Welcome! Create your account"
    end

    test "redirects to forgot password page when the Forgot Password button is clicked", %{
      conn: conn
    } do
      {:ok, lv, _html} = live(conn, ~p"/login")

      {:ok, conn} =
        lv
        |> element(~s|main a:fl-contains("Forgot your password?")|)
        |> render_click()
        |> follow_redirect(conn, ~p"/users/reset_password")

      assert conn.resp_body =~ "Forgot your password?"
    end
  end
end

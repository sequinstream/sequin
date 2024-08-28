defmodule SequinWeb.UserRegistrationLiveTest do
  use SequinWeb.ConnCase, async: true

  import Phoenix.LiveViewTest

  alias Sequin.Factory.AccountsFactory

  describe "Registration page" do
    test "renders registration page", %{conn: conn} do
      {:ok, _lv, html} = live(conn, ~p"/register")

      assert html =~ "Register"
      assert html =~ "Log in"
    end

    test "redirects if already logged in", %{conn: conn} do
      result =
        conn
        |> log_in_user(AccountsFactory.insert_user!())
        |> live(~p"/register")
        |> follow_redirect(conn, "/")

      assert {:ok, _conn} = result
    end

    test "renders errors for invalid data", %{conn: conn} do
      {:ok, lv, _html} = live(conn, ~p"/register")

      form = form(lv, "#registration_form", user: %{"email" => "with spaces", "password" => "too short"})
      result = render_submit(form)

      assert result =~ "Register"
      assert result =~ "must have the @ sign and no spaces"
      assert result =~ "should be at least 12 character"
    end
  end

  describe "register user" do
    test "creates account and logs the user in", %{conn: conn} do
      {:ok, lv, _html} = live(conn, ~p"/register")

      email = AccountsFactory.email()
      password = AccountsFactory.password()
      form = form(lv, "#registration_form", user: %{email: email, password: password})
      render_submit(form)
      conn = follow_trigger_action(form, conn)

      assert redirected_to(conn) == ~p"/"
    end

    test "renders errors for duplicated email", %{conn: conn} do
      {:ok, lv, _html} = live(conn, ~p"/register")

      user = AccountsFactory.insert_user!(%{email: "test@email.com"})

      result =
        lv
        |> form("#registration_form",
          user: %{"email" => user.email, "password" => "valid_password"}
        )
        |> render_submit()

      assert result =~ "has already been taken"
    end
  end

  describe "registration navigation" do
    test "redirects to login page when the Log in button is clicked", %{conn: conn} do
      {:ok, lv, _html} = live(conn, ~p"/register")

      {:ok, _login_live, login_html} =
        lv
        |> element(~s|a:fl-contains("Log in")|)
        |> render_click()
        |> follow_redirect(conn, ~p"/login")

      assert login_html =~ "Log in"
    end
  end
end

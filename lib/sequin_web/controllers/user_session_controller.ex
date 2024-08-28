defmodule SequinWeb.UserSessionController do
  use SequinWeb, :controller

  alias Sequin.Accounts
  alias SequinWeb.UserAuth

  def create(conn, %{"_action" => "registered"} = params) do
    create(conn, params, "Account created successfully!")
  end

  def create(conn, %{"_action" => "password_updated"} = params) do
    conn
    |> put_session(:user_return_to, ~p"/users/settings")
    |> create(params, "Password updated successfully!")
  end

  def create(conn, params) do
    create(conn, params, nil)
  end

  defp create(conn, %{"user" => user_params}, flash_info) do
    %{"email" => email, "password" => password} = user_params

    if user = Accounts.get_user_by_email_and_password(email, password) do
      conn = if flash_info, do: put_flash(conn, :toast, %{kind: :info, title: flash_info}), else: conn

      UserAuth.log_in_user(conn, user, user_params)
    else
      # In order to prevent user enumeration attacks, don't disclose whether the email is registered.
      conn
      |> put_flash(:toast, %{kind: :error, title: "Invalid email or password"})
      |> put_flash(:email, String.slice(email, 0, 160))
      |> redirect(to: ~p"/login")
    end
  end

  def delete(conn, _params) do
    conn
    |> put_flash(:toast, %{kind: :info, title: "Logged out successfully."})
    |> UserAuth.log_out_user()
  end
end

defmodule SequinWeb.UserSessionController do
  use SequinWeb, :controller

  alias Assent.Strategy.Github
  alias Sequin.Accounts
  alias Sequin.Accounts.Impersonate
  alias SequinWeb.UserAuth

  require Logger

  def start_oauth(conn, _params) do
    {:ok, %{url: url, session_params: session_params}} =
      Github.authorize_url(oauth_provider_config(:github))

    # Session params (used for OAuth 2.0 and OIDC strategies) will be
    # retrieved when user returns for the callback phase
    conn = put_session(conn, :session_params, session_params)

    # Redirect end-user to Github to authorize access to their account
    conn
    |> put_resp_header("location", url)
    |> send_resp(302, "")
  end

  def callback(conn, _params) do
    # End-user will return to the callback URL with params attached to the
    # request. These must be passed on to the strategy. In this example we only
    # expect GET query params, but the provider could also return the user with
    # a POST request where the params is in the POST body.
    %{params: params} = fetch_query_params(conn)

    # The session params (used for OAuth 2.0 and OIDC strategies) stored in the
    # request phase will be used in the callback phase
    session_params = get_session(conn, :session_params)

    :github
    |> oauth_provider_config()
    |> Keyword.put(:session_params, session_params)
    |> Github.callback(params)
    |> case do
      {:ok, %{user: user_data, token: token}} ->
        handle_oauth_callback(conn, user_data, token)

      {:error, %Assent.UnexpectedResponseError{} = error} ->
        Logger.error("Failed to login with Github: #{inspect(error)}")

        conn
        |> put_flash(:toast, %{kind: :error, title: "Failed to login with Github."})
        |> redirect(to: ~p"/login")
    end
  end

  # Not all these fields are always present
  # %{"email" => "user@example.com", "email_verified" => true,
  #   "picture" => "https://avatars.githubusercontent.com/u/12345678?v=4",
  #   "preferred_username" => "exampleuser",
  #   "profile" => "https://github.com/exampleuser", "sub" => 12345678,
  #   "name" => "Example User"}
  defp handle_oauth_callback(conn, user_data, _token) do
    %{"email" => email, "sub" => github_id} = user_data
    user = Accounts.get_user_by_auth_provider_id(:github, github_id)

    case {user, Sequin.feature_enabled?(:account_self_signup)} do
      {nil, false} ->
        conn
        |> put_flash(:toast, %{kind: :error, title: "Account creation is disabled"})
        |> redirect(to: ~p"/login")

      {nil, true} ->
        {:ok, user} =
          Accounts.register_user(:github, %{
            email: email,
            name: Map.get(user_data, "name"),
            auth_provider_id: github_id,
            extra: user_data
          })

        UserAuth.log_in_user(conn, user)

      {user, _} ->
        {:ok, updated_user} =
          Accounts.update_user_github_profile(user, %{
            name: Map.get(user_data, "name"),
            email: email,
            extra: user_data
          })

        conn
        |> put_flash(:toast, %{kind: :info, title: "Logged in successfully!"})
        |> UserAuth.log_in_user(updated_user)
    end
  end

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

  defp create(conn, %{"user" => user_params} = params, flash_info) do
    %{"email" => email, "password" => password} = user_params

    if user = Accounts.get_user_by_email_and_password(email, password) do
      conn = if flash_info, do: put_flash(conn, :toast, %{kind: :info, title: flash_info}), else: conn

      Accounts.update_user(user, %{last_login_at: DateTime.utc_now()})

      UserAuth.log_in_user(conn, user, user_params)
    else
      redirect_to = params["redirect_to"] || ~p"/login"

      # In order to prevent user enumeration attacks, don't disclose whether the email is registered.
      conn
      |> put_flash(:toast, %{kind: :error, title: "Invalid email or password"})
      |> put_flash(:email, String.slice(email, 0, 160))
      |> redirect(to: redirect_to)
    end
  end

  def delete(conn, params) do
    redirect_to = params["redirect_to"] || ~p"/login"

    conn
    |> put_flash(:toast, %{kind: :info, title: "Logged out successfully."})
    |> UserAuth.log_out_user(redirect_to)
  end

  def impersonate(conn, %{"secret" => secret}) do
    current_user_id = conn.assigns.current_user.id

    case Impersonate.getdel_secret(secret) do
      {:ok, %{impersonating_user_id: ^current_user_id, impersonated_user_id: impersonated_user_id}} ->
        impersonated_user = Accounts.get_user!(impersonated_user_id)

        conn
        |> UserAuth.log_in_user_with_impersonation(conn.assigns.current_user, impersonated_user)
        |> put_flash(:toast, %{
          kind: :info,
          title: "Impersonating user #{impersonated_user.name} (#{impersonated_user.email})"
        })

      _ ->
        conn
        |> put_flash(:toast, %{kind: :error, title: "Invalid impersonation link"})
        |> redirect(to: ~p"/")
    end
  end

  def unimpersonate(conn, _params) do
    case conn.assigns.current_user do
      %{impersonating_user: nil} ->
        conn
        |> put_flash(:toast, %{kind: :error, title: "You are not currently impersonating any account"})
        |> redirect(to: ~p"/")

      _ ->
        conn
        |> UserAuth.clear_impersonation()
        |> put_flash(:toast, %{kind: :info, title: "Impersonation ended"})
        |> redirect(to: ~p"/")
    end
  end

  defp oauth_provider_config(provider) do
    Application.get_env(:sequin, SequinWeb.UserSessionController)[provider]
  end
end

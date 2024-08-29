defmodule SequinWeb.UserSessionController do
  use SequinWeb, :controller

  alias Assent.Config
  alias Assent.Strategy.Github
  alias Sequin.Accounts
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
    # Session params should be added to the config so the strategy can use them
    |> Config.put(:session_params, session_params)
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

  defp handle_oauth_callback(conn, user_data, _token) do
    %{"email" => email, "name" => name, "sub" => github_id} = user_data
    github_id = to_string(github_id)

    case Accounts.get_user_by_auth_provider_id(:github, github_id) do
      nil ->
        {:ok, user} =
          Accounts.register_user(:github, %{
            email: email,
            name: name,
            auth_provider_id: to_string(github_id)
          })

        conn
        |> put_flash(:toast, %{kind: :info, title: "Account created successfully!"})
        |> UserAuth.log_in_user(user)

      user ->
        {:ok, updated_user} =
          Accounts.update_user_github_profile(user, %{name: name, email: email})

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

  defp oauth_provider_config(provider) do
    Application.get_env(:sequin, SequinWeb.UserSessionController)[provider]
  end
end

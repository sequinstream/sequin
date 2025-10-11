defmodule SequinWeb.UserLoginLive do
  @moduledoc false
  use SequinWeb, :live_view

  def render(assigns) do
    ~H"""
    <div class="flex items-center justify-center h-[80vh]">
      <div class="mx-auto max-w-sm w-full">
        <.alert :if={@accepting_invite?} class="mb-4">
          <.alert_title>Accepting invite</.alert_title>
          <.alert_description>
            Sign in or <.link navigate={~p"/register"}>sign up</.link>
            with the same email address as your invite to join the account.
          </.alert_description>
        </.alert>

        <.alert :if={@accepting_team_invite?} class="mb-4">
          <.alert_title>Accepting invite</.alert_title>
          <.alert_description>
            Sign in or
            <.link navigate={~p"/register"} class="font-semibold text-brand hover:underline">
              sign up
            </.link>
            to join the account.
          </.alert_description>
        </.alert>

        <.alert :if={@display_default_user_login?} class="mb-4">
          <.alert_title>Default login credentials</.alert_title>
          <.alert_description>
            Use these credentials for your first login:
            <div class="mt-2">
              <strong>Email:</strong> <%= Sequin.Accounts.default_user_email() %><br />
              <strong>Password:</strong> <%= Sequin.Accounts.default_user_password() %>
            </div>
          </.alert_description>
        </.alert>

        <.header class="text-center">
          Welcome back
          <:subtitle :if={!@accepting_invite?}>Sign in to your account</:subtitle>
          <:subtitle :if={@accepting_invite? || @accepting_team_invite?}>
            Sign in to accept your invite
          </:subtitle>
        </.header>
        <div class="mt-6 space-y-4">
          <.link href={if @github_disabled, do: "#", else: "/auth/github"}>
            <.button
              class="w-full flex items-center justify-center gap-2 mb-10 disabled:opacity-50 disabled:cursor-not-allowed"
              phx-click={if @github_disabled, do: nil, else: "github_login"}
              id="github-login-button"
              disabled={@github_loading || @github_disabled}
            >
              <svg class="w-5 h-5" viewBox="0 0 24 24" fill="currentColor">
                <path
                  fill-rule="evenodd"
                  clip-rule="evenodd"
                  d="M12 2C6.477 2 2 6.477 2 12c0 4.42 2.865 8.164 6.839 9.49.5.092.682-.217.682-.482 0-.237-.009-.866-.013-1.7-2.782.603-3.369-1.34-3.369-1.34-.454-1.156-1.11-1.463-1.11-1.463-.908-.62.069-.608.069-.608 1.003.07 1.531 1.03 1.531 1.03.892 1.529 2.341 1.087 2.91.831.092-.646.35-1.086.636-1.336-2.22-.253-4.555-1.11-4.555-4.943 0-1.091.39-1.984 1.03-2.682-.103-.253-.447-1.27.098-2.646 0 0 .84-.269 2.75 1.025A9.564 9.564 0 0112 6.844c.85.004 1.705.114 2.504.336 1.909-1.294 2.747-1.025 2.747-1.025.546 1.376.202 2.393.1 2.646.64.698 1.026 1.591 1.026 2.682 0 3.841-2.337 4.687-4.565 4.935.359.309.678.919.678 1.852 0 1.336-.012 2.415-.012 2.743 0 .267.18.578.688.48C19.138 20.161 22 16.416 22 12c0-5.523-4.477-10-10-10z"
                />
              </svg>
              <%= if @github_loading do %>
                Redirecting to GitHub...
              <% else %>
                Continue with GitHub
              <% end %>
            </.button>
          </.link>

          <div class="relative">
            <div class="absolute inset-0 flex items-center">
              <div class="w-full border-t border-gray-300"></div>
            </div>
            <div class="relative flex justify-center text-sm">
              <span class="px-2 bg-white text-gray-500">or</span>
            </div>
          </div>

          <.simple_form
            for={@form}
            id="login_form"
            action={~p"/login" <> if @redirect_to, do: "?redirect_to=#{@redirect_to}", else: ""}
            phx-update="ignore"
          >
            <.input field={@form[:email]} type="email" label="Email" required />
            <.input field={@form[:password]} type="password" label="Password" required />

            <:actions>
              <.input field={@form[:remember_me]} type="checkbox" label="Keep me logged in" />
              <.link href={~p"/users/reset_password"} class="text-sm font-semibold">
                Forgot your password?
              </.link>
            </:actions>
            <:actions>
              <.button phx-disable-with="Signing in..." class="w-full">
                Sign in
              </.button>
            </:actions>
          </.simple_form>
        </div>

        <%= cond do %>
          <% @account_self_signup? -> %>
            <p class="mt-6 text-center text-sm">
              Don't have an account?
              <.link navigate={~p"/register"} class="font-semibold text-brand hover:underline">
                Sign up
              </.link>
            </p>
          <% not @account_self_signup? and not @self_hosted -> %>
            <p class="mt-6 text-center text-sm">
              <.button
                disabled
                data-tooltip="Signups for Sequin Cloud are disabled."
                class="w-full !bg-gray-400 hover:bg-gray-300 cursor-not-allowed tooltip"
              >
                Sign up
              </.button>
            </p>
          <% true -> %>
        <% end %>
      </div>
    </div>
    """
  end

  def mount(_params, session, socket) do
    email = Phoenix.Flash.get(socket.assigns.flash, :email)

    form =
      cond do
        email ->
          to_form(%{"email" => email}, as: "user")

        Sequin.Accounts.only_default_user_and_first_login?() ->
          to_form(%{"email" => Sequin.Accounts.default_user_email()}, as: "user")

        true ->
          to_form(%{"email" => email}, as: "user")
      end

    socket =
      socket
      |> assign(form: form)
      |> assign(github_loading: false)
      |> assign(github_disabled: github_disabled?())
      |> assign(redirect_to: session["user_return_to"])
      |> assign(accepting_invite?: accepting_invite?(session))
      |> assign(accepting_team_invite?: accepting_team_invite?(session))
      |> assign(account_self_signup?: accepting_team_invite?(session) || Sequin.feature_enabled?(:account_self_signup))
      |> assign(self_hosted: Application.fetch_env!(:sequin, :self_hosted))
      |> assign(display_default_user_login?: Sequin.Accounts.only_default_user_and_first_login?())

    {:ok, socket, temporary_assigns: [form: form]}
  end

  def handle_event("github_login", _params, socket) do
    {:noreply, assign(socket, github_loading: true)}
  end

  defp github_disabled? do
    Application.get_env(:sequin, :self_hosted, false) &&
      is_nil(Application.get_env(:sequin, SequinWeb.UserSessionController)[:github][:client_id])
  end

  defp accepting_invite?(session) do
    case session["user_return_to"] do
      "/accept-invite/" <> _ -> true
      _ -> false
    end
  end

  defp accepting_team_invite?(session) do
    case session["user_return_to"] do
      "/accept-team-invite/" <> _ -> true
      _ -> false
    end
  end
end

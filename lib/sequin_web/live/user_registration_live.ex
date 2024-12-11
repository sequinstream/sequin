defmodule SequinWeb.UserRegistrationLive do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Accounts
  alias Sequin.Accounts.User

  def render(assigns) do
    ~H"""
    <div class="w-full md:h-[92vh] mx-auto my-auto flex flex-col md:flex-row bg-white rounded-lg shadow-lg">
      <div class="md:w-1/2 bg-black p-12 text-white flex flex-col max-w-[800px]">
        <div class="flex justify-between items-center mb-12">
          <.link href="https://sequinstream.com">
            <svg class="h-8" viewBox="0 0 372 115" xmlns="http://www.w3.org/2000/svg">
              <path
                d="M60.1097 66.6114C60.1097 32.0396 16.3548 44.1938 16.3548 26.3677C16.3548 19.0752 21.7567 15.0238 29.8594 15.0238C38.2323 15.0238 43.6341 19.6154 43.6341 27.5831H59.7046C59.7046 11.2426 47.2803 0.168809 29.8594 0.168809C12.7086 0.168809 0.41943 10.7024 0.41943 26.3677C0.41943 60.1292 43.9042 45.6793 43.9042 66.6114C43.9042 74.309 38.7725 78.4954 30.3996 78.4954C21.7567 78.4954 16.3548 73.9039 16.3548 65.9361H0.284385C0.284385 82.2767 12.8437 93.3505 30.3996 93.3505C47.6855 93.3505 60.1097 82.5468 60.1097 66.6114ZM136.043 58.2385C136.043 37.7116 121.459 23.1266 100.932 23.1266C80.4046 23.1266 65.8196 37.7116 65.8196 58.2385C65.8196 78.9006 80.4046 93.3505 100.932 93.3505C116.057 93.3505 129.291 85.7879 133.072 72.9585H115.787C112.41 77.9552 107.009 79.5758 100.932 79.5758C90.9381 79.5758 83.9158 73.7688 81.755 64.1805H135.503C135.773 62.2899 136.043 60.2642 136.043 58.2385ZM100.932 36.9013C110.25 36.9013 117.137 42.1681 119.433 50.9461H82.1602C84.591 42.1681 91.4783 36.9013 100.932 36.9013ZM176.235 23.1266C155.708 23.1266 141.124 37.5765 141.124 58.2385C141.124 78.7655 155.708 93.3505 176.235 93.3505C183.933 93.3505 190.55 90.7846 195.682 86.3281V114.958H211.347V24.4771H195.682V30.149C190.55 25.5574 183.933 23.1266 176.235 23.1266ZM176.235 37.4415C187.984 37.4415 195.682 45.6793 195.682 58.2385C195.682 70.7978 187.984 79.0356 176.235 79.0356C164.351 79.0356 156.519 70.7978 156.519 58.2385C156.519 45.6793 164.351 37.4415 176.235 37.4415ZM219.891 64.9908C219.891 81.6015 231.1 93.3505 247.035 93.3505C253.653 93.3505 259.325 90.9196 263.511 86.7332V92H278.906V24.4771H263.511V64.9908C263.511 73.3637 257.839 79.0356 249.466 79.0356C241.093 79.0356 235.421 73.3637 235.421 64.9908V24.4771H219.891V64.9908ZM287.681 16.3743H303.346V1.51927H287.681V16.3743ZM287.681 92H303.346V24.4771H287.681V92ZM311.947 92H327.612V51.4862C327.612 43.1134 333.014 37.4415 341.657 37.4415C349.895 37.4415 355.567 43.1134 355.567 51.4862V92H371.097V51.4862C371.097 34.8756 359.888 23.1266 343.953 23.1266C337.471 23.1266 331.799 25.4224 327.612 29.6088V24.4771H311.947V92Z"
                fill="white"
              />
            </svg>
          </.link>
          <.link
            href="https://sequinstream.com/docs"
            target="_blank"
            class="inline-flex items-center gap-2 px-4 py-2 text-sm font-semibold text-white bg-white/10 rounded-lg hover:bg-white/20"
          >
            <.icon name="hero-book-open" class="w-4 h-4" /> Documentation
          </.link>
        </div>
        <div class="flex-grow flex items-center">
          <div>
            <h2 class="text-7xl font-bold mb-4">
              Start <span class="text-purple-500">streaming</span> changes from <span class="text-purple-500">Postgres</span>
            </h2>
            <p class="text-lg mb-6">
              Sequin is designed to be a simple, fast, and reliable way to add change data capture to any Postgres database.
            </p>
          </div>
        </div>
      </div>

      <div class="md:w-1/2 p-12 flex items-center justify-center max-w-lg mx-auto">
        <div class="space-y-6 w-full">
          <div class="space-y-2 text-center">
            <h1 class="text-2xl font-semibold">Welcome! Create your account</h1>
            <p class="text-muted-foreground">
              Already have an account?
              <.link navigate={~p"/login"} class="font-semibold hover:underline text-purple-500">
                Log in
              </.link>
            </p>
          </div>

          <.alert :if={@accepting_invite?}>
            <.alert_title>Accepting Invite</.alert_title>
            <.alert_description>
              Register with the same email address as your invite to join the account.
            </.alert_description>
          </.alert>

          <.alert :if={@accepting_team_invite?}>
            <.alert_title>Accepting invite</.alert_title>
            <.alert_description>
              Register to join the account.
            </.alert_description>
          </.alert>

          <div class="space-y-4">
            <.link href={if @github_disabled, do: "#", else: "/auth/github"}>
              <.button
                class="w-full flex items-center justify-center gap-2 disabled:opacity-50 disabled:cursor-not-allowed"
                phx-click={if @github_disabled, do: nil, else: "github_register"}
                disabled={@github_loading || @github_disabled}
              >
                <svg class="w-4 h-4" viewBox="0 0 24 24" fill="currentColor">
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
                <span class="w-full border-t"></span>
              </div>
              <div class="relative flex justify-center text-xs uppercase">
                <span class="bg-white px-2 text-muted-foreground">Or continue with</span>
              </div>
            </div>

            <.simple_form
              for={@form}
              id="registration_form"
              phx-submit="save"
              phx-change="validate"
              phx-trigger-action={@trigger_submit}
              action={~p"/login?_action=registered" <> if @redirect_to, do: "?redirect_to=#{@redirect_to}", else: ""}
              method="post"
              class="space-y-4"
            >
              <.input
                field={@form[:email]}
                type="email"
                label="Email"
                placeholder="m@example.com"
                required
              />
              <.input field={@form[:password]} type="password" label="Password" required />

              <:actions>
                <.button phx-disable-with="Creating account..." class="w-full">
                  Create an account
                </.button>
              </:actions>
            </.simple_form>
          </div>
        </div>
      </div>
    </div>
    """
  end

  def mount(_params, session, socket) do
    if Sequin.feature_enabled?(:account_self_signup) do
      changeset = Accounts.change_user_registration(%User{})

      socket =
        socket
        |> assign(trigger_submit: false)
        |> assign(github_loading: false)
        |> assign(github_disabled: github_disabled?())
        |> assign(redirect_to: session["user_return_to"])
        |> assign(accepting_invite?: accepting_invite?(session))
        |> assign(accepting_team_invite?: accepting_team_invite?(session))
        |> assign_form(changeset)

      {:ok, socket, temporary_assigns: [form: nil]}
    else
      {:ok,
       socket
       |> put_flash(:toast, %{kind: :error, title: "Account creation is disabled"})
       |> redirect(to: ~p"/login")}
    end
  end

  def handle_event("save", %{"user" => user_params}, socket) do
    case Accounts.register_user(:identity, user_params) do
      {:ok, user} ->
        {:ok, _} =
          Accounts.deliver_user_confirmation_instructions(
            user,
            &url(~p"/users/confirm/#{&1}")
          )

        changeset = Accounts.change_user_registration(user)

        {:noreply, socket |> assign(trigger_submit: true) |> assign_form(changeset)}

      {:error, %Ecto.Changeset{} = changeset} ->
        {:noreply, assign_form(socket, Map.put(changeset, :action, :validate))}
    end
  end

  def handle_event("validate", %{"user" => user_params}, socket) do
    changeset = Accounts.change_user_registration(%User{}, user_params)
    {:noreply, assign_form(socket, changeset)}
  end

  def handle_event("github_register", _params, socket) do
    {:noreply, assign(socket, github_loading: true)}
  end

  defp assign_form(socket, %Ecto.Changeset{} = changeset) do
    form = to_form(changeset, as: "user")

    assign(socket, form: form)
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

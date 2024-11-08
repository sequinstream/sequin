defmodule SequinWeb.UserRegistrationLive do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Accounts
  alias Sequin.Accounts.User

  def render(assigns) do
    ~H"""
    <div class="w-full h-[92vh] mx-auto my-auto flex flex-col md:flex-row bg-white rounded-lg shadow-lg overflow-hidden">
      <div class="md:w-1/2 bg-black p-12 text-white flex flex-col max-w-[800px]">
        <div class="flex justify-between items-center mb-12">
          <.link href="https://sequinstream.com">
            <img src="/images/sequin-logo-white.svg" alt="Sequin Logo" class="h-8" />
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
              Start <span class="text-purple-500">streaming</span>
              data, right from <span class="text-purple-500">Postgres</span>
            </h2>
            <p class="text-lg mb-6">
              Sequin is designed to be 10X better for any streaming use case that starts with Postgres.
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
end

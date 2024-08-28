defmodule SequinWeb.UserRegistrationLive do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Accounts
  alias Sequin.Accounts.User

  def render(assigns) do
    ~H"""
    <div class="flex items-center justify-center h-[80vh]">
      <div class="mx-auto max-w-sm w-full">
        <.header class="text-center">
          Register for an account
          <:subtitle>
            Already registered?
            <.link navigate={~p"/login"} class="font-semibold text-brand hover:underline">
              Log in
            </.link>
          </:subtitle>
        </.header>

        <.simple_form
          for={@form}
          id="registration_form"
          phx-submit="save"
          phx-change="validate"
          phx-trigger-action={@trigger_submit}
          action={~p"/login?_action=registered"}
          method="post"
        >
          <.input field={@form[:email]} type="email" label="Email" required />
          <.input field={@form[:password]} type="password" label="Password" required />

          <:actions>
            <.button phx-disable-with="Creating account..." class="w-full">
              Create an account <span aria-hidden="true">→</span>
            </.button>
          </:actions>
        </.simple_form>
      </div>
    </div>
    """
  end

  def mount(_params, _session, socket) do
    changeset = Accounts.change_user_registration(%User{})

    socket =
      socket
      |> assign(trigger_submit: false)
      |> assign_form(changeset)

    {:ok, socket, temporary_assigns: [form: nil]}
  end

  def handle_event("save", %{"user" => user_params}, socket) do
    case Accounts.register_user(user_params) do
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

  defp assign_form(socket, %Ecto.Changeset{} = changeset) do
    form = to_form(changeset, as: "user")

    assign(socket, form: form)
  end
end

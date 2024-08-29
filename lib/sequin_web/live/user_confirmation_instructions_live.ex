defmodule SequinWeb.UserConfirmationInstructionsLive do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Accounts

  def render(assigns) do
    ~H"""
    <div class="flex items-center justify-center h-[80vh]">
      <div class="mx-auto max-w-sm w-full">
        <.header class="text-center">
          No confirmation instructions received?
          <:subtitle>We'll send a new confirmation link to your inbox</:subtitle>
        </.header>

        <.simple_form for={@form} id="resend_confirmation_form" phx-submit="send_instructions">
          <.input field={@form[:email]} type="email" placeholder="Email" required />
          <:actions>
            <.button phx-disable-with="Sending..." class="w-full">
              Resend confirmation instructions
            </.button>
          </:actions>
        </.simple_form>

        <p class="text-center mt-4">
          <.link href={~p"/register"}>Register</.link> | <.link href={~p"/login"}>Log in</.link>
        </p>
      </div>
    </div>
    """
  end

  def mount(_params, _session, socket) do
    {:ok, assign(socket, form: to_form(%{}, as: "user"))}
  end

  def handle_event("send_instructions", %{"user" => %{"email" => email}}, socket) do
    if user = Accounts.get_identity_user_by_email(email) do
      Accounts.deliver_user_confirmation_instructions(
        user,
        &url(~p"/users/confirm/#{&1}")
      )
    end

    info =
      "If your email is in our system and it has not been confirmed yet, you will receive an email with instructions shortly."

    {:noreply,
     socket
     |> put_flash(:toast, %{kind: :info, title: info})
     |> put_flash(:info, info)
     |> redirect(to: ~p"/")}
  end
end

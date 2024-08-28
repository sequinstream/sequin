defmodule SequinWeb.UserResetPasswordLive do
  @moduledoc false
  use SequinWeb, :live_view

  alias Sequin.Accounts

  def render(assigns) do
    ~H"""
    <div class="flex items-center justify-center h-[80vh]">
      <div class="mx-auto max-w-sm w-full">
        <.header class="text-center">Reset Password</.header>

        <.simple_form
          for={@form}
          id="reset_password_form"
          phx-submit="reset_password"
          phx-change="validate"
        >
          <.input field={@form[:password]} type="password" label="New password" required />
          <.input
            field={@form[:password_confirmation]}
            type="password"
            label="Confirm new password"
            required
          />
          <:actions>
            <.button phx-disable-with="Resetting..." class="w-full">Reset Password</.button>
          </:actions>
        </.simple_form>

        <p class="text-center text-sm mt-4">
          <.link href={~p"/register"}>Register</.link> | <.link href={~p"/login"}>Log in</.link>
        </p>
      </div>
    </div>
    """
  end

  def mount(params, _session, socket) do
    socket = assign_user_and_token(socket, params)

    form_source =
      case socket.assigns do
        %{user: user} ->
          Accounts.change_user_password(user)

        _ ->
          %{}
      end

    {:ok, assign_form(socket, form_source), temporary_assigns: [form: nil]}
  end

  # Do not log in the user after reset password to avoid a
  # leaked token giving the user access to the account.
  def handle_event("reset_password", %{"user" => user_params}, socket) do
    case Accounts.reset_user_password(socket.assigns.user, user_params) do
      {:ok, _} ->
        {:noreply,
         socket
         |> put_flash(:toast, %{kind: :info, title: "Password reset successfully. Go ahead and log in."})
         |> redirect(to: ~p"/login")}

      {:error, changeset} ->
        {:noreply, assign_form(socket, Map.put(changeset, :action, :insert))}
    end
  end

  def handle_event("validate", %{"user" => user_params}, socket) do
    changeset = Accounts.change_user_password(socket.assigns.user, user_params)
    {:noreply, assign_form(socket, changeset)}
  end

  defp assign_user_and_token(socket, %{"token" => token}) do
    if user = Accounts.get_user_by_reset_password_token(token) do
      assign(socket, user: user, token: token)
    else
      socket
      |> put_flash(:toast, %{kind: :error, title: "Reset password link is invalid or it has expired."})
      |> redirect(to: ~p"/")
    end
  end

  defp assign_form(socket, %{} = source) do
    assign(socket, :form, to_form(source, as: "user"))
  end
end

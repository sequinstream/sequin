defmodule SequinWeb.UserSettingsLive do
  @moduledoc false
  use SequinWeb, :live_view

  import LiveSvelte

  alias Sequin.Accounts
  alias Sequin.Accounts.User

  require Logger

  def mount(_params, _session, socket) do
    user = current_user(socket)
    password_changeset = Accounts.change_user_password(user)

    {:ok,
     socket
     |> assign(:current_user, user)
     |> assign(:password_changeset, to_form(password_changeset))
     |> assign(:self_hosted, Application.get_env(:sequin, :self_hosted))}
  end

  def handle_event(
        "change_password",
        %{
          "current_password" => current_password,
          "new_password" => new_password,
          "new_password_confirmation" => new_password_confirmation
        },
        socket
      ) do
    case Accounts.update_user_password(socket.assigns.current_user, current_password, %{
           password: new_password,
           password_confirmation: new_password_confirmation
         }) do
      {:ok, _user} ->
        Process.send_after(self(), :login, to_timeout(second: 3))

        {:reply, %{ok: true},
         put_flash(socket, :toast, %{kind: :info, title: "Password updated successfully. Redirecting to login..."})}

      {:error, changeset} ->
        errors = Sequin.Error.errors_on(changeset)

        {:reply, %{ok: false, errors: errors},
         put_flash(socket, :toast, %{kind: :error, title: "Failed to update password"})}
    end
  end

  def handle_event("update_email", %{"email" => email}, socket) do
    if !socket.assigns.self_hosted, do: raise("update_email is only supported on self-hosted instances")

    case Accounts.update_user_email(socket.assigns.current_user, email) do
      {:ok, user} ->
        {:reply, %{ok: true, email: user.email},
         socket
         |> assign(:current_user, user)
         |> put_flash(:toast, %{kind: :info, title: "Email updated successfully"})}

      {:error, changeset} ->
        errors = Sequin.Error.errors_on(changeset)

        {:reply, %{ok: false, errors: errors},
         put_flash(socket, :toast, %{kind: :error, title: "Failed to update email"})}
    end
  end

  def handle_event("delete_user", _params, socket) do
    Logger.info("Would delete user: #{socket.assigns.current_user.id}")
    {:reply, %{ok: true}, socket}
  end

  def handle_info(:login, socket) do
    {:noreply, push_navigate(socket, to: ~p"/login")}
  end

  def render(assigns) do
    assigns = assign(assigns, :parent_id, "user_settings")

    ~H"""
    <div id={@parent_id}>
      <.svelte
        name="settings/UserSettings"
        props={
          %{
            currentUser: encode(@current_user),
            parent: @parent_id,
            selfHosted: @self_hosted
          }
        }
        socket={@socket}
      />
    </div>
    """
  end

  def encode(%User{} = user) do
    %{
      id: user.id,
      email: user.email,
      auth_provider: user.auth_provider
    }
  end
end

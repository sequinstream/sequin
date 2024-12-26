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
     |> assign(:password_changeset, to_form(password_changeset))}
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
        Process.send_after(self(), :login, :timer.seconds(3))

        {:reply, %{ok: true},
         put_flash(socket, :toast, %{kind: :info, title: "Password updated successfully. Redirecting to login..."})}

      {:error, changeset} ->
        errors = Sequin.Error.errors_on(changeset)
        dbg(errors)

        {:reply, %{ok: false, errors: errors},
         put_flash(socket, :toast, %{kind: :error, title: "Failed to update password"})}
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
            parent: @parent_id
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

defmodule SequinWeb.Settings.AccountSettingsLive do
  @moduledoc false
  use SequinWeb, :live_view

  import LiveSvelte

  alias Sequin.Accounts
  alias Sequin.Accounts.User
  alias Sequin.Error
  alias Sequin.Error.NotFoundError

  def mount(_params, _session, socket) do
    {:ok, assign(socket, :accounts, accounts(socket))}
  end

  def handle_event("change_selected_account", %{"accountId" => account_id}, socket) do
    account = Sequin.Enum.find!(accounts(socket), &(&1.id == account_id))

    case Accounts.set_current_account_for_user(current_user_id(socket), account.id) do
      {:ok, updated_user} ->
        if impersonating?(socket) do
          {:noreply, push_navigate(socket, to: socket.assigns.current_path)}
        else
          {:noreply,
           socket
           |> assign(current_user: updated_user)
           |> push_navigate(to: socket.assigns.current_path)}
        end

      {:error, _changeset} ->
        {:noreply, socket}
    end
  end

  def handle_event("rename_account", %{"accountId" => account_id, "name" => new_name}, socket) do
    user = current_user(socket)
    account = Sequin.Enum.find!(user.accounts, &(&1.id == account_id))

    case Accounts.update_account(account, %{name: new_name}) do
      {:ok, _updated_account} ->
        {:noreply,
         socket
         |> assign(current_user: Accounts.get_user_with_preloads!(user.id))
         |> push_navigate(to: socket.assigns.current_path)}

      {:error, changeset} ->
        error = Error.validation(changeset: changeset)
        {:reply, %{error: Exception.message(error)}, socket}
    end
  end

  def handle_event("invite_user", %{"accountId" => account_id, "email" => sent_to}, socket) do
    user = current_user(socket)
    account = Sequin.Enum.find!(user.accounts, &(&1.id == account_id))

    case Accounts.invite_user(user, account, sent_to, &url(~p"/accept-invite/#{&1}")) do
      {:ok, _email} ->
        {:noreply, assign(socket, :pending_invites, Accounts.list_pending_invites_for_account(account))}

      {:error, %Error.InvariantError{} = error} ->
        {:reply, %{error: Exception.message(error)}, socket}

      {:error, changeset} ->
        error = Error.validation(changeset: changeset)
        {:reply, %{error: Exception.message(error)}, socket}
    end
  end

  def handle_event("revoke_invite", %{"inviteId" => invite_id, "accountId" => account_id}, socket) do
    user = current_user(socket)
    account = Sequin.Enum.find!(user.accounts, &(&1.id == account_id))

    case Accounts.revoke_account_invite(user, invite_id) do
      {:ok, _user_token} ->
        {:noreply, assign(socket, :pending_invites, Accounts.list_pending_invites_for_account(account))}

      {:error, changeset} ->
        error = Error.validation(changeset: changeset)
        {:reply, %{error: Exception.message(error)}, socket}
    end
  end

  def handle_event("delete_account", %{"accountId" => account_id}, socket) do
    user = current_user(socket)
    account = Sequin.Enum.find!(user.accounts, &(&1.id == account_id))

    case Accounts.delete_account_and_account_resources(account, delete_users: false) do
      {:ok, _deleted_account} ->
        {:reply, %{ok: true}, push_navigate(socket, to: socket.assigns.current_path)}

      {:error, %Error.InvariantError{} = error} ->
        {:reply, %{error: Exception.message(error)}, socket}

      {:error, changeset} ->
        error = Error.validation(changeset: changeset)
        {:reply, %{error: Exception.message(error)}, socket}
    end
  end

  def handle_event("remove_user", %{"accountId" => account_id, "userId" => user_id}, socket) do
    user_to_remove = Accounts.get_user!(user_id)

    current_user = current_user(socket)
    # Verify that the current user is in the account
    account = Sequin.Enum.find!(current_user.accounts, &(&1.id == account_id))

    case Accounts.remove_user_from_account(user_to_remove, account) do
      {:ok, _} ->
        {:noreply,
         socket
         |> assign(:current_account_users, Accounts.list_users_for_account(account.id))
         |> put_flash(:info, "User removed successfully")}

      {:error, %NotFoundError{} = error} ->
        {:reply, %{error: Exception.message(error)}, socket}

      {:error, changeset} ->
        error = Error.validation(changeset: changeset)
        {:reply, %{error: Exception.message(error)}, socket}
    end
  end

  def render(assigns) do
    current_account = User.current_account(assigns.current_user)

    assigns =
      assigns
      |> assign(:parent_id, "account_settings")
      |> assign(:current_account, current_account)
      |> assign(:current_account_users, Accounts.list_users_for_account(current_account.id))
      |> assign(:pending_invites, Accounts.list_pending_invites_for_account(current_account))

    ~H"""
    <div id={@parent_id}>
      <.svelte
        name="settings/AccountSettings"
        props={
          %{
            accounts: Enum.sort_by(@accounts, & &1.inserted_at, DateTime),
            selectedAccount: @current_account,
            currentAccountUsers: @current_account_users,
            currentUser: @current_user,
            parent: @parent_id,
            pendingInvites: @pending_invites
          }
        }
        socket={@socket}
      />
    </div>
    """
  end
end

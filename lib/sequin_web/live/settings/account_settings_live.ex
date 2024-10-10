defmodule SequinWeb.Settings.AccountSettingsLive do
  @moduledoc false
  use SequinWeb, :live_view

  import LiveSvelte

  alias Sequin.Accounts
  alias Sequin.Accounts.User
  alias Sequin.Error

  def mount(_params, _session, socket) do
    {:ok, socket}
  end

  def handle_event("change_selected_account", %{"accountId" => account_id}, socket) do
    user = current_user(socket)
    account = Sequin.Enum.find!(user.accounts, &(&1.id == account_id))

    case Accounts.set_current_account_for_user(user.id, account.id) do
      {:ok, updated_user} ->
        {:noreply,
         socket
         |> assign(current_user: updated_user)
         |> push_navigate(to: socket.assigns.current_path)}

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

  def render(assigns) do
    assigns =
      assigns
      |> assign(:parent_id, "account_settings")
      |> assign(:current_account, User.current_account(assigns.current_user))

    ~H"""
    <div id={@parent_id}>
      <.svelte
        name="settings/AccountSettings"
        props={
          %{
            accounts: Enum.sort_by(@current_user.accounts, & &1.inserted_at, DateTime),
            selectedAccount: @current_account,
            parent: @parent_id
          }
        }
        socket={@socket}
      />
    </div>
    """
  end
end

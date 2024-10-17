defmodule SequinWeb.LiveHelpers do
  @moduledoc """
  Helpers that are imported into LiveView components that help with common socket retrieval tasks.
  """
  alias Phoenix.LiveView.Socket
  alias Sequin.Accounts
  alias Sequin.Accounts.Account
  alias Sequin.Accounts.User

  @spec current_user(Socket.t()) :: User.t() | nil
  def current_user(socket) do
    socket.assigns.current_user.impersonating_user || socket.assigns.current_user
  end

  @spec current_user_id(Socket.t()) :: User.id() | nil
  def current_user_id(socket) do
    user = current_user(socket)
    user && user.id
  end

  def push_toast(socket, toast) do
    Phoenix.LiveView.push_event(socket, "toast", toast)
  end

  @spec current_account(Socket.t()) :: Account.t() | nil
  def current_account(socket) do
    user = socket.assigns.current_user

    if user && user.impersonating_user do
      User.current_account(user.impersonating_user)
    else
      user && User.current_account(user)
    end
  end

  @spec current_account_id(Socket.t()) :: Account.id() | nil
  def current_account_id(socket) do
    user = socket.assigns.current_user

    if user && user.impersonating_user do
      impersonating_account = User.current_account(user.impersonating_user)
      impersonating_account && impersonating_account.id
    else
      current_account = User.current_account(user)
      current_account && current_account.id
    end
  end

  def accounts(socket) do
    user = socket.assigns.current_user

    if user.impersonating_user do
      Accounts.list_accounts_for_user(user.impersonating_user.id)
    else
      user.accounts
    end
  end

  def impersonating?(socket) do
    not is_nil(socket.assigns.current_user.impersonating_user)
  end

  def blank?(value) when is_list(value) do
    value == []
  end

  def blank?(value) do
    is_nil(value) || value == ""
  end

  def not_blank?(value) when is_list(value) do
    not blank?(value)
  end

  def not_blank?(value) do
    not blank?(value)
  end
end

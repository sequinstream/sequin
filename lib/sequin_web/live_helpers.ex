defmodule SequinWeb.LiveHelpers do
  @moduledoc """
  Helpers that are imported into LiveView components that help with common socket retrieval tasks.
  """
  alias Phoenix.LiveView.Socket
  alias Sequin.Accounts.Account
  alias Sequin.Accounts.User

  @spec current_user(Socket.t()) :: User.t() | nil
  def current_user(socket) do
    socket.assigns.current_user
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
    user = current_user(socket)

    if user && user.impersonating_account do
      user.impersonating_account
    else
      user && User.current_account(user)
    end
  end

  @spec current_account_id(Socket.t()) :: Account.id() | nil
  def current_account_id(socket) do
    user = current_user(socket)

    cond do
      user && user.impersonating_account ->
        user.impersonating_account.id

      user ->
        current_account = User.current_account(user)
        current_account && current_account.id

      true ->
        nil
    end
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

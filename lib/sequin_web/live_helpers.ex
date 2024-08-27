defmodule SequinWeb.LiveHelpers do
  @moduledoc """
  Helpers that are imported into LiveView components that help with common socket retrieval tasks.
  """
  alias Phoenix.LiveView.Socket
  alias Sequin.Accounts.Account

  def push_toast(socket, kind, title, description \\ nil) do
    Phoenix.LiveView.push_event(socket, "toast", %{kind: kind, title: title, description: description})
  end

  @spec current_account(Socket.t()) :: Account.t()
  def current_account(socket) do
    socket.assigns.current_account
  end

  @spec current_account_id(Socket.t()) :: Account.id()
  def current_account_id(socket) do
    current_account(socket).id
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

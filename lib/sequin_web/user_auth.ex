defmodule SequinWeb.UserAuth do
  @moduledoc false
  import Phoenix.Component

  alias Sequin.Accounts

  def on_mount(:default, _params, _session, socket) do
    account = List.first(Accounts.list_accounts())
    {:cont, assign(socket, :current_account, account)}
  end
end

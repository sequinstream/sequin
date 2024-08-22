defmodule SequinWeb.UserAuth do
  @moduledoc false
  import Phoenix.Component
  import Phoenix.LiveView

  alias Sequin.Accounts

  def on_mount(:default, _params, _session, socket) do
    case safe_load_account() do
      {:ok, account} ->
        {:cont, assign(socket, :current_account, account)}

      :error ->
        {:halt, push_navigate(socket, to: "/")}
    end
  end

  defp safe_load_account do
    case Accounts.list_accounts() do
      [] -> :error
      [account | _] -> {:ok, account}
    end
  rescue
    _ -> :error
  end
end

defmodule SequinWeb.UserAuth do
  @moduledoc false
  import Phoenix.Component
  import Phoenix.LiveView

  alias Sequin.Accounts
  alias Sequin.Repo

  def on_mount(:default, _params, _session, socket) do
    case safe_load_user() do
      {:ok, user} ->
        user = Repo.preload(user, :account)
        {:cont, assign(socket, :current_user, user)}

      :error ->
        {:halt, push_navigate(socket, to: "/")}
    end
  end

  defp safe_load_user do
    case Accounts.list_users() do
      [] -> :error
      [user | _] -> {:ok, user}
    end
  rescue
    _ -> :error
  end
end
